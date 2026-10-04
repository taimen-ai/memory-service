"""IAM access token на HTTP-границе движка памяти (ADR-018).

Второй способ аутентификации рядом со статическими ключами (``CB_SERVER_API_KEY``,
``CB_SERVER_API_KEYS_PII``, реестр ``CB_API_KEYS``): при ``CB_IAM_ENABLED=true`` Bearer,
не совпавший ни с одним статическим ключом и похожий на JWT, проверяется как access
token IAM через общий ``platform-auth-sdk`` (подпись по JWKS, точный issuer и audience
``memory-service``, временные claims). Проверенный контекст проецируется в те же
``CallerGrants``, что и ключ реестра, — ``_authorize`` в ``app.py`` о JWT не знает.

Правила проекции (полномочия выводит сам сервис, IAM даёт identity и потолок scope):

* scopes ``memory:read`` / ``memory:write`` — флаги чтения/записи всех грантов токена;
  ``memory:pii`` — полный допуск к ПДн (без него при ``CB_PII_PROTECTION`` — маска);
  scope действует, только если он есть и в токене, и в ``scope_ceiling`` (SDK);
* namespaces: ``tenant:<tenant_id>`` (ровно) и поддерево ``tenant:<tenant_id>:*`` —
  так Control Plane именует память tenant'а (``CP_CONTEXT_NAMESPACE_PREFIX=tenant:``);
  плюс каждый namespace из claim ``memory_namespaces`` (список строк) — тоже ровно
  и его поддерево ``<ns>:*``;
* scope ``memory:tenants`` — всё поддерево ``tenant:*`` независимо от ``tenant_id``
  токена: полномочие платформенного сервиса (service account Control Plane —
  context-adapter пишет и читает память всех своих tenant'ов, а tenant IAM у него
  один). Кому выдавать этот scope, решает потолок service account в IAM;
* scope ``memory:service`` — service scope, identity ядра: регистрация доменных пакетов
  видов и, при включённом ограничении маршрутов ядра (``CB_CORE_ONLY``/
  ``CB_CORE_IDENTITIES``, амендмент MEM-ADR-020), единственный scope, с которым
  проходят reconcile, пакеты и виды namespace; namespaces не расширяет;
* token без ни одного из scope памяти валиден, но не покрывает ни одного namespace —
  каждый data-запрос получит 403 от ``_authorize``.

Это единственное место в пакете, где импортируется ``platform_auth`` — осознанное
исключение из инварианта изоляции (AGENTS.md, инвариант 1; ADR-018): SDK enforcement
общий для всех resource services платформы, а сам движок памяти его не видит.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from platform_auth import (
    AuthorizationClient,
    AuthorizationPolicy,
    AuthorizationUnavailable,
    ServiceCredentials,
    ServiceTokenProvider,
)
from platform_auth.context import TrustedAuthContext
from platform_auth.errors import InvalidToken, VerificationUnavailable
from platform_auth.jwks import JwksCache
from platform_auth.verify import KeySource, TokenVerifier, VerifierConfig

from platform_memory.core.namespaces import validate_namespace
from platform_memory.server.auth import CallerGrants, NamespaceGrant

if TYPE_CHECKING:
    from platform_memory.core.config import Settings

# Scope ceiling памяти. Значения объявляются в реестре audiences IAM (audience
# ``memory-service``); сервис лишь проверяет, что предъявленный токен их несёт.
SCOPE_READ = "memory:read"
SCOPE_WRITE = "memory:write"
SCOPE_PII = "memory:pii"
# Платформенный сервис: память всех tenant'ов (поддерево ``tenant:*``), а не только
# своего ``tenant_id``. Только в потолке service accounts, людям и агентам не выдаётся.
SCOPE_TENANTS = "memory:tenants"
# Service scope (MEM-ADR-020): операции уровня сервиса — регистрация доменных пакетов
# видов и маршруты ядра (reconcile, пакеты, виды namespace) при CB_CORE_ONLY /
# CB_CORE_IDENTITIES. Identity ядра. Только в потолке service accounts платформы.
SCOPE_SERVICE = "memory:service"

# Префикс namespaces tenant'а — как у Control Plane (CP_CONTEXT_NAMESPACE_PREFIX).
TENANT_NAMESPACE_PREFIX = "tenant:"
# Необязательный claim: дополнительные namespaces (список), выданные токену явно.
NAMESPACES_CLAIM = "memory_namespaces"


class IamAuthError(Exception):
    """Токен отсутствует, повреждён, чужой (issuer/audience) или истёк -> 401."""


class IamUnavailable(Exception):
    """Проверить нечем: verifier не сконфигурирован или JWKS недоступен -> 503."""


def looks_like_jwt(token: str) -> bool:
    """Отличить JWT по форме (три непустые части через точку), не пытаясь проверить.

    Решение по форме, а не перебором способов аутентификации: иначе код ответа выдавал
    бы, какой из способов «почти» подошёл.
    """
    parts = token.split(".")
    return len(parts) == 3 and all(parts)


def _subtree(namespace: str, *, read: bool, write: bool) -> tuple[NamespaceGrant, ...]:
    """Пара грантов «ровно namespace» + «его поддерево ``namespace:*``»."""
    return (
        NamespaceGrant(prefix=namespace, read=read, write=write, exact=True),
        NamespaceGrant(prefix=f"{namespace}:", read=read, write=write),
    )


def grants_from_context(ctx: TrustedAuthContext) -> CallerGrants:
    """Спроецировать проверенный контекст IAM в гранты движка (см. docstring модуля)."""
    read = ctx.has_scope(SCOPE_READ)
    write = ctx.has_scope(SCOPE_WRITE)
    grants: list[NamespaceGrant] = list(
        _subtree(f"{TENANT_NAMESPACE_PREFIX}{ctx.tenant_id}", read=read, write=write)
    )
    if ctx.has_scope(SCOPE_TENANTS):
        grants.append(NamespaceGrant(prefix=TENANT_NAMESPACE_PREFIX, read=read, write=write))
    extra = ctx.claims.get(NAMESPACES_CLAIM)
    if isinstance(extra, list | tuple):
        for raw in extra:
            ns = str(raw).strip()
            if not ns:
                continue
            try:
                validate_namespace(ns)
            except ValueError:
                continue  # невалидное имя не может быть namespace — молча пропускаем
            grants.extend(_subtree(ns, read=read, write=write))
    label = f"iam:{ctx.principal_type or 'principal'}:{ctx.principal_id}"
    return CallerGrants(
        name=label,
        grants=tuple(grants),
        pii=ctx.has_scope(SCOPE_PII),
        service=ctx.has_scope(SCOPE_SERVICE),
    )


def _default_key_source(settings: Settings) -> KeySource:
    if not settings.iam_jwks_url:
        raise VerificationUnavailable("jwks_url_not_configured")
    return JwksCache(settings.iam_jwks_url)


# Точка подмены для тестов: источник ключей без сети (JwksCache.seed / StaticKeySet).
key_source_factory: Callable[[Settings], KeySource] = _default_key_source

_VerifierKey = tuple[str, str, str, float]
_verifiers: dict[_VerifierKey, TokenVerifier] = {}


def reset() -> None:
    """Сбросить кэш verifier'ов (тесты, смена конфигурации)."""
    _verifiers.clear()


def _verifier(settings: Settings) -> TokenVerifier:
    """Один verifier на конфигурацию: JWKS-кэш живёт между запросами."""
    key: _VerifierKey = (
        settings.iam_issuer,
        settings.iam_jwks_url,
        settings.iam_audience,
        float(settings.iam_leeway_seconds),
    )
    verifier = _verifiers.get(key)
    if verifier is None:
        verifier = TokenVerifier(
            key_source_factory(settings),
            VerifierConfig(
                issuer=settings.iam_issuer,
                audience=settings.iam_audience,
                leeway_seconds=float(settings.iam_leeway_seconds),
            ),
        )
        _verifiers[key] = verifier
    return verifier


async def verify_token_with_context(
    settings: Settings, token: str
) -> tuple[CallerGrants, TrustedAuthContext]:
    """Проверить access token IAM: гранты движка плюс сам проверенный контекст.

    Контекст нужен слою видимости (MEM-ADR-019): subject решения policy-service —
    IAM principal id из токена. Любой дефект токена -> ``IamAuthError`` (один ответ
    на все причины — endpoint не должен быть оракулом); невозможность проверить ->
    ``IamUnavailable`` (fail closed).
    """
    try:
        verifier = _verifier(settings)
        ctx = await verifier.verify(token)
    except InvalidToken as exc:
        raise IamAuthError(exc.audit_reason) from exc
    except VerificationUnavailable as exc:
        raise IamUnavailable(exc.audit_reason) from exc
    return grants_from_context(ctx), ctx


async def verify_token(settings: Settings, token: str) -> CallerGrants:
    """Проверить access token IAM и вернуть гранты движка (см. verify_token_with_context)."""
    grants, _ = await verify_token_with_context(settings, token)
    return grants


# --- клиент policy-service (MEM-ADR-019) --------------------------------------
#
# Живёт здесь, а не в server/visibility.py: SDK платформы импортируется только в
# этом модуле (инвариант 1 AGENTS.md), при сборке без SDK удаляется вместе с ним.

PolicyUnavailable = AuthorizationUnavailable

_policy_clients: dict[tuple[str, str, str, str], AuthorizationClient] = {}


def build_policy_client(settings: Settings) -> AuthorizationClient:
    """Клиент policy-service от service identity движка (client credentials IAM)."""
    if not settings.iam_base_url or not settings.iam_client_id or not settings.iam_client_secret:
        raise AuthorizationUnavailable("policy_client_not_configured")
    key = (
        settings.policy_url,
        settings.iam_base_url,
        settings.iam_client_id,
        settings.policy_audience,
    )
    client = _policy_clients.get(key)
    if client is None:
        provider = ServiceTokenProvider(
            settings.iam_base_url,
            ServiceCredentials(
                client_id=settings.iam_client_id,
                client_secret=settings.iam_client_secret,
                audience=settings.policy_audience,
                scopes=("policy:check", "policy:check-on-behalf"),
            ),
            request_timeout_seconds=settings.policy_timeout_seconds,
        )
        client = AuthorizationClient(
            settings.policy_url,
            provider,
            policy=AuthorizationPolicy(
                cache_ttl_seconds=settings.policy_cache_ttl_seconds,
                request_timeout_seconds=settings.policy_timeout_seconds,
            ),
        )
        _policy_clients[key] = client
    return client


def reset_policy_clients() -> None:
    _policy_clients.clear()

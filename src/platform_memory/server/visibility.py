"""Видимость памяти по principal (MEM-ADR-019; TAI-ADR-0031 §4–5, дизайн policy-service v0 §11).

Namespace остаётся жёсткой границей хранения, а внутри него элемент с scope
воркспейса или principal виден только тому, чьи разрешённые scopes его пересекают.
Кто решает, что разрешено:

* статический ключ (``CB_SERVER_API_KEY`` / реестр) — service-режим, ограничений нет;
* IAM-токен человека или агента — policy-service: ``list_objects(memory.read,
  memory_namespace)`` даёт namespaces, ``list_objects(memory.read, workspace)`` —
  scopes воркспейсов; плюс приватный scope ``principal:<sub>``;
* service account со scope ``memory:on-behalf`` (Control Plane за конечного
  principal) — передаёт ``allowedNamespaces`` и ``allowedScopes`` в теле запроса,
  которые вычислил тем же ``list_objects``; без них чтение отклоняется.

Поверх серверной видимости любой вызывающий может передать в теле read-запроса
``allowedNamespaces``/``allowedScopes`` — сужение (MEM-ADR-021): берётся пересечение
с серверным множеством, расширить его нельзя; пустой список — ничего, не «всё».

Policy-service недоступен — 503, не allow. Ответы кэшируются на
``CB_POLICY_CACHE_TTL_SECONDS`` по subject.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from platform_memory.core.config import Settings
from platform_memory.core.namespaces import (
    namespace_from_policy_object,
    principal_namespace,
    validate_namespace,
)
from platform_memory.core.scopes import MAX_ALLOWED_SCOPES, resolve_scopes
from platform_memory.server import iam

# SDK платформы импортируется только в server/iam.py (инвариант 1 AGENTS.md):
# здесь клиент policy и тип контекста берутся оттуда.
PolicyUnavailable = iam.PolicyUnavailable

SCOPE_ON_BEHALF = "memory:on-behalf"
ACTION_READ = "memory.read"
POLICY_UNAVAILABLE_DETAIL = "policy-service недоступен — видимость памяти не определена"
ON_BEHALF_DETAIL = (
    "service account с memory:on-behalf обязан передать allowedNamespaces и allowedScopes"
)


@dataclass(frozen=True)
class VisibleSet:
    """Что вызывающему можно читать: None — без ограничения (service-режим)."""

    namespaces: frozenset[str] | None = None
    scopes: tuple[str, ...] | None = None

    @property
    def restricted(self) -> bool:
        return self.namespaces is not None

    def denied_namespace(self, namespaces: Iterable[str]) -> str | None:
        if self.namespaces is None:
            return None
        for ns in namespaces:
            if ns and ns not in self.namespaces:
                return ns
        return None


UNRESTRICTED = VisibleSet()


class PolicyLookup:
    """Протокол клиента policy-service, который нужен слою видимости."""

    async def list_objects(
        self,
        ctx: Any,
        action: str,
        resource_type: str,
        *,
        on_behalf_of: str | None = None,
        consistency: str = "default",
        cursor: str | None = None,
        limit: int = 1000,
    ) -> Any: ...


def _default_client(settings: Settings) -> PolicyLookup:
    return iam.build_policy_client(settings)


# Точка подмены для тестов: клиент policy без сети.
policy_client_factory: Callable[[Settings], PolicyLookup] = _default_client

_cache: dict[str, tuple[float, VisibleSet]] = {}


def reset() -> None:
    iam.reset_policy_clients()
    _cache.clear()


async def resolve_visibility(
    settings: Settings, subject: Any, body: dict[str, Any] | None
) -> VisibleSet:
    """Разрешённые namespaces и scopes вызывающего (см. docstring модуля)."""
    if not settings.policy_enabled or subject is None:
        return UNRESTRICTED
    if subject.has_scope(SCOPE_ON_BEHALF):
        return _from_body(body or {})
    key = f"{subject.tenant_id}|{subject.principal_id}"
    cached = _cache.get(key)
    now = time.monotonic()
    if cached is not None and now - cached[0] <= settings.policy_cache_ttl_seconds:
        return cached[1]
    visible = await _from_policy(settings, subject)
    _cache[key] = (now, visible)
    return visible


async def resolve_write_visibility(
    settings: Settings, subject: Any, body: dict[str, Any] | None
) -> VisibleSet:
    """Видимость пишущего: чьи существующие объекты он может перезаписать (MEM-ADR-019).

    То же, что :func:`resolve_visibility`, кроме service account'а с
    ``memory:on-behalf`` без ``allowedNamespaces``/``allowedScopes`` в теле: он пишет
    от себя (ингест документов и наблюдений ядром), как статический ключ, — без
    ограничения. С полями в теле — пишет от имени principal и ограничен ими.
    """
    if (
        settings.policy_enabled
        and subject is not None
        and subject.has_scope(SCOPE_ON_BEHALF)
        and not any(
            k in (body or {})
            for k in ("allowedNamespaces", "allowed_namespaces", "allowedScopes", "allowed_scopes")
        )
    ):
        return UNRESTRICTED
    return await resolve_visibility(settings, subject, body)


def _from_body(body: dict[str, Any]) -> VisibleSet:
    namespaces = body.get("allowedNamespaces", body.get("allowed_namespaces"))
    scopes = body.get("allowedScopes", body.get("allowed_scopes"))
    if not isinstance(namespaces, list) or not isinstance(scopes, list):
        raise HTTPException(status_code=403, detail=ON_BEHALF_DETAIL)
    return VisibleSet(
        namespaces=frozenset(str(ns) for ns in namespaces),
        scopes=tuple(str(s) for s in scopes),
    )


async def _from_policy(settings: Settings, subject: Any) -> VisibleSet:
    client = policy_client_factory(settings)
    principal = str(subject.principal_id)
    tenant = str(subject.tenant_id)
    try:
        namespaces_page = await client.list_objects(
            subject, ACTION_READ, "memory_namespace", on_behalf_of=principal
        )
        workspaces_page = await client.list_objects(
            subject, ACTION_READ, "workspace", on_behalf_of=principal
        )
    except PolicyUnavailable as exc:
        raise HTTPException(status_code=503, detail=POLICY_UNAVAILABLE_DETAIL) from exc
    namespaces: set[str] = set()
    for obj in namespaces_page.objects:
        ns = namespace_from_policy_object(tenant, str(obj))
        if ns:
            namespaces.add(ns)
    # Приватный namespace principal всегда его (owner в модели policy), даже если
    # проекция IAM ещё не доехала.
    namespaces.add(principal_namespace(tenant, principal))
    scopes = [f"workspace:{w}" for w in workspaces_page.objects] + [f"principal:{principal}"]
    return VisibleSet(namespaces=frozenset(namespaces), scopes=tuple(dict.fromkeys(scopes)))


def narrow_visibility(visible: VisibleSet, body: dict[str, Any] | None) -> VisibleSet:
    """Клиентское сужение видимости (MEM-ADR-021): ``effective = server ∩ client``.

    Поле отсутствует или ``null`` — серверная видимость без изменений; серверное
    множество ``None`` (без ограничения) — берётся клиентское. Сужение не источник
    прав: элемент, которого нет в серверном множестве, не добавится никогда.
    Пустой список — пустое множество (ничего), а не снятие фильтра.
    """
    if not body:
        return visible
    raw_namespaces = body.get("allowedNamespaces", body.get("allowed_namespaces"))
    raw_scopes = body.get("allowedScopes", body.get("allowed_scopes"))
    namespaces = visible.namespaces
    if raw_namespaces is not None:
        client_ns = _client_list(raw_namespaces, "allowedNamespaces")
        try:
            wanted = frozenset(validate_namespace(ns) for ns in client_ns)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        namespaces = wanted if namespaces is None else namespaces & wanted
    scopes = visible.scopes
    if raw_scopes is not None:
        client_scopes = _client_list(raw_scopes, "allowedScopes")
        try:
            wanted_scopes = resolve_scopes(client_scopes, limit=MAX_ALLOWED_SCOPES)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if scopes is None:
            scopes = tuple(wanted_scopes)
        else:
            keep = set(wanted_scopes)
            scopes = tuple(s for s in scopes if s in keep)
    return VisibleSet(namespaces=namespaces, scopes=scopes)


def _client_list(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise HTTPException(status_code=400, detail=f"{field} должен быть списком строк")
    return value


def check_visible(visible: VisibleSet, namespaces: Iterable[str], default: str = "") -> None:
    """403, если запрошенный namespace вне видимости principal.

    Пустое имя (``""``/пустой список — «дефолт движка») сверяется как ``default``,
    как и в грантах: иначе запрос без scope обходил бы ограничение namespaces.
    """
    resolved = [ns or default for ns in namespaces] or [default]
    denied = visible.denied_namespace(resolved)
    if denied is not None:
        raise HTTPException(status_code=403, detail=f"Namespace вне видимости principal: {denied}")

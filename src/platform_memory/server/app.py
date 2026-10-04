"""HTTP-сервис памяти platform_memory.

Машинный интерфейс к графу для агентов и интеграций. Только чтение/запись памяти
(граф знаний + векторный индекс); запись в vault невозможна.

Эндпоинты:
  GET  /healthz                  — живость + базовая статистика (без авторизации)
  POST /api/brain/query          — вектор+FTS+граф; контекст и источники (опц. LLM-ответ)
  GET  /api/brain/nodes          — список узлов с фильтром по типу/статусу
  GET  /api/brain/nodes/{key}    — узел по natural_key + соседи по графу
  GET  /api/brain/sources/{key}  — оригинал статьи целиком + provenance (source-view)
  GET  /api/brain/stats          — узлы/рёбра по типам, число чанков
  POST /api/brain/facts          — записать факт авторства агента в граф+индекс
  POST /api/brain/recall (/recall)   — релевантный контекст с цитатами (плагин)
  POST /api/brain/search (/search)   — точечный поиск узлов/фактов (плагин)
  POST /api/brain/retain (/retain)   — идемпотентная запись факта/решения (плагин)
  POST /api/brain/audit  (/audit)    — событие аудита как узел/ребро трейса (плагин)
  GET  /api/brain/trace/{trace_id}   — подграф трейса задачи
  POST /api/brain/documents          — батчевый ингест документа (узел + чанки)
  DELETE /api/brain/documents/{key}  — удалить документ (узел + рёбра + чанки, с аудитом)
  GET  /api/brain/health (/health)   — живость + проверка токена (плагин)

Авторизация: если задан CB_SERVER_API_KEY (или CB_SERVER_API_KEYS_PII, или реестр
CB_API_KEYS, или включён CB_IAM_ENABLED) — требуется заголовок
`Authorization: Bearer <token>`. Legacy-ключи — full-access по всем базам знаний;
ключи реестра CB_API_KEYS несут префиксные гранты по namespace (ADR-017, см.
``server/auth.py``); access token IAM (ADR-018, ``server/iam.py``) проецируется в те
же гранты по tenant_id и scope. Каждый data-эндпоинт авторизует namespaces запроса
против грантов вызывающего (403 при отказе).

NB: control plane (агенты/задачи/approvals/heartbeat) НЕ входит в platform_memory —
он живёт в platform_orchestrator (M2). Этот сервис инициализирует только схему памяти
(граф знаний + таблица чанков).
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import re
import time
import uuid
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from typing import Any

import psycopg
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from starlette.datastructures import MutableHeaders
from pydantic import BaseModel, field_validator

from platform_memory.core import db as dbmod
from platform_memory.core import timing
from platform_memory.core.config import get_settings
from platform_memory.core.kinds import AttributesError, UnknownKindError, UnknownRelationError
from platform_memory.core.namespaces import resolve_namespaces, validate_namespace
from platform_memory.core.pii import detect_pii, mask_pii
from platform_memory.graph import GraphStore
from platform_memory.index import VectorIndex
from platform_memory.retrieval import query as run_query
from platform_memory.retrieval import retrieve, write_and_index_fact
from platform_memory.retrieval.documents import (
    MAX_CHUNKS_PER_REQUEST,
    delete_document,
    retain_document,
)
from platform_memory.server import iam
from platform_memory.server.visibility import (
    VisibleSet,
    check_visible,
    narrow_visibility,
    resolve_visibility,
    resolve_write_visibility,
)
from platform_memory.core.scopes import (
    MAX_ITEM_SCOPES,
    ForeignObjectError,
    audit_scopes,
    is_visible,
    node_scopes,
    resolve_scopes,
)
from platform_memory.server.auth import (
    ApiKeysConfigError,
    CallerGrants,
    check_grants,
    match_key,
    parse_api_keys,
)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """При старте — создать схему памяти (best-effort; БД может ещё подниматься)."""
    try:
        settings = get_settings()
        conn = dbmod.connect(settings)
        try:
            GraphStore(conn, settings.graph_name, settings.default_namespace).ensure_schema()
            VectorIndex(conn, settings.chunks_table, settings.embedding_dim).ensure_schema()
            # Context Memory Engine (ADR-016): observations + трейсы компиляций.
            from platform_memory.context.trace import ContextTraceStore
            from platform_memory.observations.store import ObservationStore

            ObservationStore(
                conn, settings.observations_table, settings.default_namespace
            ).ensure_schema()
            ContextTraceStore(
                conn, settings.context_traces_table, settings.default_namespace
            ).ensure_schema()
            # Доменные пакеты видов и журнал снимков источников (MEM-ADR-020).
            from platform_memory.domain.reconcile import ledger_for
            from platform_memory.domain.registry import open_registry

            open_registry(conn, settings).ensure_schema()
            ledger_for(settings, conn).ensure_schema()
            # Индекс эмбеддингов сущностей видов с searchable (амендмент 2026-09-28).
            from platform_memory.domain.searchable import entity_index

            entity_index(conn, settings).ensure_schema()
        finally:
            conn.close()
    except Exception:  # noqa: BLE001
        pass  # БД недоступна на старте — схема создастся при первом обращении/init-db
    yield


app = FastAPI(
    title="platform_memory — Memory API",
    version="0.1.0",
    summary="Граф знаний компании + retrieval с цитатами на источники.",
    lifespan=lifespan,
)


class ServerTimingMiddleware:
    """Фазовый тайминг запроса при CB_SERVER_TIMING (core/timing.py, амендмент MEM-ADR-020).

    Заводит набор фаз запроса; фазы HTTP-слоя (auth, visibility, protect) и движка
    (``typed.*``) сливаются в него. ``total`` — от входа в приложение до начала ответа
    (включая сериализацию). Ответ — с заголовком ``Server-Timing``, в лог — строка по
    фазам (логгер ``platform_memory.timing``).
    """

    def __init__(self, asgi_app: Any) -> None:
        self.app = asgi_app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http" or not get_settings().server_timing:
            await self.app(scope, receive, send)
            return
        phases, token = timing.start_request()
        t0 = time.perf_counter()

        async def _send(message: Any) -> None:
            if message["type"] == "http.response.start":
                phases.add("total", time.perf_counter() - t0)
                MutableHeaders(scope=message).append("Server-Timing", phases.header())
                _timing_log.info("%s %s %s", scope["method"], scope["path"], phases.header())
            await send(message)

        try:
            await self.app(scope, receive, _send)
        finally:
            timing.end_request(token)


app.add_middleware(ServerTimingMiddleware)
_timing_log = logging.getLogger("platform_memory.timing")


@app.exception_handler(psycopg.errors.DataError)
async def _data_error_handler(_request: Request, exc: psycopg.errors.DataError) -> JSONResponse:
    """Некорректные данные (например, невалидный uuid) -> 400, а не 500."""
    return JSONResponse(status_code=400, content={"detail": f"Некорректные данные запроса: {exc}"})


@app.exception_handler(dbmod.GraphKeyTooLong)
async def _graph_key_handler(_request: Request, exc: dbmod.GraphKeyTooLong) -> JSONResponse:
    """Ключ узла или метка длиннее предела индекса -> 400 с кодом в начале ``detail``.

    Те же 400 и текст дают маршруты, которые сами переводят ``ValueError`` в 400:
    ``natural_key_too_long: …`` / ``label_too_long: …`` (MEM-ADR-023).
    """
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(UnknownKindError)
@app.exception_handler(AttributesError)
@app.exception_handler(UnknownRelationError)
async def _kind_error_handler(_request: Request, exc: ValueError) -> JSONResponse:
    """Сущность/связь неизвестного вида или вне схемы в строгом namespace -> 422 (MEM-ADR-020)."""
    return JSONResponse(status_code=422, content={"detail": str(exc)})


@dataclass(slots=True)
class Principal:
    """Вызывающий: допуск к ПДн (full/masked) и, опционально, гранты по namespace.

    ``grants=None`` — гранты не настроены (legacy-ключ, PII-ключ, локальный режим):
    доступ ко всем базам знаний, как до ADR-017. Ключ из реестра CB_API_KEYS и access
    token IAM (ADR-018) несут ``CallerGrants``, и ``_authorize`` сверяет с ними
    namespaces каждого запроса.
    """

    access: str = "full"  # "full" | "masked"
    label: str = "default"  # метка токена для журнала доступа (сам токен не пишем)
    grants: CallerGrants | None = None
    # Проверенный контекст IAM (subject решения policy-service, MEM-ADR-019);
    # None — статический ключ или локальный режим.
    subject: Any = None


AUTH_REQUIRED_DETAIL = "Требуется корректный Authorization: Bearer <key>"
IAM_UNAVAILABLE_DETAIL = "Проверка IAM-токена недоступна (JWKS/конфигурация IAM)"


async def authenticate(authorization: str | None = Header(default=None)) -> Principal:
    """Проверить Bearer-токен, определить допуск к ПДн (ADR-002) и гранты (ADR-017/018).

    Порядок совпадений: токены CB_SERVER_API_KEYS_PII — полный допуск ко всем KB;
    базовый CB_SERVER_API_KEY — все KB, при включённой CB_PII_PROTECTION маскированная
    выдача (иначе, как раньше, полный); ключ реестра CB_API_KEYS — гранты записи,
    допуск к ПДн по флагу ``pii`` записи (без него — как у базового ключа); при
    CB_IAM_ENABLED Bearer, не совпавший ни с одним статическим ключом и похожий на JWT,
    проверяется как access token IAM (``server/iam.py``) — гранты по tenant_id и scope,
    допуск к ПДн по scope ``memory:pii``. Любой дефект токена — тот же 401, что у
    неверного ключа; недоступность проверки (JWKS) — 503, не allow.
    Без настроенных токенов и с выключенным IAM — локальный режим без авторизации.
    """
    settings = get_settings()
    key = settings.server_api_key
    pii_keys = [k.strip() for k in settings.server_api_keys_pii.split(",") if k.strip()]
    try:
        registry = parse_api_keys(settings.api_keys)
    except ApiKeysConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not key and not pii_keys and not registry and not settings.iam_enabled:
        return Principal()  # авторизация не настроена (локальный режим)
    header = authorization if isinstance(authorization, str) else None
    if header is not None:
        # Сравнение в постоянном времени — против timing-side-channel по токену.
        for pk in pii_keys:
            if hmac.compare_digest(header, f"Bearer {pk}"):
                return Principal(access="full", label="pii-full")
        if key and hmac.compare_digest(header, f"Bearer {key}"):
            access = "masked" if settings.pii_protection else "full"
            return Principal(access=access, label="base")
        if header.startswith("Bearer "):
            token = header.removeprefix("Bearer ")
            grants = match_key(token, registry) if registry else None
            subject = None
            if grants is None and settings.iam_enabled and iam.looks_like_jwt(token.strip()):
                try:
                    grants, subject = await iam.verify_token_with_context(settings, token.strip())
                except iam.IamAuthError as exc:
                    raise HTTPException(status_code=401, detail=AUTH_REQUIRED_DETAIL) from exc
                except iam.IamUnavailable as exc:
                    raise HTTPException(status_code=503, detail=IAM_UNAVAILABLE_DETAIL) from exc
            if grants is not None:
                full = grants.pii or not settings.pii_protection
                return Principal(
                    access="full" if full else "masked",
                    label=grants.name,
                    grants=grants,
                    subject=subject,
                )
    raise HTTPException(status_code=401, detail=AUTH_REQUIRED_DETAIL)


def require_auth(_principal: Principal = Depends(authenticate)) -> None:
    """Совместимая зависимость: только проверка токена, без допуска (см. authenticate)."""
    return None


async def visibility(request: Request, principal: Principal = Depends(authenticate)) -> VisibleSet:
    """Разрешённые namespaces и scopes вызывающего (MEM-ADR-019/021, server/visibility.py).

    Тело читается только ради ``allowedNamespaces``/``allowedScopes``: видимость service
    account'а с ``memory:on-behalf`` и сужение любого вызывающего; Starlette кэширует
    тело, повторный разбор в хендлере бесплатен.
    """
    body: dict[str, Any] | None = None
    if request.method in {"POST", "PUT", "PATCH"}:
        try:
            parsed = await request.json()
        except Exception:  # noqa: BLE001 - тело не JSON: видимость по policy/грантам
            parsed = None
        body = parsed if isinstance(parsed, dict) else None
    subject = principal.subject if isinstance(principal, Principal) else None
    visible = await resolve_visibility(get_settings(), subject, body)
    # Клиентское сужение — пересечение с серверной видимостью, не расширение (MEM-ADR-021).
    return narrow_visibility(visible, body)


async def write_visibility(
    request: Request, principal: Principal = Depends(authenticate)
) -> VisibleSet:
    """Видимость пишущего для записи по ключу существующего объекта (MEM-ADR-019).

    Как :func:`visibility`, но service account с ``memory:on-behalf`` без
    ``allowedNamespaces``/``allowedScopes`` пишет от себя, без ограничения
    (``resolve_write_visibility``). Namespace записи решают гранты, а не видимость.
    """
    body: dict[str, Any] | None = None
    try:
        parsed = await request.json()
    except Exception:  # noqa: BLE001 - тело не JSON: его отвергнет валидация модели
        parsed = None
    body = parsed if isinstance(parsed, dict) else None
    subject = principal.subject if isinstance(principal, Principal) else None
    visible = await resolve_write_visibility(get_settings(), subject, body)
    return narrow_visibility(visible, body)


def _foreign_write(exc: ForeignObjectError) -> HTTPException:
    """Отказ записи по ключу чужого объекта: как отсутствие права, без полей объекта."""
    return HTTPException(status_code=403, detail=str(exc))


def _authorize(principal: Any, namespaces: Iterable[str], *, write: bool) -> None:
    """Авторизовать namespaces запроса против грантов вызывающего (403 при отказе).

    Пустое имя (``""``/пустой список — «дефолт движка») сверяется как default
    namespace сервиса. При прямом вызове хендлера в тестах параметр-``Depends``
    приходит объектом-дефолтом — считаем это локальным режимом (гранты не настроены).
    """
    grants = principal.grants if isinstance(principal, Principal) else None
    if grants is None:
        return
    default = get_settings().default_namespace
    resolved = [ns or default for ns in namespaces] or [default]
    denied = check_grants(grants, resolved, write=write)
    if denied is not None:
        raise HTTPException(
            status_code=403,
            detail=f"Нет прав на namespace: {denied} ({'запись' if write else 'чтение'})",
        )


def _authorize_global(principal: Any) -> None:
    """Глобальная (без namespace) выдача — только грантам с пустым префиксом."""
    grants = principal.grants if isinstance(principal, Principal) else None
    if grants is not None and check_grants(grants, [""], write=False) is not None:
        raise HTTPException(status_code=403, detail="Нет прав на глобальную статистику")


def _authorize_service(principal: Any) -> None:
    """Service scope (MEM-ADR-020): регистрация доменных пакетов видов.

    Разрешено локальному режиму и legacy-ключам (гранты не настроены), ключу реестра
    с ``"service": true``, IAM-токену со scope ``memory:service`` и гранту на запись
    с пустым префиксом (full-access ключ реестра). Остальным — 403.
    """
    grants = principal.grants if isinstance(principal, Principal) else None
    if grants is None or grants.service:
        return
    if check_grants(grants, [""], write=True) is None:
        return
    raise HTTPException(status_code=403, detail="Нужен service scope (регистрация пакетов видов)")


def core_identities(settings: Any) -> frozenset[str] | None:
    """Метки identity ядра; None — ограничение маршрутов ядра выключено."""
    names = frozenset(x.strip() for x in settings.core_identities.split(",") if x.strip())
    if not names and not settings.core_only:
        return None
    return names


def _authorize_core(principal: Any) -> None:
    """Маршруты ядра (амендмент MEM-ADR-020): reconcile, пакеты, виды namespace.

    Ограничение выключено (нет ни CB_CORE_IDENTITIES, ни CB_CORE_ONLY) — пропускает
    всех, дальше решают гранты. Включено — только identity ядра: вызывающий с service
    scope (IAM ``memory:service``, ключ реестра с ``"service": true``) или чья метка в
    CB_CORE_IDENTITIES; прочим 403, даже с грантом на namespace. Локальный режим без
    аутентификации ядра не опознаёт — тоже 403 (fail closed), если метка ``default``
    не перечислена явно.
    """
    names = core_identities(get_settings())
    if names is None:
        return
    if isinstance(principal, Principal):
        if principal.grants is not None and principal.grants.service:
            return
        if principal.label in names:
            return
    raise HTTPException(
        status_code=403,
        detail="Маршрут доступен только identity ядра (memory:service / CB_CORE_IDENTITIES)",
    )


# --- модели запросов/ответов ---


class QueryRequest(BaseModel):
    question: str
    k: int = 8
    hops: int = 1
    synthesize: bool | None = None  # None -> значение по умолчанию из конфига
    scope: dict | None = None  # {"namespace": "…"} | {"namespaces": [...]} — изоляция KB


# --- эндпоинты ---


@app.get("/healthz")
def healthz() -> dict[str, Any]:
    """Живость сервиса и базовая статистика графа."""
    settings = get_settings()
    try:
        conn = dbmod.connect(settings)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"БД недоступна: {exc}") from exc
    try:
        graph = GraphStore(conn, settings.graph_name, settings.default_namespace)
        index = VectorIndex(conn, settings.chunks_table, settings.embedding_dim)
        nodes = graph.total_nodes()
        chunks = index.count_chunks() if index.table_exists() else 0
    finally:
        conn.close()
    return {"ok": True, "graph": settings.graph_name, "nodes": nodes, "chunks": chunks}


@app.post("/api/brain/query")
def brain_query(
    req: QueryRequest,
    principal: Principal = Depends(authenticate),
    visible: VisibleSet = Depends(visibility),
    x_run_id: str | None = Header(default=None, alias="X-Run-Id"),
) -> dict[str, Any]:
    """Вопрос → найденные фрагменты, соседи по графу и источники (опц. LLM-ответ)."""
    settings = get_settings()
    synthesize = settings.query_synthesize_default if req.synthesize is None else req.synthesize
    nss = _scope_namespaces(req.scope)
    _authorize(principal, nss, write=False)
    check_visible(_visible(visible), nss, get_settings().default_namespace)
    allowed = _visible(visible).scopes
    try:
        if synthesize:
            ans = run_query(
                settings,
                req.question,
                k=req.k,
                hops=req.hops,
                namespaces=nss,
                allowed_scopes=allowed,
            )
            payload = {
                "question": req.question,
                "scope": req.scope,
                "synthesized": True,
                "answer": ans.text,
                "sources": [asdict(s) for s in ans.sources],
                "hits": ans.hits,
                "neighbors": ans.neighbors,
            }
        else:
            r = retrieve(
                settings,
                req.question,
                k=req.k,
                hops=req.hops,
                namespaces=nss,
                allowed_scopes=allowed,
            )
            payload = {
                "question": req.question,
                "scope": req.scope,
                "synthesized": False,
                "hits": [asdict(h) for h in r.hits],
                "neighbors": r.neighbors,
                "sources": [asdict(s) for s in r.sources],
                "context": r.context,
            }
        return protect_pii_response(
            payload, principal, "query", _hdr_str(x_run_id), nss[0] if nss else ""
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/brain/nodes")
def brain_nodes(
    type: str | None = Query(default=None, description="Фильтр по типу узла (decision, task, …)"),
    status: str | None = Query(default=None, description="Фильтр по статусу (Proposed, todo, …)"),
    limit: int = Query(default=50, ge=1, le=500),
    namespace: str = Query(default="", description="База знаний (пусто — дефолтная)"),
    principal: Principal = Depends(authenticate),
    visible: VisibleSet = Depends(visibility),
) -> dict[str, Any]:
    """Список узлов с фильтром по типу и статусу (структурные запросы к графу)."""
    ns = _query_namespace(namespace)
    _authorize(principal, [ns], write=False)
    check_visible(_visible(visible), [ns], get_settings().default_namespace)
    settings = get_settings()
    conn = dbmod.connect(settings)
    try:
        nodes = GraphStore(conn, settings.graph_name, settings.default_namespace).list_nodes(
            type=type, status=status, limit=limit, namespaces=[ns] if ns else []
        )
    finally:
        conn.close()
    nodes = _visible_nodes(nodes, visible)
    return {"count": len(nodes), "nodes": nodes}


def _visible(visible: Any) -> VisibleSet:
    """При прямом вызове хендлера (тесты) Depends приходит объектом-дефолтом."""
    return visible if isinstance(visible, VisibleSet) else VisibleSet()


def _visible_nodes(nodes: list[dict[str, Any]], visible: Any) -> list[dict[str, Any]]:
    """Оставить узлы, видимые вызывающему по ``props.scopes`` (MEM-ADR-019)."""
    allowed = _visible(visible).scopes
    if allowed is None:
        return nodes
    return [n for n in nodes if is_visible(node_scopes(n), allowed)]


# retain кладёт external_id=URL как natural_key, а reverse-proxy (traefik) схлопывает
# `//` в пути: `https://x` доезжает как `https:/x`. Восстанавливаем схему.
_COLLAPSED_SCHEME = re.compile(r"^([a-z][a-z0-9+.-]*):/(?!/)")


def normalize_natural_key(natural_key: str) -> str:
    """Восстановить `scheme://` в ключе-URL, схлопнутом прокси до `scheme:/`."""
    return _COLLAPSED_SCHEME.sub(r"\1://", natural_key)


# --- изоляция баз знаний: scope.namespace → движок (ADR-003) ---


def _scope_namespaces(scope: dict | None) -> list[str]:
    """Разобрать scope чтения: {"namespace": "x"} | {"namespaces": [...]} → список.

    Пусто → [] (дефолтный namespace применяет движок — обратная совместимость).
    Невалидное имя или слишком длинный список → 400.
    """
    if not scope:
        return []
    try:
        items = scope.get("namespaces")
        if items is not None:
            if not isinstance(items, list):
                raise ValueError("scope.namespaces должен быть списком строк")
            return resolve_namespaces(items) if items else []
        ns = scope.get("namespace")
        return [validate_namespace(ns)] if ns else []
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _scope_namespace(scope: dict | None) -> str:
    """Разобрать scope записи: ровно один namespace; пусто → "" (дефолт движка)."""
    ns = (scope or {}).get("namespace") or ""
    if not ns:
        return ""
    try:
        return validate_namespace(ns)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _query_namespace(namespace: str) -> str:
    """Валидировать query-параметр namespace узловых/трейс-маршрутов (пусто → дефолт).

    При прямом вызове хендлера (тесты) параметр-``Query`` приходит объектом-дефолтом —
    считаем его пустым, как и ``_hdr_str`` для заголовков.
    """
    if not isinstance(namespace, str) or not namespace:
        return ""
    try:
        return validate_namespace(namespace)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# --- защита ПДн на выдаче (152-ФЗ, ADR-002) ---

# Текстовые поля ответов, в которых могут оказаться ПДн (на любой глубине вложенности).
_PII_TEXT_FIELDS = frozenset({"text", "context", "content", "title", "answer", "heading"})


def _walk_texts(payload: Any, transform) -> Any:
    """Рекурсивно применить transform к значениям текстовых полей выдачи."""
    if isinstance(payload, dict):
        return {
            k: (
                transform(v)
                if k in _PII_TEXT_FIELDS and isinstance(v, str)
                else _walk_texts(v, transform)
            )
            for k, v in payload.items()
        }
    if isinstance(payload, list):
        return [_walk_texts(item, transform) for item in payload]
    return payload


def protect_pii_response(
    payload: dict[str, Any],
    principal: Principal,
    route: str,
    trace_id: str | None,
    namespace: str = "",
    scopes: Iterable[str] = (),
) -> dict[str, Any]:
    """Применить допуск к ПДн на исходящем ответе.

    masked — маскируем текстовые поля по паттернам (двойной барьер: даже
    непомеченные при записи ПДн не утекут) и помечаем ответ ``pii_masked``;
    full — если в выдаче есть ПДн, пишем событие журнала доступа ``pii_access``
    (учёт операций по 152-ФЗ) отдельным коротким соединением. Ошибка журнала
    выдачу не роняет. ``scopes`` — scopes выданного объекта: событие несёт их в
    ``payload.scopes``, и трейс скрывает его от того, кому объект не виден (MEM-ADR-019).
    """
    settings = get_settings()
    if not isinstance(principal, Principal):
        principal = Principal()  # прямой вызов хендлера (тесты): объект-дефолт Depends
    # Маскированный принципал маскирует ВСЕГДА, даже при выключенном глобальном флаге:
    # публичная демо-витрина (ADR-009) форсирует masked и обязана не отдавать ПДн
    # наружу независимо от CB_PII_PROTECTION. Регулярный API получает access=="masked"
    # только при уже включённой защите (см. authenticate), поэтому его поведение не
    # меняется — при выключенной защите его принципал "full" и выдача идёт как прежде.
    if not settings.pii_protection and principal.access != "masked":
        return payload  # защита выключена и принципал не маскированный — поведение прежнее
    found_total: dict[str, int] = {}
    if principal.access == "masked":

        def _mask(value: str) -> str:
            masked, found = mask_pii(value)
            for cat, n in found.items():
                found_total[cat] = found_total.get(cat, 0) + n
            return masked

        payload = _walk_texts(payload, _mask)
        payload["pii_masked"] = True
        return payload

    # full-допуск: журнал доступа к ПДн
    def _detect(value: str) -> str:
        for cat, n in detect_pii(value).items():
            found_total[cat] = found_total.get(cat, 0) + n
        return value

    _walk_texts(payload, _detect)
    if found_total:
        try:
            conn = dbmod.connect(settings)
            try:
                GraphStore(conn, settings.graph_name, settings.default_namespace).record_audit(
                    trace_id or uuid.uuid4().hex,
                    principal.label,
                    "pii_access",
                    payload={
                        "route": route,
                        "categories": found_total,
                        **audit_scopes(scopes),
                    },
                    namespace=namespace,  # журнал доступа — в аудит-контуре той же KB
                )
            finally:
                conn.close()
        except Exception:  # noqa: BLE001 — журнал не должен ронять выдачу
            pass
    return payload


@app.get("/api/brain/nodes/{natural_key:path}")
def brain_node(
    natural_key: str,
    hops: int = Query(default=1, ge=1, le=2),
    namespace: str = Query(default="", description="База знаний (пусто — дефолтная)"),
    principal: Principal = Depends(authenticate),
    visible: VisibleSet = Depends(visibility),
    x_run_id: str | None = Header(default=None, alias="X-Run-Id"),
) -> dict[str, Any]:
    """Узел по natural_key и его соседи по графу. Ключ принимает слэши (URL-ключи retain).

    Узел со scope вне видимости вызывающего — 404, неотличимо от отсутствующего;
    невидимые соседи отбрасываются, как в recall (MEM-ADR-019).
    """
    natural_key = normalize_natural_key(natural_key)
    ns = _query_namespace(namespace)
    _authorize(principal, [ns], write=False)
    check_visible(_visible(visible), [ns], get_settings().default_namespace)
    nss = [ns] if ns else []
    settings = get_settings()
    conn = dbmod.connect(settings)
    try:
        graph = GraphStore(conn, settings.graph_name, settings.default_namespace)
        node = graph.get_node(natural_key, namespaces=nss)
        if node is None or not _visible_nodes([node], visible):
            raise HTTPException(status_code=404, detail=f"Узел не найден: {natural_key}")
        neighbors = graph.expand_neighbors([natural_key], hops=hops, namespaces=nss)
    finally:
        conn.close()
    neighbors = _visible_nodes(neighbors, visible)
    payload = {"node": node, "neighbors": neighbors}
    return protect_pii_response(
        payload, principal, "node", _hdr_str(x_run_id), ns, scopes=node_scopes(node)
    )


@app.get("/api/brain/sources/{natural_key:path}")
def brain_source(
    natural_key: str,
    namespace: str = Query(default="", description="База знаний (пусто — дефолтная)"),
    principal: Principal = Depends(authenticate),
    visible: VisibleSet = Depends(visibility),
    x_run_id: str | None = Header(default=None, alias="X-Run-Id"),
) -> dict[str, Any]:
    """Оригинал статьи целиком: полный текст, provenance и traceable-метаданные.

    Source-view для потребителя: по клику на цитату показать документ-источник.
    ``content`` — оригинал, сохранённый retain'ом; если узел без сохранённого
    оригинала (например, спроецирован из vault) — текст восстанавливается из
    чанков индекса по порядку (``content_source: "chunks"``). Узел ищется строго
    в запрошенном namespace; выдача проходит ту же PII-защиту, что и остальные
    маршруты (маскирование/журнал доступа — ADR-002). Источник со scope вне
    видимости вызывающего — 404, неотличимо от отсутствующего (MEM-ADR-019).
    """
    natural_key = normalize_natural_key(natural_key)
    ns = _query_namespace(namespace)
    _authorize(principal, [ns], write=False)
    check_visible(_visible(visible), [ns], get_settings().default_namespace)
    settings = get_settings()
    conn = dbmod.connect(settings)
    try:
        graph = GraphStore(conn, settings.graph_name, settings.default_namespace)
        node = graph.get_node(natural_key, namespaces=[ns] if ns else [])
        if node is None or not _visible_nodes([node], visible):
            raise HTTPException(status_code=404, detail=f"Источник не найден: {natural_key}")
        index = VectorIndex(
            conn, settings.chunks_table, settings.embedding_dim, settings.default_namespace
        )
        chunk_texts = (
            index.texts_for_node(natural_key, namespace=ns) if index.table_exists() else []
        )
    finally:
        conn.close()
    props = node.get("props") if isinstance(node.get("props"), dict) else {}
    original = props.get("content")
    if isinstance(original, str) and original:
        content, content_source = original, "original"
    else:
        content, content_source = "\n\n".join(chunk_texts), "chunks"
    payload = {
        "natural_key": natural_key,
        "namespace": node.get("namespace"),
        "type": node.get("type"),
        "title": node.get("title"),
        "content": content,
        "content_source": content_source,
        "chunks": len(chunk_texts),
        "provenance": {
            "source": props.get("source"),
            "source_path": node.get("source_path"),
            "origin": node.get("origin"),
            "actor": props.get("actor"),
            "actor_id": props.get("actor_id"),
            "trace_id": props.get("trace_id"),
            "issue_id": props.get("issue_id"),
            "confidence": node.get("confidence"),
            "last_seen": node.get("last_seen"),
            "metadata": props.get("provenance") or {},
        },
        "pii": bool(props.get("pii", False)),
        "pii_categories": props.get("pii_categories") or [],
    }
    return protect_pii_response(
        payload, principal, "source", _hdr_str(x_run_id), ns, scopes=node_scopes(node)
    )


@app.delete("/api/brain/nodes/{natural_key:path}")
def brain_node_delete(
    natural_key: str,
    namespace: str = Query(default=""),
    actor: str = Query(default="api"),
    trace_id: str | None = Query(default=None),
    x_run_id: str | None = Header(default=None, alias="X-Run-Id"),
    principal: Principal = Depends(authenticate),
    visible: VisibleSet = Depends(visibility),
) -> dict[str, Any]:
    """Удалить узел из графа (с рёбрами) и его чанки из индекса — с обязательным аудитом.

    Каждый акт удаления фиксируется событием ``action="delete"`` со снапшотом
    удалённого узла; след восстанавливается по ``GET /api/brain/trace/{trace_id}``.
    Узлы аудит-контура (``audit_event``/``pc_trace``) защищены — 400.
    Ключ принимает слэши (``:path``): retain кладёт external_id=URL как natural_key.
    Узел со scope вне видимости вызывающего не удаляется — 404, неотличимо от
    отсутствующего (MEM-ADR-019).
    """
    natural_key = normalize_natural_key(natural_key)
    _authorize(principal, [namespace], write=True)
    check_visible(_visible(visible), [namespace], get_settings().default_namespace)
    settings = get_settings()
    trace = trace_id or _hdr_str(x_run_id) or uuid.uuid4().hex
    conn = dbmod.connect(settings)
    try:
        graph = GraphStore(conn, settings.graph_name, settings.default_namespace)
        try:
            snapshot = graph.delete_node(
                natural_key, namespace=namespace, allowed_scopes=_visible(visible).scopes
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if snapshot is None:
            raise HTTPException(status_code=404, detail=f"Узел не найден: {natural_key}")
        index = VectorIndex(
            conn, settings.chunks_table, settings.embedding_dim, settings.default_namespace
        )
        chunks_deleted = index.delete_for_node(
            natural_key, namespace=namespace, allowed_scopes=_visible(visible).scopes
        )
        graph.record_audit(
            trace,
            actor,
            "delete",
            payload={
                "natural_key": natural_key,
                "type": snapshot.get("type"),
                "title": snapshot.get("title"),
                "chunks_deleted": chunks_deleted,
                "namespace": snapshot.get("namespace"),
                # scope снимка — чтобы трейс не показал удалённый узел чужому (MEM-ADR-019)
                **audit_scopes(snapshot),
            },
            run_id=_hdr_str(x_run_id),
            namespace=namespace,
        )
    finally:
        conn.close()
    return {
        "deleted": True,
        "natural_key": natural_key,
        "type": snapshot.get("type"),
        "title": snapshot.get("title"),
        "chunks_deleted": chunks_deleted,
        "trace_id": trace,
    }


@app.get("/api/brain/stats")
def brain_stats(principal: Principal = Depends(authenticate)) -> dict[str, Any]:
    """Узлы по типам, рёбра по типам, число чанков (глобально по инстансу)."""
    _authorize_global(principal)
    settings = get_settings()
    conn = dbmod.connect(settings)
    try:
        graph = GraphStore(conn, settings.graph_name, settings.default_namespace)
        index = VectorIndex(conn, settings.chunks_table, settings.embedding_dim)
        nodes = graph.count_nodes_by_type()
        edges = graph.count_edges_by_type()
        chunks = index.count_chunks() if index.table_exists() else 0
    finally:
        conn.close()
    return {"nodes_by_type": nodes, "edges_by_type": edges, "chunks": chunks}


# --- write-back: факты авторства агента в граф ---


class FactReq(BaseModel):
    natural_key: str
    type: str
    title: str
    properties: dict | None = None
    links: list[str] | None = None
    run_id: str | None = None
    confidence: float = 0.8
    scope: dict | None = None  # {"namespace": "…"} — запись ровно в одну KB


@app.post("/api/brain/facts", status_code=201)
def write_fact(
    req: FactReq,
    x_run_id: str | None = Header(default=None, alias="X-Run-Id"),
    principal: Principal = Depends(authenticate),
    visible: VisibleSet = Depends(write_visibility),
) -> dict[str, Any]:
    """Записать факт авторства агента в граф И индекс (origin='agent').

    Переживает ре-ingest. В vault НЕ пишет. Узел ключа вне видимости вызывающего не
    перезаписывается — 403, как при отсутствии права; ``properties.scopes``
    существующего узла сливаются, а не заменяются (MEM-ADR-019).
    """
    settings = get_settings()
    ns = _scope_namespace(req.scope)
    _authorize(principal, [ns], write=True)
    try:
        return write_and_index_fact(
            settings,
            req.natural_key,
            req.type,
            req.title,
            properties=req.properties,
            links=req.links,
            run_id=req.run_id or _hdr_str(x_run_id),  # сквозной X-Run-Id, если тело не задало
            confidence=req.confidence,
            namespace=ns,
            allowed_scopes=_visible(visible).scopes,
        )
    except ForeignObjectError as exc:
        raise _foreign_write(exc) from exc


# ============================================================
# Plugin-facing memory & audit API
# ============================================================
#
# Тонкий, стабильный контракт для плагинов/агентов. Канонические пути — под общим
# префиксом `/api/brain/*`; продублированы bare-алиасами (`/recall`, …). Все, кроме
# `/healthz`, требуют Bearer-токен (если он настроен).

# Бюджет recall → сколько фрагментов тянуть (k вектора).
_RECALL_BUDGET_K = {"low": 4, "mid": 8, "high": 16}


def _first_line(text: str, limit: int = 120) -> str:
    """Первая непустая строка текста (для заголовка факта)."""
    stripped = (text or "").strip()
    if not stripped:
        return "(без названия)"
    return stripped.splitlines()[0][:limit]


def _hdr_str(value: Any) -> str | None:
    """Нормализовать значение заголовка: строка — как есть, иначе None.

    Хендлеры вызываются и через FastAPI (заголовок резолвится в строку/None), и напрямую
    в тестах (тогда параметр-``Header`` приходит как объект-дефолт). Сводим оба к строке/None.
    """
    return value if isinstance(value, str) else None


def _derive_fact_key(content: str, trace_id: str | None) -> str:
    """Стабильный natural_key факта по содержимому (для идемпотентного retain без external_id)."""
    digest = hashlib.sha1((content or "").encode("utf-8")).hexdigest()[:16]
    return f"fact:{trace_id}:{digest}" if trace_id else f"fact:{digest}"


class RecallRequest(BaseModel):
    query: str
    scope: dict | None = None
    budget: str = "mid"  # low | mid | high
    hops: int = 1


class SearchRequest(BaseModel):
    query: str
    filters: dict | None = None  # {type?, status?, limit?, meta?}
    scope: dict | None = None  # {"namespace": "…"} | {"namespaces": [...]} — изоляция KB


class RetainRequest(BaseModel):
    content: str
    type: str = "note"
    # Свободная сохраняемая metadata происхождения. Базовые поля используются
    # движком; Git-worker также передаёт commit-level provenance.
    provenance: dict | None = None
    scope: dict | None = None
    external_id: str | None = None  # идемпотентность retain
    title: str | None = None
    links: list[str] | None = None
    confidence: float = 0.8
    # Явная маркировка ПДн (152-ФЗ): дополняет авто-детекцию; категории — свободные
    # метки (например "fio", "address"), которые регекс-детектор покрыть не может.
    pii: bool | None = None
    pii_categories: list[str] | None = None


class AuditRequest(BaseModel):
    trace_id: str  # = issueId / runId
    action: str
    actor: str = ""
    payload: dict | None = None
    ts: str | None = None
    run_id: str | None = None
    actor_id: str | None = None
    actor_kind: str = "agent"
    scope: dict | None = None  # {"namespace": "…"} — аудит-контур конкретной KB


@app.post("/api/brain/recall")
@app.post("/recall")
def brain_recall(
    req: RecallRequest,
    principal: Principal = Depends(authenticate),
    visible: VisibleSet = Depends(visibility),
    x_run_id: str | None = Header(default=None, alias="X-Run-Id"),
) -> dict[str, Any]:
    """Релевантный контекст из графа+вектора с цитатами на источники (provenance)."""
    settings = get_settings()
    k = _RECALL_BUDGET_K.get((req.budget or "mid").lower(), 8)
    nss = _scope_namespaces(req.scope)
    _authorize(principal, nss, write=False)
    check_visible(_visible(visible), nss, get_settings().default_namespace)
    try:
        r = retrieve(
            settings,
            req.query,
            k=k,
            hops=req.hops,
            namespaces=nss,
            allowed_scopes=_visible(visible).scopes,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    memories = [
        {
            "node_key": h.node_key,
            "title": h.title,
            "heading": h.heading,
            "source_path": h.source_path,
            "text": h.text,
            "namespace": h.namespace,
        }
        for h in r.hits
    ]
    payload = {
        "query": req.query,
        "budget": req.budget,
        "scope": req.scope,
        "count": len(memories),
        "memories": memories,
        "sources": [asdict(s) for s in r.sources],
        "neighbors": r.neighbors,
        "context": r.context,
    }
    return protect_pii_response(
        payload, principal, "recall", _hdr_str(x_run_id), nss[0] if nss else ""
    )


@app.post("/api/brain/search")
@app.post("/search")
def brain_search(
    req: SearchRequest,
    principal: Principal = Depends(authenticate),
    visible: VisibleSet = Depends(visibility),
    x_run_id: str | None = Header(default=None, alias="X-Run-Id"),
) -> dict[str, Any]:
    """Точечный поиск узлов/фактов. С filters.type — структурный список; иначе вектор top-k.

    ``filters.meta`` — jsonb-containment по meta чанков (только семантический режим;
    например ``{"collection": "licenses"}`` для документов ``/api/brain/documents``).
    """
    settings = get_settings()
    filters = req.filters or {}
    node_type = filters.get("type")
    status = filters.get("status")
    meta_filter = filters.get("meta") if isinstance(filters.get("meta"), dict) else None
    nss = _scope_namespaces(req.scope)
    _authorize(principal, nss, write=False)
    check_visible(_visible(visible), nss, get_settings().default_namespace)
    try:
        limit = int(filters.get("limit", 20))
    except (TypeError, ValueError):
        limit = 20
    if node_type:
        conn = dbmod.connect(settings)
        try:
            results = GraphStore(conn, settings.graph_name, settings.default_namespace).list_nodes(
                type=node_type, status=status, limit=limit, namespaces=nss
            )
        finally:
            conn.close()
        results = _visible_nodes(results, visible)
        payload = {
            "query": req.query,
            "mode": "structural",
            "count": len(results),
            "results": results,
        }
        return protect_pii_response(
            payload, principal, "search", _hdr_str(x_run_id), nss[0] if nss else ""
        )
    try:
        r = retrieve(
            settings,
            req.query,
            k=limit,
            namespaces=nss,
            meta_filter=meta_filter,
            allowed_scopes=_visible(visible).scopes,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    results = [
        {
            "node_key": h.node_key,
            "title": h.title,
            "source_path": h.source_path,
            "text": h.text,
            "namespace": h.namespace,
            "meta": h.meta,
        }
        for h in r.hits
    ]
    payload = {
        "query": req.query,
        "mode": "semantic",
        "count": len(results),
        "results": results,
        "sources": [asdict(s) for s in r.sources],
    }
    return protect_pii_response(
        payload, principal, "search", _hdr_str(x_run_id), nss[0] if nss else ""
    )


@app.post("/api/brain/retain", status_code=201)
@app.post("/retain", status_code=201)
def brain_retain(
    req: RetainRequest,
    x_run_id: str | None = Header(default=None, alias="X-Run-Id"),
    principal: Principal = Depends(authenticate),
    visible: VisibleSet = Depends(write_visibility),
) -> dict[str, Any]:
    """Идемпотентно записать факт/решение в граф И индекс (origin='agent').

    Идемпотентность по external_id. Узел ключа вне видимости вызывающего не
    перезаписывается — 403, как при отсутствии права; scopes существующего узла
    сохраняются (MEM-ADR-019).
    """
    settings = get_settings()
    ns = _scope_namespace(req.scope)
    _authorize(principal, [ns], write=True)
    prov = dict(req.provenance or {})
    # Сквозной X-Run-Id: если тело не задало run_id — взять из заголовка (и как fallback trace).
    header_run = _hdr_str(x_run_id)
    if header_run and not prov.get("run_id"):
        prov["run_id"] = header_run
    trace_id = prov.get("trace_id") or prov.get("issue_id") or prov.get("run_id")
    natural_key = req.external_id or _derive_fact_key(req.content, trace_id)
    title = req.title or _first_line(req.content)
    properties: dict[str, Any] = {"content": req.content}
    if prov:
        # Вложенная карта не конфликтует с существующими плоскими полями и
        # сохраняет Git provenance до появления специализированной модели.
        properties["provenance"] = prov
    for key in ("actor", "actor_id", "issue_id", "trace_id", "kind", "source"):
        if prov.get(key) is not None:
            properties[key] = prov[key]
    # Маркировка ПДн (152-ФЗ): авто-детекция по content + явная метка запроса.
    # Учёт/инвентаризация; маскирование при чтении работает и без маркировки.
    pii_categories: list[str] = []
    if settings.pii_protection:
        pii_categories = sorted(set(detect_pii(req.content)) | set(req.pii_categories or []))
        if req.pii and not pii_categories:
            pii_categories = ["unspecified"]  # помечено явно, категория не названа
        if pii_categories:
            properties["pii"] = True
            properties["pii_categories"] = pii_categories
    try:
        result = write_and_index_fact(
            settings,
            natural_key,
            req.type,
            title,
            text=req.content,
            properties=properties,
            links=req.links,
            run_id=prov.get("run_id"),
            confidence=req.confidence,
            source=prov.get("source"),
            trace_id=trace_id,
            namespace=ns,
            allowed_scopes=_visible(visible).scopes,
        )
    except ForeignObjectError as exc:
        raise _foreign_write(exc) from exc
    response = {"retained": True, "type": req.type, "title": title, **result}
    if pii_categories:
        response["pii_categories"] = pii_categories  # аддитивно: видно, что пометили
    return response


@app.post("/api/brain/audit", status_code=201)
@app.post("/audit", status_code=201)
def brain_audit(
    req: AuditRequest,
    x_run_id: str | None = Header(default=None, alias="X-Run-Id"),
    principal: Principal = Depends(authenticate),
) -> dict[str, Any]:
    """Записать событие аудита как узел/ребро трейса задачи (trace_id = issueId/runId).

    ``payload.scopes`` — список scopes объекта события: по нему трейс скрывает событие
    от вызывающего без видимости (MEM-ADR-019). Не список или некорректный scope — 400;
    сохраняется канонизированный список.
    """
    settings = get_settings()
    ns = _scope_namespace(req.scope)
    _authorize(principal, [ns], write=True)
    payload = req.payload
    if payload is not None and payload.get("scopes") is not None:
        raw = payload["scopes"]
        if not isinstance(raw, list):
            raise HTTPException(
                status_code=400, detail="payload.scopes должен быть списком 'type:id'"
            )
        try:
            payload = {**payload, "scopes": resolve_scopes(raw, limit=MAX_ITEM_SCOPES)}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"payload.scopes: {exc}") from exc
    conn = dbmod.connect(settings)
    try:
        event = GraphStore(conn, settings.graph_name, settings.default_namespace).record_audit(
            req.trace_id,
            req.actor,
            req.action,
            payload=payload,
            ts=req.ts,
            run_id=req.run_id or _hdr_str(x_run_id),  # сквозной X-Run-Id, если тело не задало
            actor_id=req.actor_id,
            actor_kind=req.actor_kind,
            namespace=ns,
        )
    finally:
        conn.close()
    return {"recorded": True, **event}


@app.get("/api/brain/trace/{trace_id}")
def brain_trace(
    trace_id: str,
    namespace: str = Query(default="", description="База знаний (пусто — дефолтная)"),
    principal: Principal = Depends(authenticate),
    visible: VisibleSet = Depends(visibility),
) -> dict[str, Any]:
    """Подграф трейса задачи: события аудита (по времени) + привязанные факты.

    Факты и события со scope вне видимости вызывающего отбрасываются в запросе, до
    лимита; scope события — его ``props.scopes`` и ``payload.scopes`` (scopes
    удалённого или выданного объекта), MEM-ADR-019.
    """
    ns = _query_namespace(namespace)
    _authorize(principal, [ns], write=False)
    check_visible(_visible(visible), [ns], get_settings().default_namespace)
    settings = get_settings()
    conn = dbmod.connect(settings)
    try:
        return GraphStore(conn, settings.graph_name, settings.default_namespace).trace_subgraph(
            trace_id, namespace=ns, allowed_scopes=_visible(visible).scopes
        )
    finally:
        conn.close()


# --- документы: батчевый ингест готовых чанков (мигрируемые Базы знаний, ADR-017) ---


class DocumentChunkIn(BaseModel):
    text: str
    heading: str = ""
    order: int | None = None


class DocumentIngestRequest(BaseModel):
    natural_key: str  # например, doc:<uuid> — идемпотентный re-ingest
    title: str
    type: str = "document"
    # База знаний: ``namespace`` (как у клиента platform-memory-client) либо
    # ``scope.namespace`` (как у остальных маршрутов записи); пусто -> default сервиса.
    namespace: str = ""
    scope: dict | None = None
    source_path: str = ""  # цель цитат (например, s3://bucket/key)
    properties: dict | None = None
    meta: dict | None = None  # копируется на каждый чанк (фильтруемые теги)
    links: list[str] | None = None
    chunks: list[DocumentChunkIn] = []
    replace: bool = True  # первый вызов True; продолжение большого документа — False
    # Явная маркировка ПДн (152-ФЗ) — как у retain: дополняет авто-детекцию по чанкам.
    pii: bool | None = None
    pii_categories: list[str] | None = None

    @field_validator("namespace")
    @classmethod
    def _validate_ns(cls, value: str) -> str:
        return validate_namespace(value) if value else value

    @field_validator("chunks")
    @classmethod
    def _validate_chunks(cls, value: list[DocumentChunkIn]) -> list[DocumentChunkIn]:
        if len(value) > MAX_CHUNKS_PER_REQUEST:
            raise ValueError(
                f"Слишком много чанков: {len(value)} > {MAX_CHUNKS_PER_REQUEST}; "
                f"продолжайте повторными вызовами с replace=false"
            )
        return value


def _document_namespace(req: DocumentIngestRequest) -> str:
    """Namespace документа: top-level ``namespace`` и ``scope.namespace`` не должны спорить."""
    scoped = _scope_namespace(req.scope)
    if req.namespace and scoped and req.namespace != scoped:
        raise HTTPException(
            status_code=400, detail="namespace и scope.namespace задают разные базы знаний"
        )
    return req.namespace or scoped


@app.post("/api/brain/documents", status_code=201)
def brain_document_ingest(
    req: DocumentIngestRequest,
    x_run_id: str | None = Header(default=None, alias="X-Run-Id"),
    principal: Principal = Depends(authenticate),
    visible: VisibleSet = Depends(write_visibility),
) -> dict[str, Any]:
    """Записать документ: узел графа + батч чанков с эмбеддингами в один namespace.

    Хост парсит документ сам и передаёт готовые текстовые чанки; эмбеддинги считает
    движок. Идемпотентно по natural_key (``replace=true`` заменяет чанки узла).
    ПДн маркируются на узле по авто-детекции в чанках плюс явной метке (ADR-002).
    Узел или чанк ключа вне видимости вызывающего — 403 до любой записи, как при
    отсутствии права; scopes существующего узла сливаются, чанки их несут (MEM-ADR-019).
    """
    settings = get_settings()
    ns = _document_namespace(req)
    _authorize(principal, [ns], write=True)
    properties: dict[str, Any] = dict(req.properties or {})
    pii_categories: list[str] = []
    if settings.pii_protection:
        found: set[str] = set(req.pii_categories or [])
        for chunk in req.chunks:
            found |= set(detect_pii(chunk.text))
        pii_categories = sorted(found)
        if req.pii and not pii_categories:
            pii_categories = ["unspecified"]
        if pii_categories:
            properties["pii"] = True
            properties["pii_categories"] = pii_categories
    try:
        result = retain_document(
            settings,
            namespace=ns,
            natural_key=req.natural_key,
            title=req.title,
            node_type=req.type,
            source_path=req.source_path,
            properties=properties,
            meta=req.meta,
            links=req.links,
            chunks=[
                {"text": c.text, "heading": c.heading, "order": c.order}
                if c.order is not None
                else {"text": c.text, "heading": c.heading}
                for c in req.chunks
            ],
            replace=req.replace,
            run_id=_hdr_str(x_run_id),
            allowed_scopes=_visible(visible).scopes,
        )
    except ForeignObjectError as exc:
        raise _foreign_write(exc) from exc
    except (UnknownKindError, AttributesError, UnknownRelationError):
        raise  # строгий режим видов namespace -> 422 (MEM-ADR-020)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if pii_categories:
        result["pii_categories"] = pii_categories  # аддитивно: видно, что пометили
    return result


@app.delete("/api/brain/documents/{natural_key:path}")
def brain_document_delete(
    natural_key: str,
    namespace: str = Query(default="", description="База знаний (пусто — дефолтная)"),
    actor: str = Query(default="api"),
    trace_id: str | None = Query(default=None),
    x_run_id: str | None = Header(default=None, alias="X-Run-Id"),
    principal: Principal = Depends(authenticate),
    visible: VisibleSet = Depends(visibility),
) -> dict[str, Any]:
    """Удалить документ: узел с рёбрами и все его чанки в пределах namespace — с аудитом.

    Идемпотентно: повторный вызов по отсутствующему ключу отвечает ``deleted: false``
    (в отличие от ``DELETE /api/brain/nodes/{key}``, который даёт 404). Документ со
    scope вне видимости вызывающего не удаляется и отвечает так же, как отсутствующий
    (MEM-ADR-019).
    """
    natural_key = normalize_natural_key(natural_key)
    ns = _query_namespace(namespace)
    _authorize(principal, [ns], write=True)
    check_visible(_visible(visible), [ns], get_settings().default_namespace)
    try:
        return delete_document(
            get_settings(),
            natural_key,
            ns,
            actor=_hdr_str(actor) or "api",  # прямой вызов хендлера: Query-дефолт -> "api"
            trace_id=_hdr_str(trace_id),
            run_id=_hdr_str(x_run_id),
            allowed_scopes=_visible(visible).scopes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/brain/health", dependencies=[Depends(require_auth)])
@app.get("/health", dependencies=[Depends(require_auth)])
def brain_health() -> dict[str, Any]:
    """Живость + проверка токена (auth-gated): для onValidateConfig плагина."""
    return healthz()


# Memory Console (/console): server-rendered админ-интерфейс поверх этого же движка.
# Выключена по умолчанию (CB_CONSOLE_ENABLED=false) — гейт проверяется на каждом
# запросе, публичный Memory API работает без Console и новых переменных окружения.
# Импорт внизу файла: console ссылается на хендлеры/сторы этого модуля.
from platform_memory.server.console import router as _console_router  # noqa: E402

app.include_router(_console_router)

# Публичная демо-витрина (/demo): read-only, одна KB, gated CB_DEMO_PUBLIC_ENABLED=false.
# Тот же паттерн, что Console — in-process поверх этого движка, ключ в браузер не уходит.
from platform_memory.server.demo import router as _demo_router  # noqa: E402

app.include_router(_demo_router)

# Context Memory Engine (/api/memory/*, ADR-016): observations + build_context.
# Аддитивно к /api/brain/*; auth и PII-барьер — те же (вызываются из этого модуля).
from platform_memory.server.memory_api import router as _memory_router  # noqa: E402

app.include_router(_memory_router)


def main() -> None:
    """Запустить сервис (uvicorn). Точка входа console-script `platform-memory-serve`."""
    import uvicorn

    settings = get_settings()
    uvicorn.run(app, host=settings.server_host, port=settings.server_port)


if __name__ == "__main__":
    main()

# Адаптировано из graphify/serve.py (https://github.com/safishamsi/graphify), MIT License.
# Copyright (c) 2026 Safi Shamsi. See THIRD_PARTY.md for the full license text.
#
# Перенесён MCP-каркас: low-level mcp.server.Server, транспорты stdio и Streamable HTTP,
# api-key middleware (raw-ASGI, не ломающий SSE), per-session state. Адаптировано: источник
# графа — наш GraphStore (а не graph.json), tool'ы переписаны на GraphStore/retrieve_subgraph,
# поля чистятся нашей границей доверия (llm_safety). Tool-набор сведён к контракту задачи:
# query_graph / get_node / get_neighbors / shortest_path.
"""MCP-сервер: отдать граф Company Brain агентам по Model Context Protocol.

Тот же набор tool'ов на обоих транспортах (stdio для локального агента, Streamable HTTP
для общего стенда). HTTP закрывается api-key (CB_SERVER_API_KEY): заголовок
``Authorization: Bearer <key>`` или ``X-API-Key: <key>``.

Видимость (MEM-ADR-019): MCP — служебная поверхность. Вызывающий — держатель
статического ключа сервиса (или локальный процесс с доступом к БД по stdio), это
service-режим, как у статического ключа HTTP API: без ограничения видимости.
Агент, действующий за конечного principal, передаёт в аргументах любого tool'а
``allowed_scopes`` — сужение (MEM-ADR-021): невидимые узлы, чанки и наблюдения не
выдаются и не перезаписываются, scopes записи — только из этого списка.
"""

from __future__ import annotations

import sys

import networkx as nx

from platform_memory.core import db as dbmod
from platform_memory.core.config import Settings, get_settings
from platform_memory.core.llm_safety import neutralize_prompt_injection, sanitize_label
from platform_memory.core.scopes import MAX_ALLOWED_SCOPES, is_visible, node_scopes, resolve_scopes
from platform_memory.graph import GraphStore, to_networkx
from platform_memory.retrieval import retrieve_subgraph

# ── общие хелперы ────────────────────────────────────────────────────────────────


def _clean(value: object) -> str:
    """Санитизировать поле перед склейкой в ответ MCP (control/ANSI/инъекции)."""
    return neutralize_prompt_injection(sanitize_label(str(value)))


def _allowed(args: dict) -> list[str] | None:
    """Сужение видимости вызова (``allowed_scopes``, MEM-ADR-021); нет поля — None.

    Пустой список — только элементы уровня namespace, а не «без фильтра».
    """
    raw = args.get("allowed_scopes")
    if raw is None:
        return None
    if not isinstance(raw, list) or not all(isinstance(v, str) for v in raw):
        raise ValueError("allowed_scopes должен быть списком строк 'type:id'")
    return resolve_scopes(raw, limit=MAX_ALLOWED_SCOPES)


def _resolve_node(store: GraphStore, query: str, allowed: list[str] | None = None) -> str | None:
    """Разрешить запрос в natural_key: точное совпадение ключа, затем поиск по заголовку.

    Только в default namespace и только среди видимых ``allowed`` узлов: невидимый
    узел неотличим от отсутствующего.
    """
    q = query.strip()
    if store.get_node(q) is not None and store.key_visible(q, allowed_scopes=allowed):
        return q
    ql = q.casefold()
    best: str | None = None
    for props in store.all_nodes(namespaces=[store.default_namespace]):
        if not is_visible(node_scopes(props), allowed):
            continue
        nk = str(props.get("natural_key", ""))
        title = str(props.get("title", ""))
        # Ключ может нести и невидимый узел другого вида — get_node вернул бы его.
        if nk.casefold() == ql or title.casefold() == ql:
            if store.key_visible(nk, allowed_scopes=allowed):
                return nk
        elif best is None and (ql in nk.casefold() or ql in title.casefold()):
            if store.key_visible(nk, allowed_scopes=allowed):
                best = nk
    return best


# ── реализации tool'ов (модульные — тестируются напрямую) ───────────────────────


def tool_query_graph(settings: Settings, args: dict) -> str:
    """Поиск по графу: vector-first seed'ы → BFS/DFS-расширение → текст подграфа."""
    question = args["question"]
    mode = args.get("mode", "bfs")
    depth = min(int(args.get("depth", 2)), 6)
    budget = int(args.get("token_budget", 2000))
    result = retrieve_subgraph(
        settings,
        question,
        mode=mode,
        depth=depth,
        token_budget=budget,
        allowed_scopes=_allowed(args),
    )
    return result["text"] or "Подходящих узлов не найдено."


def tool_get_node(settings: Settings, args: dict) -> str:
    """Детали узла по natural_key или заголовку: тип, источник, степень."""
    allowed = _allowed(args)
    conn = dbmod.connect(settings, autocommit=True)
    try:
        store = GraphStore(conn, settings.graph_name, settings.default_namespace)
        nk = _resolve_node(store, args["label"], allowed)
        if nk is None:
            return f"Узел не найден: {args['label']!r}"
        node = store.get_node(nk) or {}
        neighbors = [
            n for n in store.expand_neighbors([nk], hops=1) if is_visible(node_scopes(n), allowed)
        ]
        inner = node.get("props") if isinstance(node.get("props"), dict) else {}
        lines = [
            f"Узел: {_clean(node.get('title', nk))}",
            f"  ключ: {_clean(nk)}",
            f"  тип: {_clean(node.get('type', 'entity'))}",
            f"  источник: {_clean(node.get('source_path', ''))}",
            f"  origin: {_clean(node.get('origin', 'vault'))}",
            f"  соседей: {len(neighbors)}",
        ]
        if node.get("community_name"):
            lines.append(f"  тема: {_clean(node['community_name'])}")
        elif isinstance(node.get("community"), int):
            lines.append(f"  сообщество: #{node['community']}")
        for key in ("status", "priority", "due", "date"):
            if isinstance(inner, dict) and inner.get(key):
                lines.append(f"  {key}: {_clean(inner[key])}")
        return "\n".join(lines)
    finally:
        conn.close()


def tool_get_neighbors(settings: Settings, args: dict) -> str:
    """Прямые соседи узла (с типом связи). Опционально фильтр по типу связи."""
    rel_filter = str(args.get("relation_filter", "")).casefold()
    allowed = _allowed(args)
    conn = dbmod.connect(settings, autocommit=True)
    try:
        store = GraphStore(conn, settings.graph_name, settings.default_namespace)
        nk = _resolve_node(store, args["label"], allowed)
        if nk is None:
            return f"Узел не найден: {args['label']!r}"
        G = to_networkx(store, namespaces=[settings.default_namespace], allowed_scopes=allowed)
        if nk not in G:
            return f"Узел не найден в графе: {nk}"
        lines = [f"Соседи «{_clean(G.nodes[nk].get('label', nk))}»:"]
        for nb in G.successors(nk):
            rel = str(G.get_edge_data(nk, nb).get("relation", ""))
            if rel_filter and rel_filter not in rel.casefold():
                continue
            lines.append(f"  --{_clean(rel)}--> {_clean(G.nodes[nb].get('label', nb))}")
        for nb in G.predecessors(nk):
            rel = str(G.get_edge_data(nb, nk).get("relation", ""))
            if rel_filter and rel_filter not in rel.casefold():
                continue
            lines.append(f"  <--{_clean(rel)}-- {_clean(G.nodes[nb].get('label', nb))}")
        return "\n".join(lines) if len(lines) > 1 else lines[0] + "\n  (нет связей)"
    finally:
        conn.close()


def tool_get_community(settings: Settings, args: dict) -> str:
    """Узлы сообщества по его id (community-summaries из Phase 3)."""
    cid = int(args["community_id"])
    allowed = _allowed(args)
    conn = dbmod.connect(settings, autocommit=True)
    try:
        members = GraphStore(
            conn, settings.graph_name, settings.default_namespace
        ).community_members(cid)
    finally:
        conn.close()
    members = [m for m in members if is_visible(node_scopes(m), allowed)]
    if not members:
        return f"Сообщество #{cid} пусто или не найдено."
    name = next((m.get("community_name") for m in members if m.get("community_name")), None)
    head = (
        f"Сообщество #{cid}" + (f" — {_clean(name)}" if name else "") + f" ({len(members)} узлов):"
    )
    lines = [head]
    for m in members:
        label = _clean(m.get("title", m.get("natural_key", "")))
        lines.append(f"  [{_clean(m.get('type', 'entity'))}] {label}")
    return "\n".join(lines)


def tool_shortest_path(settings: Settings, args: dict) -> str:
    """Кратчайший путь между двумя сущностями (по неориентированной проекции).

    Путь идёт только через видимые узлы: невидимый промежуточный узел — как
    отсутствующий (MEM-ADR-019).
    """
    allowed = _allowed(args)
    conn = dbmod.connect(settings, autocommit=True)
    try:
        store = GraphStore(conn, settings.graph_name, settings.default_namespace)
        src = _resolve_node(store, args["source"], allowed)
        tgt = _resolve_node(store, args["target"], allowed)
        if src is None:
            return f"Источник не найден: {args['source']!r}"
        if tgt is None:
            return f"Цель не найдена: {args['target']!r}"
        if src == tgt:
            return f"Источник и цель — один узел: {_clean(src)}"
        G = to_networkx(store, namespaces=[settings.default_namespace], allowed_scopes=allowed)
        und = G.to_undirected(as_view=True)
        try:
            path = nx.shortest_path(und, src, tgt)
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return f"Пути между «{_clean(src)}» и «{_clean(tgt)}» нет."
        labels = [_clean(G.nodes[n].get("label", n)) for n in path]
        return f"Путь ({len(path) - 1} шаг(ов)): " + " → ".join(labels)
    finally:
        conn.close()


def tool_build_context(settings: Settings, args: dict) -> str:
    """Собрать bounded ContextPack (Context Compiler, ADR-016) — текстовый рендер."""
    from platform_memory.context import build_context

    pack = build_context(
        settings,
        {
            "query": str(args.get("query", "")),
            "scopes": list(args.get("scopes") or []),
            "anchors": list(args.get("anchors") or []),
            "max_tokens": int(args.get("max_tokens", 0)),
            "namespaces": [args["namespace"]] if args.get("namespace") else [],
            "allowed_scopes": _allowed(args),
        },
    )
    text = pack.to_text().strip()
    footer = (
        f"\n(≈{pack.token_estimate} токенов; "
        f"отброшено по бюджету: {len(pack.budget.get('dropped', []))}; trace {pack.trace_id})"
    )
    return (text or "Релевантного контекста не найдено.") + footer


def tool_remember_observation(settings: Settings, args: dict) -> str:
    """Сохранить наблюдение (идемпотентно по source identity, ADR-016).

    Видимость пишущего — ``allowed_scopes`` (нет — service-режим статического ключа),
    та же проверка, что у ``POST /api/memory/observations`` (MEM-ADR-019).
    """
    from platform_memory.observations.ingest import retain_observation

    payload = {
        "kind": str(args.get("kind", "")),
        "content": str(args.get("content", "")),
        "occurred_at": str(args.get("occurred_at", "")),
        "scopes": list(args.get("scopes") or []),
        "data": dict(args.get("data") or {}),
        "provenance": dict(args.get("provenance") or {}),
    }
    if args.get("source"):
        payload["source"] = dict(args["source"])
    result = retain_observation(
        settings,
        payload,
        namespace=str(args.get("namespace", "") or ""),
        allowed_scopes=_allowed(args),
    )
    dup = " (дубликат — уже было)" if result["duplicate"] else ""
    return f"Сохранено: {result['observation_id']} status={result['status']}{dup}"


_TOOL_SPECS = [
    (
        "query_graph",
        "Поиск по графу знаний (BFS/DFS от vector-first seed'ов). Возвращает текст подграфа.",
        {
            "type": "object",
            "properties": {
                "question": {"type": "string", "description": "Вопрос или ключевые слова"},
                "mode": {"type": "string", "enum": ["bfs", "dfs"], "default": "bfs"},
                "depth": {"type": "integer", "default": 2, "description": "Глубина обхода (1-6)"},
                "token_budget": {"type": "integer", "default": 2000},
            },
            "required": ["question"],
        },
        tool_query_graph,
    ),
    (
        "get_node",
        "Детали узла по natural_key или заголовку.",
        {
            "type": "object",
            "properties": {"label": {"type": "string", "description": "natural_key или заголовок"}},
            "required": ["label"],
        },
        tool_get_node,
    ),
    (
        "get_neighbors",
        "Прямые соседи узла с типами связей.",
        {
            "type": "object",
            "properties": {
                "label": {"type": "string"},
                "relation_filter": {"type": "string", "description": "Фильтр по типу связи"},
            },
            "required": ["label"],
        },
        tool_get_neighbors,
    ),
    (
        "get_community",
        "Узлы тематического сообщества по его id (community-summaries).",
        {
            "type": "object",
            "properties": {"community_id": {"type": "integer", "description": "ID сообщества"}},
            "required": ["community_id"],
        },
        tool_get_community,
    ),
    (
        "shortest_path",
        "Кратчайший путь между двумя сущностями графа.",
        {
            "type": "object",
            "properties": {
                "source": {"type": "string"},
                "target": {"type": "string"},
            },
            "required": ["source", "target"],
        },
        tool_shortest_path,
    ),
    (
        "build_context",
        "Собрать bounded контекст по запросу: факты, документы, наблюдения, сущности "
        "с provenance и токен-бюджетом (без LLM-синтеза).",
        {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Запрос/задача"},
                "scopes": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Scope-фильтры 'type:id' (например, 'project:x')",
                },
                "anchors": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Сущности-якоря graph expansion (natural_key)",
                },
                "max_tokens": {"type": "integer", "default": 0},
                "namespace": {"type": "string", "description": "KB (пусто — дефолтная)"},
            },
            "required": ["query"],
        },
        tool_build_context,
    ),
    (
        "remember_observation",
        "Сохранить наблюдение в память (идемпотентно): kind, content, source identity, scopes.",
        {
            "type": "object",
            "properties": {
                "kind": {"type": "string", "description": "Тип события: work.completed, …"},
                "content": {"type": "string", "description": "Человекочитаемое содержимое"},
                "occurred_at": {"type": "string", "description": "Когда произошло (ISO-8601)"},
                "scopes": {"type": "array", "items": {"type": "string"}},
                "source": {
                    "type": "object",
                    "description": "{system, stream, external_id} — идемпотентность доставки",
                },
                "data": {"type": "object"},
                "provenance": {"type": "object", "description": "{uri: …}"},
                "namespace": {"type": "string"},
            },
            "required": ["content"],
        },
        tool_remember_observation,
    ),
]


# Сужение видимости (MEM-ADR-021) — общий необязательный аргумент всех tool'ов.
_ALLOWED_SCOPES_SCHEMA = {
    "type": "array",
    "items": {"type": "string"},
    "description": (
        "Видимость вызова: scopes 'workspace:<id>'/'principal:<id>', за кого действует "
        "агент. Узлы, чанки и наблюдения с чужим scope не выдаются и не перезаписываются; "
        "пустой список — только общая память базы. Нет поля — без ограничения."
    ),
}
for _name, _desc, _schema, _fn in _TOOL_SPECS:
    _schema["properties"]["allowed_scopes"] = _ALLOWED_SCOPES_SCHEMA


def build_server(settings: Settings | None = None):
    """Собрать low-level MCP Server со всеми tool'ами над графом памяти."""
    from mcp import types
    from mcp.server import Server

    settings = settings or get_settings()
    handlers = {name: fn for name, _desc, _schema, fn in _TOOL_SPECS}

    server = Server("company-brain")

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(name=name, description=desc, inputSchema=schema)
            for name, desc, schema, _fn in _TOOL_SPECS
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
        handler = handlers.get(name)
        if handler is None:
            return [types.TextContent(type="text", text=f"Неизвестный tool: {name}")]
        try:
            text = handler(settings, arguments or {})
        except Exception as exc:  # noqa: BLE001 — не роняем сессию на ошибке одного вызова
            text = f"Ошибка выполнения {name}: {exc}"
        return [types.TextContent(type="text", text=text)]

    return server


# ── транспорты (вендорено) ──────────────────────────────────────────────────────


class _MCPASGIApp:
    """Raw-ASGI обёртка над менеджером сессий Streamable HTTP."""

    def __init__(self, manager) -> None:
        self._manager = manager

    async def __call__(self, scope, receive, send) -> None:
        await self._manager.handle_request(scope, receive, send)


class _ApiKeyMiddleware:
    """Pure-ASGI api-key гейт. Raw-ASGI (не BaseHTTPMiddleware) — чтобы не ломать SSE-поток."""

    def __init__(self, app, api_key: str) -> None:
        self.app = app
        self._expected = api_key.encode("utf-8")

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        import hmac

        headers = dict(scope.get("headers") or [])
        provided = headers.get(b"x-api-key")
        if provided is None:
            scheme, _, token = headers.get(b"authorization", b"").partition(b" ")
            if scheme.lower() == b"bearer" and token:
                provided = token.strip()
        if provided is None or not hmac.compare_digest(provided, self._expected):
            body = b'{"error": "unauthorized"}'
            await send(
                {
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"content-length", str(len(body)).encode("ascii")),
                    ],
                }
            )
            await send({"type": "http.response.body", "body": body})
            return
        await self.app(scope, receive, send)


def _is_loopback(host: str) -> bool:
    """Адрес bind — только loopback (127.0.0.0/8, ::1, localhost)."""
    import ipaddress

    if host.strip().lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host.strip().strip("[]")).is_loopback
    except ValueError:
        return False  # имя хоста, пустой bind — не loopback


class InsecureBindError(RuntimeError):
    """Streamable HTTP на не-loopback адресе без api-key (MEM-ADR-019)."""


def build_http_app(
    settings: Settings | None = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8088,
    api_key: str | None = None,
    path: str = "/mcp",
    json_response: bool = False,
    stateless: bool = False,
    session_timeout: float | None = 3600.0,
):
    """Собрать Starlette-ASGI приложение Streamable HTTP транспорта (тестируется in-process).

    Без api-key допустим только loopback-адрес: MCP — служебная поверхность
    держателя ``CB_SERVER_API_KEY`` (MEM-ADR-019), и без ключа на сетевом адресе он
    отдавал бы граф без ограничения видимости любому. Иначе — ``InsecureBindError``.
    """
    import contextlib

    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from mcp.server.transport_security import TransportSecuritySettings
    from starlette.applications import Starlette
    from starlette.middleware import Middleware
    from starlette.routing import Route

    settings = settings or get_settings()
    api_key = (api_key if api_key is not None else settings.server_api_key or "").strip() or None
    if not api_key and not _is_loopback(host):
        raise InsecureBindError(
            f"MCP по HTTP на {host or '<все интерфейсы>'} без api-key не запускается: "
            "задайте CB_SERVER_API_KEY (или --api-key) либо bind на 127.0.0.1"
        )
    server = build_server(settings)

    if host in ("0.0.0.0", "::", ""):
        security = TransportSecuritySettings(enable_dns_rebinding_protection=False)
    else:
        allowed = {host, "localhost", "127.0.0.1"}
        allowed |= {f"{h}:{port}" for h in list(allowed)}
        security = TransportSecuritySettings(allowed_hosts=sorted(allowed))

    idle_timeout = (
        None if (stateless or not session_timeout or session_timeout <= 0) else session_timeout
    )

    manager = StreamableHTTPSessionManager(
        app=server,
        json_response=json_response,
        stateless=stateless,
        security_settings=security,
        session_idle_timeout=idle_timeout,
    )

    @contextlib.asynccontextmanager
    async def lifespan(_app):
        async with manager.run():
            yield

    middleware = []
    if api_key:
        middleware.append(Middleware(_ApiKeyMiddleware, api_key=api_key))

    return Starlette(
        routes=[Route(path, endpoint=_MCPASGIApp(manager))],
        middleware=middleware,
        lifespan=lifespan,
    )


def serve_stdio(settings: Settings | None = None) -> None:
    """Запустить MCP-сервер по stdio (локальный транспорт на разработчика)."""
    import asyncio

    from mcp.server.stdio import stdio_server

    server = build_server(settings)

    async def _main() -> None:
        async with stdio_server() as streams:
            await server.run(streams[0], streams[1], server.create_initialization_options())

    asyncio.run(_main())


def serve_http(
    settings: Settings | None = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8088,
    api_key: str | None = None,
    path: str = "/mcp",
) -> None:
    """Запустить MCP-сервер по Streamable HTTP. api-key из CB_SERVER_API_KEY, если не задан."""
    import uvicorn

    settings = settings or get_settings()
    try:
        app = build_http_app(settings, host=host, port=port, api_key=api_key, path=path)
    except InsecureBindError as exc:
        print(f"ОШИБКА: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    resolved_key = (api_key if api_key is not None else settings.server_api_key or "").strip()
    auth_note = "api-key обязателен" if resolved_key else "без авторизации, только loopback"
    print(
        f"Company Brain MCP (streamable-http) на http://{host}:{port}{path} — {auth_note}",
        file=sys.stderr,
    )
    uvicorn.run(app, host=host, port=port)


def main() -> None:
    """CLI-вход `cb-mcp`: транспорт по argv (stdio по умолчанию | http)."""
    import argparse

    parser = argparse.ArgumentParser(description="MCP-сервер графа Company Brain.")
    parser.add_argument("--transport", choices=["stdio", "http"], default="stdio")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8088)
    parser.add_argument("--api-key", default=None, help="Переопределить CB_SERVER_API_KEY")
    parser.add_argument("--path", default="/mcp")
    args = parser.parse_args()
    if args.transport == "http":
        serve_http(host=args.host, port=args.port, api_key=args.api_key, path=args.path)
    else:
        serve_stdio()


if __name__ == "__main__":
    main()

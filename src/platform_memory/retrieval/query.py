"""query(text) -> ответ + источники: вектор top-k + расширение по графу + LLM."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from platform_memory.core import db as dbmod
from platform_memory.core.config import Settings
from platform_memory.core.llm_safety import neutralize_prompt_injection
from platform_memory.core.models import Chunk
from platform_memory.core.scopes import (
    check_write_scopes,
    is_visible,
    meta_scopes,
    meta_with_scopes,
    node_scopes,
)
from platform_memory.core.namespaces import resolve_namespace, resolve_namespaces
from platform_memory.domain.registry import attach_kind_guard
from platform_memory.graph import GraphStore, to_networkx
from platform_memory.index import VectorIndex, build_embedder
from platform_memory.index.embeddings import Embedder
from platform_memory.index.store import SearchHit
from platform_memory.retrieval.expansion import expand_subgraph, pick_seeds
from platform_memory.retrieval.llm import LLM, build_llm
from platform_memory.retrieval.rerank import build_reranker

_SYSTEM = (
    "Ты — ассистент по базе знаний компании. Отвечай на русском, опираясь ИСКЛЮЧИТЕЛЬНО на "
    "приведённый контекст из графа знаний. Не придумывай факты. Если ответа в контексте нет — "
    "честно скажи «В графе знаний нет данных по этому вопросу». В конце ответа приведи список "
    "использованных источников (путь к файлу + id), на которые опирался."
)


@dataclass(slots=True)
class Source:
    """Источник ответа: путь к файлу, ключ узла и заголовок."""

    source_path: str
    node_key: str
    title: str


@dataclass(slots=True)
class Retrieval:
    """Результат извлечения без генерации: найденные фрагменты, соседи, источники, контекст.

    ``stats`` — счётчики этапов конвейера (кандидаты вектор+FTS, соседи графа, размер пула
    реранка, было ли переоценивание): нужны, чтобы наглядно показать процесс поиска на
    витрине. Аддитивно, дефолт — пустой словарь; существующих потребителей не затрагивает.
    """

    hits: list[SearchHit] = field(default_factory=list)
    neighbors: list[dict] = field(default_factory=list)
    sources: list[Source] = field(default_factory=list)
    context: str = ""
    stats: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Answer:
    """Ответ LLM с текстом, источниками, контекстом и счётчиками найденного."""

    text: str
    sources: list[Source] = field(default_factory=list)
    context: str = ""
    hits: int = 0
    neighbors: int = 0


def _fmt_neighbor(props: dict) -> str:
    nk = props.get("natural_key", "?")
    typ = props.get("type", "entity")
    title = props.get("title", "")
    sp = props.get("source_path", "")
    extra = ""
    inner = props.get("props") or {}
    if isinstance(inner, dict):
        for key in ("status", "priority", "due", "date"):
            if inner.get(key):
                extra += f" {key}={inner[key]}"
    # Тема сообщества (Phase 3), если размечена — помогает модели сгруппировать соседей.
    community = props.get("community_name")
    if community:
        extra += f" тема={community}"
    return f"- [{nk}] ({typ}) {title}{extra} — {sp}".rstrip()


def _build_context(hits: list[SearchHit], neighbors: list[dict]) -> str:
    parts: list[str] = ["# Найденные фрагменты\n"]
    for h in hits:
        head = " — ".join(p for p in (h.title, h.heading) if p)
        # Тело чанка — недоверенный контент (письма/транскрипты/агентский write-back):
        # глушим prompt-injection перед склейкой в контекст модели (T-073).
        safe_text = neutralize_prompt_injection(h.text)
        parts.append(f"## [{h.node_key}] {head}\nИсточник: {h.source_path}\n\n{safe_text}\n")
    if neighbors:
        parts.append("\n# Связанные сущности (граф, соседи)\n")
        parts.extend(_fmt_neighbor(n) for n in neighbors)
    return "\n".join(parts)


def _apply_rerank(settings: Settings, question: str, hits: list[SearchHit]) -> list[SearchHit]:
    """Переоценить кандидатов реранкером и пересортировать по rerank_score.

    При недоступности реранкера (echo/без ключа), ошибке LLM или неполном ответе —
    вернуть исходный порядок RRF без изменений (ADR-005: реранк не роняет поиск).
    """
    if not hits:
        return hits
    reranker = build_reranker(settings)
    if reranker is None:
        return hits
    docs = [f"{h.title} — {h.heading}\n{h.text}".strip() for h in hits]
    scores = reranker.score(question, docs)
    if not scores or len(scores) != len(hits):
        return hits  # fallback: сохраняем порядок RRF
    for h, s in zip(hits, scores):
        h.rerank_score = s
    return sorted(
        hits, key=lambda h: h.rerank_score if h.rerank_score is not None else -1.0, reverse=True
    )


# При реранке берём меньше чисто-векторных кандидатов — освобождаем место граф-соседям.
_RERANK_VECTOR_K = 12


def _with_neighbor_candidates(
    hits: list[SearchHit], neighbors: list[dict[str, Any]], pool_cap: int, k: int
) -> list[SearchHit]:
    """Пул реранка = вектор-хиты ∪ граф-соседи с текстом (ADR-005): связанный по графу узел
    может оказаться релевантнее любого вектор-хита. Дедуп по node_key, общий кап пула."""
    pool = list(hits)
    seen = {h.node_key for h in hits}
    for nbr in neighbors:
        nk = str(nbr.get("natural_key", ""))
        if not nk or nk in seen:
            continue
        props = nbr.get("props") if isinstance(nbr.get("props"), dict) else {}
        text = (props.get("content") or "").strip() or str(nbr.get("title", "")).strip()
        if not text:
            continue
        seen.add(nk)
        pool.append(
            SearchHit(
                chunk_id=-1,
                node_key=nk,
                source_path=str(nbr.get("source_path", ""))
                or f"memory:{nbr.get('type', 'node')}/{nk}",
                title=str(nbr.get("title", "")),
                heading="",
                text=text,
                score=0.0,
                namespace=str(nbr.get("namespace", "")),
            )
        )
    return pool[: max(pool_cap, k)]


def retrieve(
    settings: Settings,
    question: str,
    *,
    k: int = 8,
    hops: int = 1,
    namespaces: Sequence[str] = (),
    meta_filter: dict[str, Any] | None = None,
    embedder: Embedder | None = None,
    rerank: bool | None = None,
    allowed_scopes: Sequence[str] | None = None,
) -> Retrieval:
    """Извлечь контекст БЕЗ генерации: вектор top-k + русский FTS + расширение по графу.

    Машинный слой для HTTP/программного использования: возвращает структурированные
    фрагменты, соседние узлы и дедуплицированные источники. ``namespaces`` — область
    видимости (пусто -> default), ``meta_filter`` — jsonb-containment по meta чанков.
    ``rerank`` — переоценка кандидатов реранкером (ADR-005): None → значение
    ``CB_RERANK_ENABLED``; True/False — явное переопределение (витрина включает точечно).
    """
    embedder = embedder or build_embedder(settings)
    nss = resolve_namespaces(namespaces, settings.default_namespace)
    do_rerank = settings.rerank_enabled if rerank is None else rerank
    # При реранке берём меньше вектор-кандидатов — общий пул дополним граф-соседями.
    vector_k = min(settings.rerank_pool, _RERANK_VECTOR_K) if do_rerank else k
    conn = dbmod.connect(settings, autocommit=True)
    try:
        graph = GraphStore(conn, settings.graph_name, settings.default_namespace)
        index = VectorIndex(
            conn, settings.chunks_table, settings.embedding_dim, settings.default_namespace
        )

        q_emb = embedder.embed_one(question)
        vhits = index.search(
            q_emb,
            question,
            k=vector_k,
            namespaces=nss,
            meta_filter=meta_filter,
            allowed_scopes=allowed_scopes,
        )
        vector_candidates = len(vhits)

        # Соседи по графу от вектор-хитов: и как кандидаты реранка, и для источников/контекста.
        seed_keys = list(dict.fromkeys(h.node_key for h in vhits))
        neighbors = (
            graph.expand_neighbors(seed_keys, hops=hops, namespaces=nss) if seed_keys else []
        )
        if allowed_scopes is not None:
            # Видимость по principal (MEM-ADR-019): соседи с чужим scope отсекаются.
            neighbors = [n for n in neighbors if is_visible(_node_scopes(n), allowed_scopes)]

        pool_size = vector_candidates
        if do_rerank:
            # Реранкаем вектор-хиты ВМЕСТЕ с граф-соседями (ADR-005), затем top-k.
            candidates = _with_neighbor_candidates(vhits, neighbors, settings.rerank_pool, k)
            pool_size = len(candidates)
            hits = _apply_rerank(settings, question, candidates)[:k]
        else:
            hits = vhits[:k]
        # Счётчики этапов для наглядной визуализации конвейера на витрине.
        stats = {
            "vector_candidates": vector_candidates,
            "neighbors": len(neighbors),
            "pool": pool_size,
            "reranked": do_rerank
            and any(getattr(h, "rerank_score", None) is not None for h in hits),
            "final": len(hits),
        }

        # Источники: из найденных чанков + соседних узлов (дедуп по node_key).
        sources: list[Source] = []
        seen: set[str] = set()
        for h in hits:
            if h.node_key not in seen:
                seen.add(h.node_key)
                sources.append(Source(h.source_path, h.node_key, h.title))
        for n in neighbors:
            nk = str(n.get("natural_key", ""))
            if nk and nk not in seen:
                seen.add(nk)
                sources.append(Source(str(n.get("source_path", "")), nk, str(n.get("title", ""))))

        return Retrieval(
            hits=hits,
            neighbors=neighbors,
            sources=sources,
            context=_build_context(hits, neighbors),
            stats=stats,
        )
    finally:
        conn.close()


def query(
    settings: Settings,
    question: str,
    *,
    k: int = 8,
    hops: int = 1,
    namespaces: Sequence[str] = (),
    meta_filter: dict[str, Any] | None = None,
    embedder: Embedder | None = None,
    llm: LLM | None = None,
    rerank: bool | None = None,
    system: str | None = None,
    max_tokens: int | None = None,
    allowed_scopes: Sequence[str] | None = None,
) -> Answer:
    """Ответить на вопрос: извлечение (retrieve) + генерация ответа LLM с цитатами.

    ``rerank`` пробрасывается в retrieve (витрина включает точечно). ``system`` —
    переопределение системного промпта (например, англоязычный ответ для демо).
    ``max_tokens`` — лимит длины генерации (короткий ответ витрины — быстрее).
    """
    embedder = embedder or build_embedder(settings)
    llm = llm or build_llm(settings)

    r = retrieve(
        settings,
        question,
        k=k,
        hops=hops,
        namespaces=namespaces,
        meta_filter=meta_filter,
        embedder=embedder,
        rerank=rerank,
        allowed_scopes=allowed_scopes,
    )
    if not r.hits:
        return Answer(
            text="В графе знаний нет данных по этому вопросу.",
            sources=[],
            context=r.context,
            hits=0,
            neighbors=len(r.neighbors),
        )

    label = "Question" if system else "Вопрос"
    user = f"{label}: {question}\n\n{r.context}"
    text = llm.answer(system or _SYSTEM, user, max_tokens=max_tokens)
    return Answer(
        text=text,
        sources=r.sources,
        context=r.context,
        hits=len(r.hits),
        neighbors=len(r.neighbors),
    )


def retrieve_subgraph(
    settings: Settings,
    question: str,
    *,
    k: int = 8,
    mode: str = "bfs",
    depth: int = 2,
    token_budget: int = 2000,
    max_seeds: int = 3,
    namespaces: Sequence[str] = (),
    embedder: Embedder | None = None,
    allowed_scopes: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Расширение по графу от vector-first seed'ов + сериализация подграфа под токен-бюджет.

    ``allowed_scopes`` — видимость читающего (MEM-ADR-019; None — без ограничения):
    seed'ы — только из видимых чанков, обход — только по видимым узлам.

    Глубже, чем ``retrieve`` (1–2 hop соседей): seed-узлы берутся из pgvector (с отсечением
    по разрыву скоров), затем BFS/DFS на ``depth`` шагов по проекции графа в NetworkX,
    и результат рендерится текстом с цитатами на source_file под бюджет токенов. Для
    обзорных запросов «как связано X с Y» поверх обычного chunk-retrieval.
    """
    embedder = embedder or build_embedder(settings)
    nss = resolve_namespaces(namespaces, settings.default_namespace)
    conn = dbmod.connect(settings, autocommit=True)
    try:
        index = VectorIndex(
            conn, settings.chunks_table, settings.embedding_dim, settings.default_namespace
        )
        q_emb = embedder.embed_one(question)
        hits = index.search(q_emb, question, k=k, namespaces=nss, allowed_scopes=allowed_scopes)

        # Seed'ы из pgvector: максимум-скор на node_key, затем отсечение по разрыву.
        best: dict[str, float] = {}
        for h in hits:
            if h.node_key not in best or h.score > best[h.node_key]:
                best[h.node_key] = h.score
        scored = sorted(best.items(), key=lambda kv: (-kv[1], kv[0]))
        seeds = pick_seeds([(s, nk) for nk, s in scored], max_k=max_seeds)

        store = GraphStore(conn, settings.graph_name, settings.default_namespace)
        G = to_networkx(store, namespaces=nss, allowed_scopes=allowed_scopes)
        if allowed_scopes is not None:
            seeds = [n for n in seeds if n in G]  # чанк виден, а его узел — нет
        nodes, edges, text = expand_subgraph(
            G, seeds, mode=mode, depth=depth, token_budget=token_budget
        )
        sources = [
            Source(
                source_path=str(G.nodes[n].get("source_file", "")),
                node_key=n,
                title=str(G.nodes[n].get("label", n)),
            )
            for n in seeds
            if n in G
        ]
        return {
            "seeds": seeds,
            "node_count": len(nodes),
            "edge_count": len(edges),
            "text": text,
            "sources": sources,
        }
    finally:
        conn.close()


def write_and_index_fact(
    settings: Settings,
    natural_key: str,
    node_type: str,
    title: str,
    *,
    text: str | None = None,
    properties: dict[str, Any] | None = None,
    links: list[str] | None = None,
    run_id: str | None = None,
    confidence: float = 0.8,
    source: str | None = None,
    trace_id: str | None = None,
    namespace: str = "",
    meta: dict[str, Any] | None = None,
    embedder: Embedder | None = None,
    allowed_scopes: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Записать агентский факт в граф И проиндексировать чанк для retrieval с цитатой.

    Замыкает разрыв M1.4: write_fact пишет только в граф; этот хелпер дополнительно
    добавляет чанк в векторный индекс, чтобы retrieve() нашёл факт и вернул source_path
    с корректной цитатой на агентский источник (не .md-файл vault).
    Запись — ровно в один ``namespace`` (пусто -> default).

    ``allowed_scopes`` — видимость пишущего (None — без ограничения): узел или чанк
    ключа вне неё не перезаписывается — :class:`ForeignObjectError` до любой записи;
    scopes существующего объекта сливаются, а не стираются, и переходят на чанк
    (MEM-ADR-019).
    """
    embedder = embedder or build_embedder(settings)
    source_path = source or f"agent:run/{run_id or '-'}"
    ns = resolve_namespace(namespace, settings.default_namespace)

    conn = dbmod.connect(settings, autocommit=True)
    try:
        graph = GraphStore(conn, settings.graph_name, settings.default_namespace)
        graph.ensure_schema()
        attach_kind_guard(graph, settings)  # строгий режим видов namespace (MEM-ADR-020)
        index = VectorIndex(
            conn, settings.chunks_table, settings.embedding_dim, settings.default_namespace
        )
        index.ensure_schema()
        check_write_scopes(meta_scopes(meta), allowed_scopes)  # scopes чанков — свои
        # Текст чанка: явный text > properties["content"] > title. Эмбеддинг — до лока
        # ключа: провайдер может быть сетевым, а лок держит других пишущих.
        chunk = Chunk(
            node_key=natural_key,
            text=text or (properties or {}).get("content") or title,
            source_path=source_path,
            title=title,
            namespace=ns,
        )
        emb = embedder.embed_one(chunk.embedding_input())
        # Проверка чанков и узла ключа и запись — под одним локом ключа (MEM-ADR-019).
        with index.key_write_lock(natural_key, ns):
            chunk_scopes = index.guard_chunk_write(natural_key, ns, allowed_scopes)
            result = graph.write_fact(
                natural_key,
                node_type,
                title,
                properties=properties,
                links=links,
                run_id=run_id,
                confidence=confidence,
                source=source,
                trace_id=trace_id,
                namespace=ns,
                allowed_scopes=allowed_scopes,
            )
            written = graph.get_node(natural_key, namespaces=[ns]) or {}
            chunk.meta = meta_with_scopes(meta, node_scopes(written), chunk_scopes)
            index.add_chunks([chunk], [emb])
    finally:
        conn.close()

    return result


def _node_scopes(node: dict[str, Any]) -> list[str]:
    props = node.get("props") if isinstance(node.get("props"), dict) else {}
    raw = props.get("scopes") if props else None
    return [str(s) for s in raw] if isinstance(raw, list | tuple) else []

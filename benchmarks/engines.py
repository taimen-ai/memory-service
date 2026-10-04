"""Замер AGE против таблиц на одних данных (MEM-ADR-023 п.6, TASK-001286).

Вызывается как ``bench.py engines``. Нужна база, где граф ``--graph`` есть и в AGE, и в
таблицах ``graph_nodes``/``graph_edges`` — то есть после ``cb migrate-graph-from-age``
(синтетический граф — ``graph_gen.py``, рабочий — копия базы установки). Данные не
меняются, кроме таблицы трасс Context Compiler (``--traces-table``, удаляется в конце).

Конфигурации — пары ``движок:jit``: ``age:off`` (как работал сервис на AGE, ADR-010),
``tables:off`` (текущий дефолт ``CB_DB_JIT=false``), ``tables:on`` (без обхода ADR-010),
``age:on`` (справочно). У каждой — своё соединение; JIT задаётся ``SET jit`` сессии.

Операции и выборки детерминированы по ``--seed``:

- ключи — половина равномерно среди узлов namespace со связями, половина среди
  верхних 10 % по степени (хабы, типичные якоря), через один; порядок обхода один для
  всех конфигураций; у операций по одному ключу квантили есть и отдельно по хабам и
  обычным узлам;
- ``entity_lookup`` — ``GraphStore.get_node``; ``one_hop``/``two_hop`` —
  ``expand_neighbors`` на 1/2 хопа (лимит 40, как по умолчанию);
  ``neighborhood`` — 5 ключей, 1 хоп, лимит ``CB_CONTEXT_MAX_NODES``;
- ``temporal_filter`` — ``FactStore.facts(subject, as_of)`` по субъектам фактов, ``as_of`` —
  медиана ``valid_from`` фактов namespace;
- ``hybrid`` — ``build_context`` (стратегия hybrid): запрос — начало текста случайного
  чанка, якоря — два ключа (хаб и обычный), ``allowed_scopes`` — несколько scopes из
  данных (фильтр видимости), эмбеддинг запроса — детерминированный fake-эмбеддер
  размерности таблицы чанков.

Перед замером — сверка ответов движков на первых ключах (одинаковые множества узлов и
фактов при снятом лимите). Замер: ``--warmup`` прогревочных вызовов, затем
``--iterations`` вызовов на операцию и конфигурацию (однопоточно, по кругу ключей;
прогрев ограничен четвертью бюджета);
время — от вызова метода до разобранного ответа (как его видит сервис). Конфигурации
идут вперемешку по операциям, чтобы дрейф машины ложился на все одинаково. Операция,
не уложившаяся в ``--budget-seconds``, останавливается с ``n`` меньше заданного.

Для каждой операции записывается максимальная оценка стоимости плана SQL (``EXPLAIN``)
против ``jit_above_cost`` — включился бы JIT. ``--explain-dir`` дополнительно пишет
``EXPLAIN (ANALYZE, BUFFERS)`` всех запросов одного вызова каждой операции.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import statistics
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql as psql

from benchmarks.graph_ops import AgeBenchOps, TableBenchOps, age_connect
from platform_memory.core.config import Settings
from platform_memory.index import build_embedder

OPS = ("entity_lookup", "one_hop", "two_hop", "neighborhood", "temporal_filter", "hybrid")
KEYED_OPS = ("entity_lookup", "one_hop", "two_hop")
DEFAULT_CONFIGS = "age:off,tables:off,tables:on,age:on"


class RecordingCursor(psycopg.Cursor):
    """Курсор, который при включённой записи запоминает SELECT-запросы и параметры."""

    def execute(self, query, params=None, **kw):  # type: ignore[override]
        log = getattr(self.connection, "_bench_log", None)
        if log is not None:
            log.append((query, params))
        return super().execute(query, params, **kw)


def _connect(engine: str, dsn: str, jit: bool) -> psycopg.Connection:
    if engine == "age":
        conn = age_connect(dsn, jit=jit)
    else:
        conn = psycopg.connect(dsn, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("SET jit = " + ("on" if jit else "off"))
    conn.cursor_factory = RecordingCursor
    return conn


def _order(seed: int, value: str) -> str:
    return hashlib.sha256(f"{seed}|{value}".encode()).hexdigest()


def _sample(conn: psycopg.Connection, graph: str, ns: str, n: int, seed: int) -> dict[str, Any]:
    """Детерминированные выборки ключей, субъектов фактов, scopes и запросов."""
    N = psql.Identifier(graph, "graph_nodes")
    E = psql.Identifier(graph, "graph_edges")
    with conn.cursor() as cur:
        cur.execute(
            psql.SQL(
                "WITH d AS (SELECT n.natural_key, "
                " (SELECT count(*) FROM {E} e WHERE e.src_id = n.id) + "
                " (SELECT count(*) FROM {E} e WHERE e.dst_id = n.id) AS deg "
                " FROM {N} n WHERE n.namespace = %s) "
                "SELECT natural_key, sum(deg)::int FROM d GROUP BY natural_key HAVING sum(deg) > 0"
            ).format(N=N, E=E),
            (ns,),
        )
        degrees = {k: int(d) for k, d in cur.fetchall()}
        cur.execute(
            psql.SQL(
                "SELECT DISTINCT a.natural_key FROM {E} e JOIN {N} a ON a.id = e.src_id "
                "WHERE e.namespace = %s AND e.fact_id IS NOT NULL"
            ).format(N=N, E=E),
            (ns,),
        )
        subjects = sorted((r[0] for r in cur.fetchall()), key=lambda k: _order(seed, k))[:n]
        cur.execute(
            psql.SQL(
                "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY valid_from) FROM {E} "
                "WHERE namespace = %s AND fact_id IS NOT NULL AND valid_from <> ''"
            ).format(E=E),
            (ns,),
        )
        as_of = (cur.fetchone() or [None])[0] or ""
        cur.execute(
            psql.SQL(
                "SELECT DISTINCT s FROM {N} n, "
                "jsonb_array_elements_text(CASE WHEN jsonb_typeof(n.props->'props'->'scopes') "
                "= 'array' THEN n.props->'props'->'scopes' ELSE '[]' END) s "
                "WHERE n.namespace = %s"
            ).format(N=N),
            (ns,),
        )
        scopes = sorted((r[0] for r in cur.fetchall()), key=lambda s: _order(seed, s))
    by_degree = sorted(degrees, key=lambda k: (-degrees[k], k))
    hubs = by_degree[: max(1, len(by_degree) // 10)]
    rest = sorted(degrees, key=lambda k: _order(seed, k))
    hub_pick = sorted(hubs, key=lambda k: _order(seed, k))[: n // 2]
    uni_pick = [k for k in rest if k not in set(hub_pick)][: n - len(hub_pick)]
    keys = [k for pair in zip(hub_pick, uni_pick, strict=False) for k in pair]
    keys += [k for k in hub_pick + uni_pick if k not in set(keys)]
    return {
        "keys": keys,
        "hub_keys": hub_pick,
        "uniform_keys": uni_pick,
        "subjects": subjects,
        "as_of": as_of,
        "scopes": scopes[: max(1, len(scopes) // 4)],
        "degree_p50": statistics.median(degrees.values()) if degrees else 0,
        "degree_max": max(degrees.values()) if degrees else 0,
        "hub_degree_min": degrees[hubs[-1]] if hubs else 0,
    }


def _queries(
    conn: psycopg.Connection, settings: Settings, ns: str, n: int, seed: int
) -> tuple[list[str], int]:
    """Тексты запросов гибридного канала и размерность вектора.

    Источник — начало случайных чанков namespace; нет чанков — начало наблюдений
    (``content <> ''``), нет и их — ``natural_key`` узлов графа. Крупнейший namespace
    копии установки бывает без чанков (на копии staging — ``ZeroDivisionError``
    в ``hybrid`` на пустом списке запросов).
    """
    sources = (
        (settings.chunks_table, "text", "text <> ''"),
        (settings.observations_table, "content", "content <> ''"),
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT atttypmod FROM pg_attribute WHERE attrelid = %s::regclass "
            "AND attname = 'embedding'",
            (f'public."{settings.chunks_table}"',),
        )
        dim = int(cur.fetchone()[0])
        rows: list[tuple[Any, Any]] = []
        for table, column, cond in sources:
            cur.execute("SELECT to_regclass(%s)", (f'public."{table}"',))
            if cur.fetchone()[0] is None:
                continue
            cur.execute(
                psql.SQL("SELECT id, {c} FROM {t} WHERE namespace = %s AND " + cond).format(
                    c=psql.Identifier(column), t=psql.Identifier("public", table)
                ),
                (ns,),
            )
            rows = cur.fetchall()
            if rows:
                break
        if not rows:
            cur.execute(
                psql.SQL("SELECT id, natural_key FROM {N} WHERE namespace = %s").format(
                    N=psql.Identifier(settings.graph_name, "graph_nodes")
                ),
                (ns,),
            )
            rows = cur.fetchall()
    rows = sorted(rows, key=lambda r: _order(seed, str(r[0])))[:n]
    queries = [q for q in (" ".join(str(t).split()[:12]) for _, t in rows) if q]
    if not queries:
        raise SystemExit(f"В namespace {ns!r} нет ни чанков, ни наблюдений, ни узлов графа")
    return queries, dim


def _calls(
    ops: Any, sample: dict[str, Any], queries: list[str], ns: str, settings: Settings, embedder: Any
) -> dict[str, Callable[[int], Any]]:
    keys, subjects = sample["keys"], sample["subjects"]
    hubs, uni = sample["hub_keys"], sample["uniform_keys"]
    as_of = sample["as_of"]
    allowed = sample["scopes"]
    width = settings.context_max_nodes

    def hybrid(i: int) -> Any:
        return ops.hybrid_context(
            {
                "query": queries[i % len(queries)],
                "anchors": [hubs[i % len(hubs)], uni[i % len(uni)]],
                "namespaces": [ns],
                "allowed_scopes": allowed,
                "max_tokens": 4000,
            },
            embedder,
        )

    return {
        "entity_lookup": lambda i: ops.entity_lookup(keys[i % len(keys)]),
        "one_hop": lambda i: ops.one_hop(keys[i % len(keys)]),
        "two_hop": lambda i: ops.two_hop(keys[i % len(keys)]),
        "neighborhood": lambda i: ops.neighborhood(
            [keys[(i + j) % len(keys)] for j in range(5)], width
        ),
        "temporal_filter": lambda i: ops.temporal_filter(subjects[i % len(subjects)], as_of),
        "hybrid": hybrid,
    }


def _quantiles(samples: list[float]) -> dict[str, Any]:
    if not samples:
        return {"n": 0}
    s = sorted(samples)
    return {
        "p50_ms": round(statistics.median(s) * 1000, 3),
        "p95_ms": round(s[max(0, int(round(len(s) * 0.95)) - 1)] * 1000, 3),
        "mean_ms": round(statistics.fmean(s) * 1000, 3),
        "max_ms": round(s[-1] * 1000, 3),
        "n": len(s),
    }


def _verify(age: AgeBenchOps, tables: TableBenchOps, sample: dict[str, Any]) -> dict[str, Any]:
    """Сверка ответов: одинаковые множества узлов и фактов при снятом лимите."""

    def nodes(rows: list[dict[str, Any]]) -> set[tuple[str, str]]:
        return {(str(r.get("natural_key")), str(r.get("type"))) for r in rows}

    out: dict[str, Any] = {}
    keys = sample["keys"][:10]
    out["entity_lookup"] = sum(
        bool(age.entity_lookup(k)) != bool(tables.entity_lookup(k)) for k in keys
    )
    out["one_hop"] = sum(
        nodes(age.one_hop(k, limit=10**6)) != nodes(tables.one_hop(k, limit=10**6)) for k in keys
    )
    # Два хопа от хаба у AGE — тяжёлый перебор путей; сверяем на обычных ключах.
    two = sample["uniform_keys"][:5]
    out["two_hop"] = sum(
        nodes(age.two_hop(k, limit=10**6)) != nodes(tables.two_hop(k, limit=10**6)) for k in two
    )
    as_of = sample["as_of"]
    out["temporal_filter"] = sum(
        {f["fact_id"] for f in age.temporal_filter(s, as_of)}
        != {f["fact_id"] for f in tables.temporal_filter(s, as_of)}
        for s in sample["subjects"][:10]
    )
    out["checked"] = {"keys": len(keys), "two_hop_keys": len(two), "subjects": 10}
    return out


def _explain(
    conn: psycopg.Connection,
    call: Callable[[int], Any],
    i: int,
    analyze_to: Path | None,
    title: str,
) -> dict[str, Any]:
    """Оценка стоимости планов SQL одного вызова; при ``analyze_to`` — ANALYZE+BUFFERS в файл."""
    conn._bench_log = []  # type: ignore[attr-defined]
    try:
        call(i)
    finally:
        log, conn._bench_log = conn._bench_log, None  # type: ignore[attr-defined]
    costs: list[float] = []
    texts: list[str] = []
    for query, params in log:
        text = query.as_string(conn) if isinstance(query, psql.Composable) else str(query)
        head = text.lstrip().split(None, 1)[0].upper() if text.strip() else ""
        if head not in ("SELECT", "WITH"):
            continue
        with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            try:
                psycopg.Cursor.execute(cur, "EXPLAIN (FORMAT JSON) " + text, params)
                plan = cur.fetchone()[0]
                plan = plan if isinstance(plan, list) else json.loads(plan)
                costs.append(float(plan[0]["Plan"]["Total Cost"]))
                if analyze_to is not None:
                    psycopg.Cursor.execute(cur, "EXPLAIN (ANALYZE, BUFFERS) " + text, params)
                    texts.append(
                        f"-- {text}\n-- params: {params!r}\n"
                        + "\n".join(r[0] for r in cur.fetchall())
                    )
            except psycopg.Error as exc:  # запрос без плана (например, DDL) — пропуск
                texts.append(f"-- {text}\n-- EXPLAIN не выполнен: {exc}")
    if analyze_to is not None:
        analyze_to.mkdir(parents=True, exist_ok=True)
        (analyze_to / f"{title}.txt").write_text("\n\n".join(texts) + "\n", encoding="utf-8")
    return {"queries": len(costs), "max_cost": round(max(costs), 1) if costs else 0.0}


def _environment(conn: psycopg.Connection, graph: str, ns: str, settings: Settings) -> dict:
    env: dict[str, Any] = {
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
        }
    }
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        env["postgres"] = cur.fetchone()[0]
        cur.execute("SELECT extname, extversion FROM pg_extension ORDER BY 1")
        env["extensions"] = dict(cur.fetchall())
        settings_rows = {}
        for name in ("shared_buffers", "work_mem", "effective_cache_size", "jit_above_cost"):
            cur.execute(psql.SQL("SHOW {}").format(psql.Identifier(name)))
            settings_rows[name] = cur.fetchone()[0]
        env["pg_settings"] = settings_rows
        N = psql.Identifier(graph, "graph_nodes")
        E = psql.Identifier(graph, "graph_edges")
        cur.execute(
            psql.SQL("SELECT count(*), count(*) FILTER (WHERE namespace = %s) FROM {N}").format(
                N=N
            ),
            (ns,),
        )
        nodes_all, nodes_ns = cur.fetchone()
        cur.execute(
            psql.SQL(
                "SELECT count(*), count(*) FILTER (WHERE namespace = %s), "
                "count(*) FILTER (WHERE fact_id IS NOT NULL) FROM {E}"
            ).format(E=E),
            (ns,),
        )
        edges_all, edges_ns, facts = cur.fetchone()
        cur.execute(
            psql.SQL("SELECT count(*) FROM {t}").format(
                t=psql.Identifier("public", settings.chunks_table)
            )
        )
        chunks = cur.fetchone()[0]
        cur.execute(
            "SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))) FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = %s "
            "AND c.relkind = 'r' AND c.relname NOT IN ('graph_nodes', 'graph_edges')",
            (graph,),
        )
        age_size = cur.fetchone()[0]
        cur.execute(
            "SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))) FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = %s "
            "AND c.relname IN ('graph_nodes', 'graph_edges')",
            (graph,),
        )
        tables_size = cur.fetchone()[0]
    env["data"] = {
        "graph": graph,
        "namespace": ns,
        "nodes": nodes_all,
        "nodes_in_namespace": nodes_ns,
        "edges": edges_all,
        "edges_in_namespace": edges_ns,
        "fact_edges": facts,
        "chunks": chunks,
        "age_size": age_size,
        "tables_size": tables_size,
    }
    return env


def markdown(report: dict[str, Any]) -> str:
    """Таблица p50/p95 (мс) по операциям и конфигурациям + оценка плана — для отчёта."""
    configs = list(next(iter(report["results"].values()), {}))
    head = "| Операция | " + " | ".join(f"{c} p50 / p95" for c in configs) + " |"
    lines = [head, "|---|" + "---:|" * len(configs)]
    for op, by_cfg in report["results"].items():
        cells = []
        for c in configs:
            r = by_cfg.get(c, {})
            cell = f"{r.get('p50_ms', '—')} / {r.get('p95_ms', '—')}"
            if r.get("truncated"):
                cell += f" (n={r['n']})"
            cells.append(cell)
        lines.append(f"| `{op}` | " + " | ".join(cells) + " |")
    lines += ["", "| Операция | " + " | ".join(f"{c} max cost" for c in configs) + " |"]
    lines.append("|---|" + "---:|" * len(configs))
    for op, by_cfg in report["plans"].items():
        cells = [str(by_cfg.get(c, {}).get("max_cost", "—")) for c in configs]
        lines.append(f"| `{op}` | " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def _main_namespace(conn: psycopg.Connection, graph: str) -> str:
    with conn.cursor() as cur:
        cur.execute(
            psql.SQL(
                "SELECT namespace FROM {N} GROUP BY namespace ORDER BY count(*) DESC, namespace LIMIT 1"
            ).format(N=psql.Identifier(graph, "graph_nodes"))
        )
        row = cur.fetchone()
    if row is None:
        raise SystemExit(
            f"в таблицах графа {graph!r} нет узлов — сначала cb migrate-graph-from-age"
        )
    return str(row[0])


def run(argv: list[str]) -> dict[str, Any]:
    parser = argparse.ArgumentParser(prog="bench.py engines", description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--dsn", default=os.environ.get("CB_DATABASE_URL"), help="База с AGE и таблицами графа"
    )
    parser.add_argument("--graph", default=os.environ.get("CB_GRAPH_NAME", "company_brain"))
    parser.add_argument(
        "--namespace", default="", help="Namespace замера (по умолчанию — самый крупный)"
    )
    parser.add_argument("--chunks-table", default=os.environ.get("CB_CHUNKS_TABLE", "chunks"))
    parser.add_argument(
        "--observations-table", default=os.environ.get("CB_OBSERVATIONS_TABLE", "observations")
    )
    parser.add_argument(
        "--traces-table", default="bench_context_traces", help="Трассы компилятора (удаляется)"
    )
    parser.add_argument("--configs", default=DEFAULT_CONFIGS, help="Пары движок:jit через запятую")
    parser.add_argument("--ops", default=",".join(OPS))
    parser.add_argument("--keys", type=int, default=50, help="Размер выборки ключей")
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--iterations", type=int, default=200)
    parser.add_argument(
        "--budget-seconds", type=float, default=120.0, help="Предел на операцию и конфигурацию"
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--explain-dir", default="", help="Куда писать EXPLAIN (ANALYZE, BUFFERS)")
    parser.add_argument("--no-verify", action="store_true", help="Без сверки ответов движков")
    parser.add_argument("--out", default="", help="JSON-отчёт в файл (иначе stdout)")
    parser.add_argument("--markdown", default="", help="Таблица p50/p95 в Markdown-файл")
    args = parser.parse_args(argv)
    if not args.dsn:
        raise SystemExit("Задайте --dsn или CB_DATABASE_URL")

    configs = [tuple(c.strip().split(":")) for c in args.configs.split(",") if c.strip()]
    ops_wanted = [o.strip() for o in args.ops.split(",") if o.strip()]
    probe = psycopg.connect(args.dsn, autocommit=True)
    try:
        ns = args.namespace or _main_namespace(probe, args.graph)
        base = Settings(
            _env_file=None,
            database_url=args.dsn,
            graph_name=args.graph,
            default_namespace=ns,
            chunks_table=args.chunks_table,
            observations_table=args.observations_table,
            context_traces_table=args.traces_table,
            embedding_provider="fake",
            llm_provider="echo",
        )
        sample = _sample(probe, args.graph, ns, args.keys, args.seed)
        queries, dim = _queries(probe, base, ns, max(10, args.keys), args.seed)
        settings = base.model_copy(update={"embedding_dim": dim})
        env = _environment(probe, args.graph, ns, settings)
    finally:
        probe.close()
    embedder = build_embedder(settings)

    conns: dict[tuple[str, str], psycopg.Connection] = {}
    engines: dict[tuple[str, str], Any] = {}
    for engine, jit in configs:
        conn = _connect(engine, args.dsn, jit == "on")
        conn._bench_log = None  # type: ignore[attr-defined]
        conns[(engine, jit)] = conn
        cls = AgeBenchOps if engine == "age" else TableBenchOps
        engines[(engine, jit)] = cls(settings, conn)

    report: dict[str, Any] = {
        "seed": args.seed,
        "iterations": args.iterations,
        "warmup": args.warmup,
        "budget_seconds": args.budget_seconds,
        "environment": env,
        "sample": {
            "keys": len(sample["keys"]),
            "hub_keys": len(sample["hub_keys"]),
            "subjects": len(sample["subjects"]),
            "as_of": sample["as_of"],
            "allowed_scopes": sample["scopes"],
            "degree_p50": sample["degree_p50"],
            "hub_degree_min": sample["hub_degree_min"],
            "degree_max": sample["degree_max"],
            "queries": len(queries),
            "embedding_dim": dim,
        },
        "results": {},
        "plans": {},
    }
    explain_dir = Path(args.explain_dir) if args.explain_dir else None
    try:
        age_key = next((c for c in engines if c[0] == "age"), None)
        tab_key = next((c for c in engines if c[0] == "tables"), None)
        if not args.no_verify and age_key and tab_key:
            report["verify_mismatches"] = _verify(engines[age_key], engines[tab_key], sample)
        calls = {
            cfg: _calls(engines[cfg], sample, queries, ns, settings, embedder) for cfg in engines
        }
        for op in ops_wanted:
            report["results"][op] = {}
            report["plans"][op] = {}
            for cfg in engines:
                call = calls[cfg][op]
                name = f"{cfg[0]}:{cfg[1]}"
                report["plans"][op][name] = _explain(
                    conns[cfg], call, 0, explain_dir, f"{op}__{cfg[0]}_jit-{cfg[1]}"
                )
                # Прогрев тоже ограничен: четверть бюджета (у AGE вызов бывает десятки секунд).
                started = time.monotonic()
                for i in range(args.warmup):
                    call(i)
                    if time.monotonic() - started > args.budget_seconds / 4:
                        break
                samples: list[float] = []
                started = time.monotonic()
                truncated = False
                for i in range(args.iterations):
                    t0 = time.perf_counter()
                    call(i)
                    samples.append(time.perf_counter() - t0)
                    if time.monotonic() - started > args.budget_seconds:
                        truncated = i + 1 < args.iterations
                        break
                stats = _quantiles(samples)
                if truncated:
                    stats["truncated"] = True
                if op in KEYED_OPS:
                    # Ключи чередуются: чётный вызов — хаб, нечётный — обычный узел.
                    stats["hub"] = _quantiles(samples[0::2])
                    stats["uniform"] = _quantiles(samples[1::2])
                report["results"][op][name] = stats
                print(f"{op:16s} {name:11s} {json.dumps(stats)}", flush=True)
    finally:
        for conn in conns.values():
            conn.close()
        cleanup = psycopg.connect(args.dsn, autocommit=True)
        try:
            with cleanup.cursor() as cur:
                cur.execute(
                    psql.SQL("DROP TABLE IF EXISTS {}").format(
                        psql.Identifier("public", args.traces_table)
                    )
                )
        finally:
            cleanup.close()
    if args.out:
        Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=1) + "\n")
    if args.markdown:
        Path(args.markdown).write_text(markdown(report), encoding="utf-8")
    return report

"""Benchmark suite Context Memory Engine (ADR-016, ТЗ §42).

Вне pytest: замеры на реальной БД с fake-эмбеддером (сеть не нужна; семантика
вектора не важна для латентности — важна размерность и объём).

Запуск (изолированная БД, датасеты 1k/10k/100k):

    CB_TEST_DATABASE_URL=postgresql://brain:brain@127.0.0.1:5435/company_brain \
      uv run python benchmarks/bench.py --size 1000

Сценарии: observation ingest (single/batch), lexical/vector retrieval,
graph expansion (GraphBenchOps), hybrid build_context на трёх бюджетах.
Результат — JSON в stdout (медиана/квантили по латентности, rps по ingest).

Сравнение движков графа (MEM-ADR-023 п.6): ``bench.py engines --dsn … --graph …`` —
GraphBenchOps для AGE и для таблиц на одних данных, гибридный запрос Context Compiler,
JIT вкл./выкл. (``benchmarks/engines.py``, порядок прогона — ``benchmarks/graph-engines.md``).
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from benchmarks.graph_ops import TableBenchOps  # noqa: E402

from platform_memory import build_context, retain_observation, retain_observations  # noqa: E402
from platform_memory.core import db as dbmod  # noqa: E402
from platform_memory.core.config import Settings  # noqa: E402
from platform_memory.index import VectorIndex, build_embedder  # noqa: E402

NS = "bench"

WORDS = (
    "деплой регрессия мигратор клиент возврат заказ сервер память граф "
    "контекст наблюдение факт документ поиск индекс проект встреча решение"
).split()


def _content(i: int) -> str:
    base = " ".join(WORDS[(i + j) % len(WORDS)] for j in range(12))
    return f"{base} ticket T-{i:05d} sha {uuid.uuid5(uuid.NAMESPACE_DNS, str(i)).hex[:12]}"


def _observation(i: int) -> dict:
    obs: dict = {
        "source": {"system": "bench", "stream": "events", "external_id": f"ev-{i}"},
        "kind": "bench.event",
        "occurred_at": f"2026-08-{(i % 28) + 1:02d}T10:00:00Z",
        "scopes": [f"project:p{i % 20}"],
        "content": _content(i),
    }
    if i % 10 == 0:  # каждый десятый несёт structured assertions
        obs["assertions"] = [
            {"assert": "entity", "entity": {"type": "person", "id": f"u{i % 50}"}},
            {"assert": "entity", "entity": {"type": "project", "id": f"p{i % 20}"}},
            {
                "assert": "fact",
                "fact": {
                    "subject": f"person:u{i % 50}",
                    "predicate": "WORKS_ON",
                    "object": f"project:p{i % 20}",
                    "valid_from": "2026-01-01T00:00:00Z",
                },
            },
        ]
    return obs


def _quantiles(samples: list[float]) -> dict:
    if not samples:
        return {}
    s = sorted(samples)
    return {
        "p50_ms": round(statistics.median(s) * 1000, 2),
        "p95_ms": round(s[int(len(s) * 0.95) - 1] * 1000, 2),
        "max_ms": round(s[-1] * 1000, 2),
        "n": len(s),
    }


def main() -> None:
    if sys.argv[1:2] == ["engines"]:
        from benchmarks.engines import run

        report = run(sys.argv[2:])
        print(json.dumps(report, ensure_ascii=False, indent=1))
        return
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=1000, help="Число наблюдений (1k/10k/100k)")
    parser.add_argument("--queries", type=int, default=30, help="Число замеров retrieval")
    parser.add_argument("--keep", action="store_true", help="Не удалять данные после прогона")
    args = parser.parse_args()

    db_url = os.environ.get("CB_TEST_DATABASE_URL") or os.environ.get("CB_DATABASE_URL")
    if not db_url:
        sys.exit("Задайте CB_TEST_DATABASE_URL (изолированная БД для бенчмарка)")

    suffix = uuid.uuid4().hex[:6]
    settings = Settings(
        _env_file=None,
        database_url=db_url,
        graph_name=f"bench_{suffix}",
        chunks_table=f"bench_chunks_{suffix}",
        observations_table=f"bench_obs_{suffix}",
        context_traces_table=f"bench_traces_{suffix}",
        default_namespace=NS,
        embedding_provider="fake",
        embedding_dim=256,
        llm_provider="echo",
    )
    embedder = build_embedder(settings)
    report: dict = {"size": args.size, "dim": settings.embedding_dim}

    conn = dbmod.connect(settings, autocommit=True)
    try:
        from platform_memory.graph.store import GraphStore
        from platform_memory.observations.store import ObservationStore

        GraphStore(conn, settings.graph_name, NS).ensure_schema()
        VectorIndex(conn, settings.chunks_table, settings.embedding_dim, NS).ensure_schema()
        ObservationStore(conn, settings.observations_table, NS).ensure_schema()

        # --- ingest: одиночный ---
        single_n = min(200, args.size)
        t0 = time.monotonic()
        for i in range(single_n):
            retain_observation(
                settings, _observation(i), namespace=NS, embedder=embedder, conn=conn
            )
        dt = time.monotonic() - t0
        report["ingest_single"] = {"n": single_n, "rps": round(single_n / dt, 1)}

        # --- ingest: батчами по 200 ---
        rest = args.size - single_n
        t0 = time.monotonic()
        done = single_n
        while done < args.size:
            batch = [_observation(i) for i in range(done, min(done + 200, args.size))]
            retain_observations(settings, batch, namespace=NS, embedder=embedder)
            done += len(batch)
        if rest > 0:
            dt = time.monotonic() - t0
            report["ingest_batch"] = {"n": rest, "rps": round(rest / dt, 1)}

        # --- документы для вектора (text-чанки уже созданы assertions не для всех) ---
        index = VectorIndex(conn, settings.chunks_table, settings.embedding_dim, NS)

        # --- lexical retrieval ---
        obs_store = ObservationStore(conn, settings.observations_table, NS)
        samples = []
        for q in range(args.queries):
            ticket = f"T-{(q * 7) % args.size:05d}"
            t0 = time.monotonic()
            got = obs_store.search_lexical("", [ticket], namespaces=[NS], limit=8)
            samples.append(time.monotonic() - t0)
            assert got, f"lexical не нашёл {ticket}"
        report["lexical"] = _quantiles(samples)

        # --- vector retrieval (по чанкам text-assertions нет — используем observations FTS
        #     + вектор по чанкам, если есть; замер честного index.search) ---
        if index.count_chunks([NS]):
            samples = []
            for q in range(args.queries):
                emb = embedder.embed_one(_content(q))
                t0 = time.monotonic()
                index.search(emb, _content(q), k=8, namespaces=[NS])
                samples.append(time.monotonic() - t0)
            report["vector"] = _quantiles(samples)

        # --- graph expansion (GraphBenchOps) ---
        ops = TableBenchOps(settings, conn)
        keys = [f"person:u{i}" for i in range(0, 50, 5)]
        for name, fn in (
            ("entity_lookup", lambda k: ops.entity_lookup(k)),
            ("one_hop", lambda k: ops.one_hop(k)),
            ("two_hop", lambda k: ops.two_hop(k)),
            ("temporal_filter", lambda k: ops.temporal_filter(k, "2026-06-01T00:00:00Z")),
        ):
            samples = []
            for key in keys:
                t0 = time.monotonic()
                fn(key)
                samples.append(time.monotonic() - t0)
            report[f"graph_{name}"] = _quantiles(samples)

        # --- hybrid build_context на трёх бюджетах ---
        for budget in (2000, 8000, 16000):
            samples = []
            for q in range(min(args.queries, 10)):
                t0 = time.monotonic()
                pack = build_context(
                    settings,
                    {
                        "query": _content(q * 3),
                        "scopes": [f"project:p{q % 20}"],
                        "anchors": [f"person:u{q % 50}"],
                        "max_tokens": budget,
                        "namespaces": [NS],
                    },
                    embedder=embedder,
                    conn=conn,
                )
                samples.append(time.monotonic() - t0)
                assert pack.token_estimate <= budget
            report[f"context_budget_{budget}"] = _quantiles(samples)
    finally:
        if not args.keep:
            try:
                dbmod.drop_graph(conn, settings.graph_name)
                with conn.cursor() as cur:
                    for table in (
                        settings.chunks_table,
                        settings.observations_table,
                        settings.context_traces_table,
                    ):
                        cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            except Exception:  # noqa: BLE001 — зачистка best-effort
                pass
        conn.close()

    print(json.dumps(report, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()

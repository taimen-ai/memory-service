"""Профиль typed-обхода ``/api/memory/context/typed`` на графе размера staging.

MEM-ADR-020, амендмент «пакетное разрешение якорей и обход одним запросом на шаг».
Генерирует снимки software-delivery пяти синтетических репозиториев (тысячи сущностей,
связи ``calls``/``defined_in``/``governs``/``part_of``/``emits``) в изолированный граф
и таблицы, плюс «шум» — узлы другого namespace (граф общий на все namespaces). Затем
замеряет два профиля — один якорь + один шаг и coding-task (4 якоря, 3 шага) — с
разбивкой по фазам (по вызывающей функции каждого SQL) и числом SQL на запрос.

Запуск (изолированная БД; данные удаляются, если не задан ``--keep``)::

    CB_TEST_DATABASE_URL=postgresql://… uv run python benchmarks/typed_context.py
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path

import psycopg

from platform_memory.context.typed import compile_typed_context
from platform_memory.core import db as dbmod
from platform_memory.core.config import Settings
from platform_memory.domain.reconcile import reconcile
from platform_memory.domain.registry import open_registry
from platform_memory.graph.store import GraphStore

NS = "tenant:bench:ws:w1"
NOISE_NS = "tenant:bench:ws:other"
PACK = Path(__file__).parent.parent / "tests" / "fixtures" / "software_delivery" / "pack.json"
REPOS = ("control-plane", "platform-web", "memory-service", "policy-service", "superproject")
RESOURCES = ("tasks", "runs", "goals", "approvals", "artifacts", "claims", "events", "skills")

# Фаза SQL — по ближайшей известной функции в стеке вызова.
PHASES = {
    "resolve_anchors": "anchors",
    "resolve_anchor": "anchors",
    "_ledger_hits": "anchors",
    "resolve_candidates": "anchors",
    "traverse": "traverse",
    "traverse_step": "traverse",
    "_load_versions": "visibility",
    "catalog_for": "setup",
    "open_registry": "setup",
    "ensure_graph": "setup",
    "table_exists": "setup",
    "ensure_schema": "trace",
    "record": "trace",
}


class _Profile:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.count = 0
        self.by_phase: dict[str, list[float]] = defaultdict(list)

    def note(self, seconds: float) -> None:
        self.count += 1
        frame = sys._getframe(2)
        phase = "other"
        while frame is not None:
            name = frame.f_code.co_name
            if name in PHASES:
                phase = PHASES[name]
                break
            frame = frame.f_back
        self.by_phase[phase].append(seconds)


PROFILE = _Profile()


class _TimedCursor(psycopg.Cursor):
    def execute(self, query, params=None, **kw):  # type: ignore[override]
        t0 = time.perf_counter()
        try:
            return super().execute(query, params, **kw)
        finally:
            PROFILE.note(time.perf_counter() - t0)


def _snapshot(repo: str, idx: int, scale: int) -> dict:
    """Снимок одного репозитория: файлы, эндпоинты, вызовы, события, таблицы, ADR."""
    ents: list[dict] = [{"kind": "component", "key": repo, "attributes": {}}]
    rels: list[dict] = []

    def ent(kind: str, key: str, **attrs) -> dict:
        e = {"kind": kind, "key": key, "attributes": attrs}
        ents.append(e)
        return e

    def rel(relation: str, fk: str, fkey: str, tk: str, tkey: str) -> None:
        rels.append(
            {
                "relation": relation,
                "from": {"kind": fk, "key": fkey},
                "to": {"kind": tk, "key": tkey},
            }
        )

    files = [f"{repo}:src/{repo.replace('-', '_')}/m{i}.py" for i in range(60 * scale)]
    for f in files:
        ent("source_file", f, path=f.split(":", 1)[1])
        rel("part_of", "source_file", f, "component", repo)
    n_ep = 40 * scale
    for i in range(n_ep):
        res = RESOURCES[i % len(RESOURCES)]
        for method in ("GET", "POST"):
            key = f"{method} /api/v{idx}/{res}{i // len(RESOURCES)}/{{}}"
            e = ent("endpoint", key, method=method, path=key.split(" ", 1)[1])
            e["aliases"] = [key.split(" ", 1)[1].replace("{}", f"{{{res}_ref}}")]
            e["provenance"] = {"repo": repo, "sha": "abc", "path": files[i % len(files)], "line": i}
            rel("defined_in", "endpoint", key, "source_file", files[i % len(files)])
    for i in range(20 * scale):
        ev = f"{repo.replace('-', '_')}.entity{i}.changed"
        ent("event", ev)
        rel("defined_in", "event", ev, "source_file", files[(i * 7) % len(files)])
        rel("emits", "source_file", files[(i * 3) % len(files)], "event", ev)
    for i in range(10 * scale):
        ent("table", f"{repo}:table_{i}")
        rel("defined_in", "table", f"{repo}:table_{i}", "source_file", files[i % len(files)])
    series = ("CP", "MEM", "PC", "TAI", "PW")[idx]
    for i in range(1, 8 * scale):
        key = f"{series}-{i:04d}"
        ent("adr", key, title=f"ADR {i}")
        for j in range(4):
            rel("governs", "adr", key, "source_file", files[(i * 5 + j) % len(files)])
    # Вызовы в эндпоинты «главного» репозитория (idx 0) из клиентов и UI всех репо.
    for i in range(120 * scale):
        ui = f"{repo}:src/app/page{i % 50}.tsx:{i}"
        ent("ui_call", ui, literal=f"/api/v0/{RESOURCES[i % len(RESOURCES)]}")
        rel("defined_in", "ui_call", ui, "source_file", files[i % len(files)])
        target = f"GET /api/v0/{RESOURCES[i % len(RESOURCES)]}{(i // 8) % (5 * scale)}/{{}}"
        rel("calls", "ui_call", ui, "endpoint", target)
    for i in range(20 * scale):
        cm = f"{repo}:client.Client.method{i}"
        ent("client_method", cm)
        rel("defined_in", "client_method", cm, "source_file", files[i % len(files)])
        target = f"POST /api/v0/{RESOURCES[i % len(RESOURCES)]}{i % (5 * scale)}/{{}}"
        rel("calls", "client_method", cm, "endpoint", target)
    return {
        "pack": "software-delivery@1",
        "source": f"git:{repo}",
        "scope": repo,
        "snapshotId": f"{repo}@1",
        "observedAt": "2026-09-23T10:00:00Z",
        "entities": ents,
        "relations": rels,
    }


def _noise(graph: GraphStore, conn, n: int) -> None:
    """Узлы другого namespace (граф общий): typed-запросы не должны их сканировать."""
    with conn.cursor() as cur:
        cur.execute(
            graph.sql(
                "INSERT INTO {N} (namespace, label, natural_key, type, props) "
                "SELECT %s, 'document', 'doc:' || i, 'document', "
                "jsonb_build_object('props', jsonb_build_object('aliases', jsonb_build_array())) "
                "FROM generate_series(0, %s) i"
            ),
            (NOISE_NS, n - 1),
        )


PROFILES = {
    "one_anchor_one_step": {
        "anchors": [{"kind": "endpoint", "value": "GET /api/v0/tasks0/{id}"}],
        "traverse": [{"relation": "calls", "direction": "in", "depth": 1, "limit": 20}],
    },
    "coding_task": {
        "anchors": [
            {"kind": "endpoint", "value": "GET /api/v0/tasks0/{id}"},
            {"kind": "endpoint", "value": "/api/v0/runs1/{id}"},
            {"kind": "adr", "value": "CP-ADR-0003"},
            {"kind": "event", "value": "control_plane.entity3.changed"},
        ],
        "traverse": [
            {"relation": "calls", "direction": "in", "depth": 1, "limit": 20},
            {"relation": "defined_in", "direction": "out", "depth": 1, "limit": 20},
            {"relation": "governs", "direction": "in", "from": "previous", "limit": 20},
        ],
    },
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, default=5, help="Множитель размера снимков")
    parser.add_argument("--noise", type=int, default=20000, help="Узлы чужого namespace")
    parser.add_argument("--runs", type=int, default=10, help="Замеров на профиль")
    parser.add_argument("--keep", action="store_true", help="Не удалять данные после прогона")
    args = parser.parse_args()

    db_url = os.environ.get("CB_TEST_DATABASE_URL")
    if not db_url:
        sys.exit("Задайте CB_TEST_DATABASE_URL (изолированная БД для бенчмарка)")
    suffix = uuid.uuid4().hex[:6]
    settings = Settings(
        _env_file=None,
        database_url=db_url,
        graph_name=f"bench_typed_{suffix}",
        chunks_table=f"bench_chunks_{suffix}",
        observations_table=f"bench_obs_{suffix}",
        context_traces_table=f"bench_traces_{suffix}",
        domain_packs_table=f"bench_packs_{suffix}",
        namespace_settings_table=f"bench_nss_{suffix}",
        snapshots_table=f"bench_snaps_{suffix}",
        default_namespace=NS,
        embedding_provider="fake",
        llm_provider="echo",
    )
    conn = dbmod.connect(settings, autocommit=True)
    graph = GraphStore(conn, settings.graph_name, NS)
    report: dict = {"scale": args.scale, "noise": args.noise}
    try:
        graph.ensure_schema()
        registry = open_registry(conn, settings)
        registry.ensure_schema()
        registry.register(json.loads(PACK.read_text(encoding="utf-8")))
        registry.put_settings(NS, strict=True, packages=["software-delivery"])
        t0 = time.monotonic()
        for i, repo in enumerate(REPOS):
            reconcile(settings, snapshot=_snapshot(repo, i, args.scale), namespace=NS, conn=conn)
        _noise(graph, conn, args.noise)
        with conn.cursor() as cur:
            cur.execute(graph.sql("ANALYZE {N}"))
            cur.execute(graph.sql("ANALYZE {E}"))
        report["load_s"] = round(time.monotonic() - t0, 1)
        report["graph"] = {
            "nodes": graph.count_nodes_by_type([NS]),
            "edges": graph.count_edges_by_type([NS]),
        }

        timed = dbmod.connect(settings, autocommit=True)
        timed.cursor_factory = _TimedCursor
        try:
            for name, payload in PROFILES.items():
                payload = {**payload, "namespaces": [NS]}
                compile_typed_context(settings, payload, conn=timed)  # прогрев
                total: list[float] = []
                phases: dict[str, list[float]] = defaultdict(list)
                for _ in range(args.runs):
                    PROFILE.reset()
                    t0 = time.perf_counter()
                    pack = compile_typed_context(settings, payload, conn=timed)
                    total.append(time.perf_counter() - t0)
                    for phase, samples in PROFILE.by_phase.items():
                        phases[phase].append(sum(samples))
                    queries = PROFILE.count
                    per_phase = {p: len(s) for p, s in PROFILE.by_phase.items()}
                report[name] = {
                    "p50_ms": round(statistics.median(total) * 1000, 1),
                    "max_ms": round(max(total) * 1000, 1),
                    "sql": queries,
                    "sql_by_phase": per_phase,
                    "phase_p50_ms": {
                        p: round(statistics.median(s) * 1000, 1) for p, s in sorted(phases.items())
                    },
                    "entities": pack["stats"]["entities"],
                    "facts": pack["stats"]["facts"],
                    "steps": [s["reached"] for s in pack["stats"]["steps"]],
                    "anchors": [
                        {
                            "matchedBy": a.get("matchedBy"),
                            "resolved": len(a["resolved"]),
                            "ambiguous": a.get("ambiguous", False),
                        }
                        for a in pack["anchors"]
                    ],
                }
        finally:
            timed.close()
    finally:
        if not args.keep:
            dbmod.drop_graph(conn, settings.graph_name)
            with conn.cursor() as cur:
                for table in (
                    settings.context_traces_table,
                    settings.domain_packs_table,
                    settings.namespace_settings_table,
                    settings.snapshots_table,
                    f"{settings.snapshots_table}_items",
                ):
                    cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.close()
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

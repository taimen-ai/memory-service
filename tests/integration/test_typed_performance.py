"""Число SQL typed-обхода — регрессия производительности (MEM-ADR-020, амендмент
«пакетное разрешение якорей и обход одним запросом на шаг»).

Якоря разрешаются пачкой (граф — запрос точных ключей и запрос по журналу/псевдонимам,
журнал — не больше трёх запросов на все якоря и namespaces), уровень шага обхода —
запрос рёбер и запрос концов на весь фронт. Число SQL поэтому не зависит от числа
якорей и стартовых узлов, а графовые запросы не просматривают таблицы графа целиком
(узлы всех namespaces — так typed-запрос на staging занимал секунды): каждый идёт
по индексу (MEM-ADR-023).
"""

from __future__ import annotations

import json
from pathlib import Path

import psycopg
import pytest

from platform_memory.context.typed import compile_typed_context
from platform_memory.domain.reconcile import reconcile
from platform_memory.domain.registry import open_registry

pytestmark = pytest.mark.integration

NS = "tenant:t1:ws:w1"
OTHER = "tenant:t1:ws:other"
PACK = Path(__file__).parent.parent / "fixtures" / "software_delivery" / "pack.json"
RES = ("tasks", "runs", "goals", "approvals")

# Потолки числа SQL на запрос (до пакетного разрешения в профиле бенчмарка
# ``benchmarks/typed_context.py`` — 18 и 37, причём разрешение
# и обход росли с числом якорей и узлов): служебные — 8 (граф, реестр пакетов, журнал,
# трейс), разрешение якорей — до 6 на все якоря, метки графа — 1, уровень шага
# обхода — 3 (рёбра, концы, версии журнала).
ONE_ANCHOR_MAX_SQL = 17
CODING_TASK_MAX_SQL = 24


def _snapshot() -> dict:
    ents: list[dict] = [{"kind": "component", "key": "cp", "attributes": {}}]
    rels: list[dict] = []

    def rel(name: str, fk: str, fkey: str, tk: str, tkey: str) -> None:
        rels.append(
            {"relation": name, "from": {"kind": fk, "key": fkey}, "to": {"kind": tk, "key": tkey}}
        )

    files = [f"cp:src/cp/m{i}.py" for i in range(40)]
    for f in files:
        ents.append({"kind": "source_file", "key": f, "attributes": {}})
        rel("part_of", "source_file", f, "component", "cp")
    for i in range(80):
        for method in ("GET", "POST"):
            key = f"{method} /api/v1/{RES[i % 4]}{i // 4}/{{}}"
            ents.append(
                {
                    "kind": "endpoint",
                    "key": key,
                    "attributes": {},
                    "aliases": [f"/api/v1/{RES[i % 4]}{i // 4}/{{{RES[i % 4]}_ref}}"],
                }
            )
            rel("defined_in", "endpoint", key, "source_file", files[i % 40])
    for i in range(1, 20):
        ents.append({"kind": "adr", "key": f"CP-{i:04d}", "attributes": {}})
        for j in range(3):
            rel("governs", "adr", f"CP-{i:04d}", "source_file", files[(i * 3 + j) % 40])
    for i in range(10):
        ents.append({"kind": "event", "key": f"cp.entity{i}.changed", "attributes": {}})
        rel("defined_in", "event", f"cp.entity{i}.changed", "source_file", files[i])
    for i in range(300):
        ui = f"web:src/app/page{i % 30}.tsx:{i}"
        ents.append({"kind": "ui_call", "key": ui, "attributes": {}})
        rel("calls", "ui_call", ui, "endpoint", f"GET /api/v1/{RES[i % 4]}{(i // 4) % 5}/{{}}")
    return {
        "pack": "software-delivery@1",
        "source": "git:cp",
        "scope": "cp",
        "snapshotId": "cp@1",
        "observedAt": "2026-09-23T10:00:00Z",
        "entities": ents,
        "relations": rels,
    }


@pytest.fixture
def graph_db(stores, settings):
    conn, graph, _, _, _ = stores
    reg = open_registry(conn, settings)
    reg.ensure_schema()
    reg.register(json.loads(PACK.read_text(encoding="utf-8")))
    reg.put_settings(NS, strict=True, packages=["software-delivery"])
    reconcile(settings, snapshot=_snapshot(), namespace=NS, conn=conn)
    # Узлы другого namespace в общем графе: typed-запросы NS их не читают.
    with conn.cursor() as cur:
        cur.execute(
            graph.sql(
                "INSERT INTO {N} (namespace, label, natural_key, type) "
                "SELECT %s, 'document', 'doc:' || i, 'document' FROM generate_series(0, 2999) i"
            ),
            (OTHER,),
        )
        cur.execute(graph.sql("ANALYZE {N}"))
        cur.execute(graph.sql("ANALYZE {E}"))
    return conn


class _Counting(psycopg.Cursor):
    statements: list[str] = []
    graph_reads: list[tuple[str, object]] = []

    def execute(self, query, params=None, **kw):  # type: ignore[override]
        text = str(query if isinstance(query, str) else query.as_string(self.connection))
        _Counting.statements.append(text)
        if "graph_nodes" in text and text.lstrip().upper().startswith(("SELECT", "WITH")):
            _Counting.graph_reads.append((text, params))
        return super().execute(query, params, **kw)


def _typed(settings, conn, payload: dict) -> tuple[dict, list[str]]:
    _Counting.statements = []
    _Counting.graph_reads = []
    conn.cursor_factory = _Counting
    try:
        pack = compile_typed_context(settings, {"namespaces": [NS], **payload}, conn=conn)
    finally:
        conn.cursor_factory = psycopg.Cursor
    return pack, list(_Counting.statements)


ONE = {
    "anchors": [{"kind": "endpoint", "value": "GET /api/v1/tasks0/{id}"}],
    "traverse": [{"relation": "calls", "direction": "in", "depth": 1, "limit": 20}],
}

CODING_TASK = {
    "anchors": [
        {"kind": "endpoint", "value": "GET /api/v1/tasks0/{id}"},
        {"kind": "endpoint", "value": "/api/v1/runs1/{id}"},
        {"kind": "adr", "value": "CP-ADR-0003"},
        {"kind": "event", "value": "cp.entity3.changed"},
    ],
    "traverse": [
        {"relation": "calls", "direction": "in", "depth": 1, "limit": 20},
        {"relation": "defined_in", "direction": "out", "depth": 1, "limit": 20},
        {"relation": "governs", "direction": "in", "from": "previous", "limit": 20},
    ],
}


def _no_full_graph_scan(conn) -> None:
    """Каждое чтение таблиц графа typed-пути идёт по индексу: без seq scan плана нет."""
    assert _Counting.graph_reads
    for text, params in _Counting.graph_reads:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute("SET LOCAL enable_seqscan = off")
            cur.execute("EXPLAIN " + text, params)
            plan = "\n".join(r[0] for r in cur.fetchall())
        assert "Seq Scan on graph_" not in plan, (text, plan)


def test_one_anchor_one_step_sql_budget(settings, graph_db):
    pack, statements = _typed(settings, graph_db, ONE)
    assert pack["anchors"][0]["matchedBy"] == "normalized"
    assert pack["stats"]["steps"][0]["reached"] == 15
    assert len(statements) <= ONE_ANCHOR_MAX_SQL, statements
    _no_full_graph_scan(graph_db)


def test_coding_task_profile_sql_budget(settings, graph_db):
    pack, statements = _typed(settings, graph_db, CODING_TASK)
    assert [a.get("matchedBy") for a in pack["anchors"]] == [
        "normalized",
        "normalized",
        "alias",
        "natural_key",
    ]
    assert [s["reached"] for s in pack["stats"]["steps"]] == [20, 3, 4]
    assert pack["unresolved"] == []
    assert len(statements) <= CODING_TASK_MAX_SQL, statements
    _no_full_graph_scan(graph_db)


def test_sql_count_independent_of_anchor_count(settings, graph_db):
    """20 якорей (лимит) — столько же SQL, сколько 4: нет запросов на якорь и на узел."""
    anchors = [
        {"kind": "endpoint", "value": f"GET /api/v1/{RES[i % 4]}{i // 4}/{{id}}"} for i in range(16)
    ] + CODING_TASK["anchors"]
    pack, statements = _typed(settings, graph_db, {**CODING_TASK, "anchors": anchors})
    assert len(pack["anchors"]) == 20 and pack["unresolved"] == []
    _, few = _typed(settings, graph_db, CODING_TASK)
    assert len(statements) == len(few), (statements, few)

"""Шаг typed-обхода на таблицах графа: валидность рёбер до лимита чтения (ревью 367 п.1).

Рёбра фронта читаются по индексам концов с лимитом (``typed_neighbors``,
``max_edges``). Валидность на ``as_of``, ``evidence_lost`` и namespace ребра
проверяются в SQL до лимита: у хаба, накопившего за сверки закрытые версии рёбер с
меньшими id, действующие рёбра не вытесняются. Здесь же — псевдоним узла только в
графе (без журнала) и индексы концов рёбер (MEM-ADR-023).
"""

from __future__ import annotations

import pytest

from psycopg.types.json import Jsonb

from platform_memory.context.typed import compile_typed_context
from platform_memory.core.models import Node
from platform_memory.graph.store import GraphStore

pytestmark = pytest.mark.integration

NS = "tenant:t1:ws:w1"
OTHER = "tenant:t1:ws:other"
CLOSED = 30


def _node(graph: GraphStore, label: str, key: str, ns: str = NS) -> int:
    """Узел напрямую строкой таблицы (как его оставила бы прежняя запись); вернуть id."""
    with graph.conn.cursor() as cur:
        cur.execute(
            graph.sql(
                "INSERT INTO {N} (namespace, label, natural_key, type, props) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING id"
            ),
            (ns, label, key, label, Jsonb({"title": key})),
        )
        return int(cur.fetchone()[0])


def _fact(graph: GraphStore, src: int, dst: int, fid: str, ns: str = NS, **cols) -> None:
    """Ребро-факт PART_OF строкой таблицы: ``cols`` — valid_from/valid_to/evidence_lost."""
    cols = {"valid_from": "2026-01-01T00:00:00Z", **cols}
    names = ", ".join(cols)
    marks = ", ".join(["%s"] * len(cols))
    with graph.conn.cursor() as cur:
        cur.execute(
            graph.sql(
                "INSERT INTO {E} (namespace, type, src_id, dst_id, fact_id, observation_ids, "
                + names
                + ") VALUES (%s, 'PART_OF', %s, %s, %s, ARRAY[]::text[], "
                + marks
                + ")"
            ),
            (ns, src, dst, fid, *cols.values()),
        )


@pytest.fixture
def hub(stores):
    """Хаб с CLOSED закрытыми рёбрами (меньшие id) и тремя действующими после них.

    Ещё два ребра после действующих не проходятся: ``evidence_lost`` и ребро с
    namespace другой базы.
    """
    conn, graph, _, _, _ = stores
    hub_id = _node(graph, "component", "hub")
    for i in range(1, CLOSED + 1):
        _fact(
            graph,
            _node(graph, "source_file", f"old{i}"),
            hub_id,
            f"old{i}",
            valid_to="2026-02-01T00:00:00Z",
        )
    for i in range(1, 4):
        _fact(graph, _node(graph, "source_file", f"live{i}"), hub_id, f"live{i}")
    _fact(graph, _node(graph, "source_file", "lost1"), hub_id, "lost1", evidence_lost=True)
    _fact(graph, _node(graph, "source_file", "alien"), hub_id, "alien", ns=OTHER)
    return conn, graph


def _neighbors(graph: GraphStore, **kw):
    (node,) = graph.lookup_nodes({"component": ["hub"]}, namespaces=[NS])
    return graph.typed_neighbors(
        {node["_gid"]: NS}, "part_of", ["in"], labels=graph.graph_labels(), **kw
    )


def _keys(rows) -> list[str]:
    return sorted(r[2]["natural_key"] for r in rows)


def test_closed_edges_do_not_crowd_out_live_ones(hub):
    _, graph = hub
    rows, truncated = _neighbors(graph, max_edges=5)
    assert _keys(rows) == ["live1", "live2", "live3"]
    assert truncated is False


def test_as_of_interval_filtered_before_limit_and_truncation_reported(hub):
    _, graph = hub
    # На as_of внутри интервала закрытых — валидны и они, и действующие: 33 > 5.
    rows, truncated = _neighbors(graph, as_of="2026-01-15T00:00:00Z", max_edges=5)
    assert len(rows) == 5 and truncated is True
    rows, truncated = _neighbors(graph, as_of="2026-01-15T00:00:00Z", max_edges=100)
    assert len(rows) == CLOSED + 3 and truncated is False
    # До начала интервалов — ни одного ребра.
    rows, _ = _neighbors(graph, as_of="2025-12-31T00:00:00Z", max_edges=5)
    assert rows == []


def test_typed_context_step_reports_truncation(hub, settings, monkeypatch):
    conn, _ = hub
    real = GraphStore.typed_neighbors

    def small(self, *a, **kw):
        return real(self, *a, **{**kw, "max_edges": 5})

    monkeypatch.setattr(GraphStore, "typed_neighbors", small)
    payload = {
        "namespaces": [NS],
        "anchors": [{"value": "hub"}],
        "traverse": [{"relation": "part_of", "direction": "in", "limit": 50}],
    }
    pack = compile_typed_context(settings, payload, conn=conn)
    (step,) = pack["stats"]["steps"]
    assert step["reached"] == 3 and "truncated" not in step
    assert sorted(f["fact_id"] for f in pack["facts"]) == ["live1", "live2", "live3"]
    assert set(pack["stats"]["phases_ms"]) >= {"setup", "anchors", "traverse", "assemble"}

    pack = compile_typed_context(settings, {**payload, "as_of": "2026-01-15T00:00:00Z"}, conn=conn)
    (step,) = pack["stats"]["steps"]
    assert step["reached"] == 5 and step["truncated"] is True


def test_graph_only_alias_resolves_in_graph(stores, settings):
    """Псевдоним узла без журнала (props.aliases) — совпадение в графе (ревью 367 п.4)."""
    conn, graph, _, _, _ = stores
    for key, ns in (("note:payment-bug", NS), ("note:elsewhere", OTHER)):
        graph.upsert_node(
            Node(
                type="note",
                natural_key=key,
                title=key,
                properties={"aliases": ["payment-bug-alias"]},
                namespace=ns,
            )
        )
    for anchor in ({"value": "payment-bug-alias"}, {"kind": "note", "value": "payment-bug-alias"}):
        pack = compile_typed_context(settings, {"namespaces": [NS], "anchors": [anchor]}, conn=conn)
        (out,) = pack["anchors"]
        assert out["matchedBy"] == "alias", out
        assert [r["natural_key"] for r in out["resolved"]] == ["note:payment-bug"]


def test_edge_endpoints_are_indexed_by_type(stores):
    """Шаг обхода выбирает рёбра фронта по индексам концов, без просмотра таблицы рёбер."""
    conn, graph, _, _, _ = stores
    with conn.cursor() as cur:
        cur.execute(
            "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = %s AND tablename = %s",
            (graph.graph, "graph_edges"),
        )
        defs = dict(cur.fetchall())
    assert "(src_id, type)" in defs["graph_edges_src"]
    assert "(dst_id, type)" in defs["graph_edges_dst"]

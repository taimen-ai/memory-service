"""Перенос графа из Apache AGE в таблицы (MEM-ADR-023 п.5, ``cb migrate-graph-from-age``).

Нужна тестовая база с расширением AGE и суперпользователем (``LOAD 'age'``) — образ
``infra/memory-db``; без AGE тесты скипаются.

Эталон — граф, записанный методами GraphStore/FactStore и пайплайна наблюдений в таблицы
(граф ``<имя>_ref``). Граф AGE строится из его узлов и рёбер через ``cypher()`` с теми же
картами свойств, что писал прежний код на AGE (``node_json``/``edge_json`` дают ровно их:
служебные поля, вложенный ``props``, без пустых значений). Перенос этого графа AGE обязан
дать те же узлы, рёбра и тот же ответ Context API, что у эталона.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import psycopg
import pytest
from psycopg.adapt import Dumper
from typer.testing import CliRunner

from platform_memory import build_context, retain_observation
from platform_memory.context.typed import compile_typed_context
from platform_memory.core import db as dbmod
from platform_memory.core.models import Edge, Node, Provenance
from platform_memory.graph.age_import import AgeGraphNotFound, migrate_graph_from_age
from platform_memory.graph.facts import FactStore
from platform_memory.graph.store import GraphStore, edge_json, node_json
from tests.integration.conftest import _tables

pytestmark = pytest.mark.integration

A = "tenant:t1:ws:a"
B = "tenant:t1:ws:b"


# --- AGE: соединение с загруженным расширением и параметры agtype ---


class _Agtype:
    def __init__(self, value: Any):
        self.text = json.dumps(value, ensure_ascii=False)


def _age_connect(settings) -> psycopg.Connection:
    conn = psycopg.connect(settings.database_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT oid FROM pg_type WHERE typname = 'agtype'")
            row = cur.fetchone()
            if row is None:
                pytest.skip("в тестовой базе нет расширения AGE")
            cur.execute("LOAD 'age'")
            cur.execute('SET search_path = ag_catalog, "$user", public')
    except psycopg.Error as exc:
        conn.close()
        pytest.skip(f"AGE не загружается в сессию: {exc}")

    class _Dumper(Dumper):
        oid = int(row[0])

        def dump(self, obj: _Agtype) -> bytes:
            return obj.text.encode("utf-8")

    conn.adapters.register_dumper(_Agtype, _Dumper)
    return conn


def _cypher(conn: psycopg.Connection, graph: str, query: str, params: dict | None = None):
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM cypher('{graph}', $$ {query} $$, %s) AS (v agtype)",
            (_Agtype(params or {}),),
        )
        return [r[0] for r in cur.fetchall()]


def _sets(alias: str, props: dict[str, Any]) -> str:
    return ", ".join(f"{alias}.`{k}` = $p.`{k}`" for k in props)


def _create_vertex(conn, graph: str, label: str, props: dict[str, Any]) -> int:
    query = f"CREATE (n:{label})"
    if props:
        query += " SET " + _sets("n", props)
    (gid,) = _cypher(conn, graph, query + " RETURN id(n)", {"p": props})
    return int(gid)


def _create_edge(conn, graph: str, etype: str, src: int, dst: int, props: dict[str, Any]) -> int:
    query = f"MATCH (a), (b) WHERE id(a) = $s AND id(b) = $d CREATE (a)-[r:{etype}]->(b)"
    if props:
        query += " SET " + _sets("r", props)
    (gid,) = _cypher(conn, graph, query + " RETURN id(r)", {"s": src, "d": dst, "p": props})
    return int(gid)


@pytest.fixture()
def env(settings):
    """(эталон, граф AGE, AGE-соединение): эталон пуст, граф AGE создан; в конце — зачистка."""
    age = _age_connect(settings)
    ref = settings.model_copy_with(graph_name=f"{settings.graph_name}_ref")
    with age.cursor() as cur:
        cur.execute("SELECT create_graph(%s)", (settings.graph_name,))
    yield ref, settings, age
    try:
        conn = dbmod.connect(settings)
        try:
            dbmod.drop_graph(conn, ref.graph_name)
            with conn.cursor() as cur:
                for table in _tables(settings):
                    cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        finally:
            conn.close()
        _fix_catalog(age, settings.graph_name)
        with age.cursor() as cur:
            cur.execute("SELECT drop_graph(%s, true)", (settings.graph_name,))
    finally:
        age.close()


# --- эталонный набор ---


def _populate(settings) -> None:
    """Несколько namespaces и видов, факты с историей, структурные связи, временны́е поля."""
    retain_observation(
        settings,
        {
            "source": {"system": "tracker", "stream": "events", "external_id": "ev-1"},
            "kind": "work.completed",
            "occurred_at": "2026-08-01T10:00:00Z",
            "scopes": ["project:alpha"],
            "content": "Alice закончила деплой проекта Альфа",
            "assertions": [
                {"assert": "entity", "entity": {"type": "person", "id": "alice", "title": "Alice"}},
                {
                    "assert": "entity",
                    "entity": {"type": "project", "id": "alpha", "title": "Альфа"},
                },
                {
                    "assert": "fact",
                    "fact": {
                        "subject": "person:alice",
                        "predicate": "WORKS_ON",
                        "object": "project:alpha",
                        "valid_from": "2026-08-01T00:00:00Z",
                    },
                },
                {
                    "assert": "text",
                    "text": {"content": "Деплой прошёл без отката", "title": "Итог"},
                },
            ],
        },
        namespace=A,
    )
    conn = dbmod.connect(settings)
    try:
        graph = GraphStore(conn, settings.graph_name, settings.default_namespace)
        graph.ensure_schema()
        facts = FactStore(graph)

        def node(key: str, kind: str, ns: str, **props: Any) -> None:
            graph.upsert_node(
                Node(
                    type=kind,
                    natural_key=key,
                    title=f"Т {key}",
                    properties=props,
                    namespace=ns,
                    provenance=Provenance(
                        source_path=f"vault/{key}.md",
                        source_id="s1",
                        confidence=0.9,
                        last_seen="run-1",
                    ),
                )
            )

        node(
            "person:bob",
            "person",
            A,
            aliases=["Боб", "bob"],
            age=30,
            ratio=0.5,
            flag=True,
            nothing=None,
            nested={"a": [1, {"b": "c::vertex"}], "q": 'он сказал "да" \\ ::edge'},
        )
        node("project:beta", "project", A, status="active")
        node("person:bob", "contact", A)  # тот же ключ — другой вид
        node("person:bob", "person", B)  # тот же ключ — другая база
        node("project:gamma", "project", B)
        node("article:1", "article", A)
        node("article:2", "article", A)
        node("concept:deploy", "concept", A)
        node("doc:legacy", "note", "nexus", body="старый документ")
        for src in ("article:1", "article:2"):
            graph.upsert_edge(
                Edge(
                    type="COVERS",
                    src=src,
                    dst="concept:deploy",
                    namespace=A,
                    source_path=f"vault/{src}.md",
                    properties={"w": 1},
                ),
                "run-1",
            )
        graph.upsert_edge(
            Edge(type="LINKS_TO", src="project:alpha", dst="project:beta", namespace=A), "run-1"
        )
        graph.upsert_edge(
            Edge(type="LINKS_TO", src="person:bob", dst="project:gamma", namespace=B), "run-1"
        )
        graph.upsert_entities(
            "team",
            [
                {"k": "team:core", "t": "Core", "sp": "snap", "props": {"size": 5}},
                {"k": "team:old", "t": "Old", "sp": "snap"},
            ],
            A,
            valid_from="2026-01-01T00:00:00Z",
            last_seen="2026-02-01T00:00:00Z",
        )
        graph.close_entities("team", ["team:old"], A, valid_to="2026-03-01T00:00:00Z")
        f1 = facts.assert_fact(
            "person:bob",
            "WORKS_ON",
            "project:beta",
            namespace=A,
            valid_from="2026-01-01T00:00:00Z",
            observation_id="obs-1",
            scopes=["project:beta"],
        )["fact_id"]
        facts.assert_fact(
            "person:bob",
            "WORKS_ON",
            "project:beta",
            namespace=A,
            valid_from="2026-01-01T00:00:00Z",
            observation_id="obs-2",
            confidence=0.7,
        )
        facts.assert_fact(
            "person:bob",
            "WORKS_ON",
            "project:alpha",
            namespace=A,
            valid_from="2026-04-01T00:00:00Z",
            supersedes=f1,
            observation_id="obs-3",
        )
        facts.assert_fact(
            "person:bob", "MEMBER_OF", "team:core", namespace=A, observation_id="obs-4"
        )
        facts.mark_evidence_lost("obs-4", A)
        f4 = facts.assert_fact("project:alpha", "OWNED_BY", "person:bob", namespace=A)["fact_id"]
        facts.invalidate_fact(
            f4, namespace=A, valid_to="2026-05-01T00:00:00Z", observation_id="obs-5"
        )
        facts.assert_fact("person:bob", "WORKS_ON", "project:gamma", namespace=B)
        facts.create_facts(
            "MEMBER_OF",
            "person",
            "team",
            [
                {
                    "fid": "fact-snap-1",
                    "s": "person:bob",
                    "d": "team:core",
                    "vf": "2026-01-01T00:00:00Z",
                    "sp": "snap",
                    "a": {"role": "lead"},
                    "src": "hr",
                    "scope": "all",
                    "sid": "snap-1",
                    "scopes": None,
                }
            ],
            namespace=A,
            observed_at="2026-01-02T00:00:00Z",
        )
        graph.record_audit(
            "trace-1",
            "agent-x",
            "did.thing",
            payload={"k": 1},
            actor_id="agent-x",
            ts="2026-08-02T00:00:00",
            namespace=A,
        )
        graph.write_fact(
            "fact:agent",
            "note",
            "Факт агента",
            properties={"x": 1},
            links=["project:alpha"],
            trace_id="trace-1",
            namespace=A,
        )
        graph.set_node_communities(
            {"article:1": 1, "article:2": 1}, {1: 0.5}, {1: "deploy"}, namespace=A
        )
    finally:
        conn.close()


def _build_age_from(ref, age_settings, age: psycopg.Connection) -> dict[int, int]:
    """Граф AGE с теми же картами свойств, что у эталона (как их писал прежний код).

    Вернуть id ребра эталона -> graphid ребра AGE.
    """
    conn = dbmod.connect(ref)
    try:
        g = GraphStore(conn, ref.graph_name, ref.default_namespace)
        nodes = g._all("SELECT n.id, n.label, " + node_json("n") + " FROM {N} n ORDER BY n.id")
        edges = g._all(
            "SELECT e.id, e.type, e.src_id, e.dst_id, " + edge_json("e") + " FROM {E} e "
            "ORDER BY e.id"
        )
    finally:
        conn.close()
    ids = {
        nid: _create_vertex(age, age_settings.graph_name, label, props)
        for nid, label, props in nodes
    }
    return {
        eid: _create_edge(age, age_settings.graph_name, etype, ids[src], ids[dst], props)
        for eid, etype, src, dst, props in edges
    }


# --- снимки для сравнения ---


def _graph_dump(settings) -> tuple[dict, list]:
    """Узлы (ключ -> свойства) и рёбра (отсортированный список) графа на таблицах."""
    conn = dbmod.connect(settings)
    try:
        g = GraphStore(conn, settings.graph_name, settings.default_namespace)
        nodes = {
            (ns, label, nk): props
            for ns, label, nk, props in g._all(
                "SELECT n.namespace, n.label, n.natural_key, " + node_json("n") + " FROM {N} n"
            )
        }
        edges = sorted(
            (json.dumps(row, sort_keys=True, ensure_ascii=False),)
            for row in g._all(
                "SELECT e.namespace, e.type, a.namespace, a.label, a.natural_key, b.namespace, "
                "b.label, b.natural_key, " + edge_json("e") + " FROM {E} e "
                "JOIN {N} a ON a.id = e.src_id JOIN {N} b ON b.id = e.dst_id"
            )
        )
    finally:
        conn.close()
    return nodes, edges


def _age_fingerprint(settings) -> tuple[int, str]:
    """Число строк и хэш всех строк таблиц меток AGE — проверка «данные AGE не тронуты»."""
    conn = dbmod.connect(settings)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT id::text, properties::text FROM "{settings.graph_name}"._ag_label_vertex '
                f"UNION ALL SELECT id::text || start_id::text || end_id::text, properties::text "
                f'FROM "{settings.graph_name}"._ag_label_edge ORDER BY 1'
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    return len(rows), hashlib.sha256(repr(rows).encode()).hexdigest()


_VOLATILE = {"trace_id", "latencies", "latencies_ms", "phases_ms", "latency_ms", "elapsed_ms"}
# Порядок строк: перенос выдаёт id в порядке graphid AGE (метка за меткой — так AGE и
# сканировал граф), эталон — в порядке записи. От порядка зависит только ранг элемента
# внутри канала (``signals.channels``) и итоговый RRF-``score``; состав и содержание
# ответа от него не зависят и сравниваются целиком.
_ORDER_DEPENDENT = {"score", "channels"}


def _normalize(value: Any) -> Any:
    """Ответ без идентификаторов прогона, замеров и рангов по порядку строк."""
    if isinstance(value, dict):
        return {
            k: _normalize(v)
            for k, v in value.items()
            if k not in _VOLATILE and k not in _ORDER_DEPENDENT
        }
    if isinstance(value, list):
        items = [_normalize(v) for v in value]
        if items and all(isinstance(i, dict) for i in items):
            return sorted(items, key=lambda i: json.dumps(i, sort_keys=True, ensure_ascii=False))
        return items
    return value


_CONTEXT_REQUESTS = [
    {
        "query": "деплой Альфа",
        "anchors": ["person:alice"],
        "namespaces": [A],
        "budget": {"tokens": 4000},
    },
    {
        "query": "кто работает над проектом",
        "anchors": ["person:bob"],
        "namespaces": [A, B],
        "as_of": "2026-02-01T00:00:00Z",
        "budget": {"tokens": 4000},
    },
    {"query": "bob", "anchors": ["person:bob"], "namespaces": [A], "scopes": ["project:beta"]},
]
_TYPED_REQUESTS = [
    {
        "namespaces": [A],
        "anchors": [{"value": "person:bob"}],
        "traverse": [{"relation": "WORKS_ON", "direction": "out", "depth": 1}],
    },
    {
        "namespaces": [A],
        "anchors": [{"value": "person:bob"}],
        "as_of": "2026-02-01T00:00:00Z",
        "traverse": [
            {"relation": "WORKS_ON", "direction": "out"},
            {"relation": "LINKS_TO", "direction": "both", "from": "previous"},
        ],
    },
    {
        "namespaces": [A, B],
        "anchors": [{"value": "concept:deploy"}],
        "traverse": [{"relation": "COVERS", "direction": "in", "depth": 1}],
    },
]


def _context_answers(settings, edge_ids: dict[int, int] | None = None) -> list[Any]:
    """Ответы Context API на набор запросов; ``edge_ids`` — замена ссылок ``edge:<id>``."""
    raw = [build_context(settings, req).to_payload() for req in _CONTEXT_REQUESTS]
    raw += [compile_typed_context(settings, req) for req in _TYPED_REQUESTS]
    if edge_ids is not None:
        raw = [_remap_edge_refs(answer, edge_ids) for answer in raw]
    return [_normalize(answer) for answer in raw]


# --- тесты ---


def test_age_graph_moves_to_tables_exactly_and_idempotently(env):
    ref, target, age = env
    _populate(ref)
    edge_ids = _build_age_from(ref, target, age)
    before_age = _age_fingerprint(target)
    ref_nodes, ref_edges = _graph_dump(ref)

    report = migrate_graph_from_age(target, batch_size=7)  # несколько пачек

    assert report.ok, report
    assert report.catalog_warning == ""
    assert not report.anomalies
    assert report.nodes_inserted == len(ref_nodes)
    assert report.edges_inserted == len(ref_edges)
    assert set(report.namespaces) == {A, B, "nexus"}
    for counts in report.namespaces.values():
        assert counts.table_nodes == counts.age_nodes and counts.table_edges == counts.age_edges
    nodes, edges = _graph_dump(target)
    assert nodes == ref_nodes
    assert edges == ref_edges
    # id строк — graphid AGE; новые строки сервиса идут после перенесённых.
    conn = dbmod.connect(target)
    try:
        g = GraphStore(conn, target.graph_name, target.default_namespace)
        assert {r[0] for r in g._all("SELECT id FROM {E}")} == set(edge_ids.values())
        assert min(r[0] for r in g._all("SELECT id FROM {N}")) >= 2**48
        g.upsert_node(Node(type="note", natural_key="after", title="after", namespace=A))
        g.upsert_edge(Edge(type="LINKS_TO", src="after", dst="project:beta", namespace=A))
        newest = g._all("SELECT max(id) FROM {N}")[0][0]
        assert g.get_node("after", [A]) is not None
        assert newest == g._all("SELECT id FROM {N} WHERE natural_key = 'after'")[0][0]
        assert g._run("DELETE FROM {N} WHERE natural_key = 'after'") == 1
    finally:
        conn.close()

    # Повтор ничего не дублирует.
    again = migrate_graph_from_age(target)
    assert again.ok and again.nodes_inserted == 0 and again.edges_inserted == 0
    assert _graph_dump(target) == (ref_nodes, ref_edges)

    # Недостающее доносится: узел (вместе со своими рёбрами) и отдельный факт.
    conn = dbmod.connect(target)
    try:
        g = GraphStore(conn, target.graph_name, target.default_namespace)
        lost_edges = g._run(
            "DELETE FROM {E} WHERE src_id IN (SELECT id FROM {N} WHERE natural_key = 'article:1') "
            "OR dst_id IN (SELECT id FROM {N} WHERE natural_key = 'article:1')"
        )
        lost_edges += g._run(
            "DELETE FROM {E} WHERE namespace = %s AND fact_id IS NOT NULL AND valid_to IS NOT NULL",
            (A,),
        )
        assert g._run("DELETE FROM {N} WHERE natural_key = 'article:1'") == 1
    finally:
        conn.close()
    assert not migrate_graph_from_age(target, dry_run=True).mismatched()
    partial = migrate_graph_from_age(target)
    assert partial.ok and partial.nodes_inserted == 1 and partial.edges_inserted == lost_edges
    assert _graph_dump(target) == (ref_nodes, ref_edges)

    # Данные AGE не тронуты.
    assert _age_fingerprint(target) == before_age


def test_context_api_answers_the_same_after_migration(env):
    ref, target, age = env
    _populate(ref)
    edge_ids = _build_age_from(ref, target, age)
    assert migrate_graph_from_age(target).ok
    # Ответ на AGE ссылался на структурные рёбра по graphid (``edge:<graphid>``): в
    # эталоне на таблицах на их месте id строк эталона.
    expected = _context_answers(ref, edge_ids)
    got = _context_answers(target)
    # Набор не пустой: якоря разрешились, факты и связи нашлись.
    for pack in expected[: len(_CONTEXT_REQUESTS)]:
        assert any(section["items"] for section in pack["sections"] if section["kind"] != "current")
    for pack in expected[len(_CONTEXT_REQUESTS) :]:
        assert pack["used"]["entities"] and not pack["unresolved"], pack
    typed_facts = [f for pack in expected[len(_CONTEXT_REQUESTS) :] for f in pack["used"]["facts"]]
    assert any(f.startswith("edge:") for f in typed_facts)
    assert any(f.startswith("fact-") for f in typed_facts)
    assert got == expected


def _remap_edge_refs(value: Any, edge_ids: dict[int, int]) -> Any:
    if isinstance(value, dict):
        return {k: _remap_edge_refs(v, edge_ids) for k, v in value.items()}
    if isinstance(value, list):
        return [_remap_edge_refs(v, edge_ids) for v in value]
    if isinstance(value, str) and value.startswith("edge:") and value[5:].isdigit():
        return f"edge:{edge_ids[int(value[5:])]}"
    return value


def test_report_lists_what_cannot_be_moved(env):
    """Длинный ключ, ребро к нему, дубли, узел без namespace — в отчёте; сверка не сходится."""
    _, target, age = env
    g = target.graph_name
    long_key = "k:" + hashlib.sha256(b"x").hexdigest() * 70  # ~4,5 КБ, плохо сжимается
    a = _create_vertex(age, g, "note", {"natural_key": "a", "namespace": A, "type": "note"})
    long_v = _create_vertex(age, g, "note", {"natural_key": long_key, "namespace": A})
    _create_vertex(age, g, "note", {"natural_key": "a", "namespace": A, "title": "дубль"})
    legacy = _create_vertex(age, g, "note", {"natural_key": "legacy", "type": "note"})
    _create_edge(age, g, "LINKS_TO", a, long_v, {"origin": "vault"})
    _create_edge(age, g, "LINKS_TO", a, legacy, {})
    _create_edge(age, g, "LINKS_TO", a, legacy, {})  # дубль структурной связи
    # Факт прежнего кода на ключе с узлами двух видов: два ребра с одним fact_id; в
    # таблицах остаётся ребро между вершинами с наименьшими id (как у assert_fact).
    b_contact = _create_vertex(age, g, "contact", {"natural_key": "b", "namespace": A})
    b_person = _create_vertex(age, g, "person", {"natural_key": "b", "namespace": A})
    fact = {"fact_id": "fact-x", "namespace": A, "valid_from": "", "observation_ids": ["o1"]}
    _create_edge(age, g, "KNOWS", a, b_person, fact)
    _create_edge(age, g, "KNOWS", a, b_contact, fact)
    assert b_contact < b_person

    dry = migrate_graph_from_age(target, dry_run=True)
    assert dry.mismatched() == [A]
    conn = dbmod.connect(target)
    try:
        assert not dbmod.graph_exists(conn, g)  # dry-run ничего не создаёт
    finally:
        conn.close()

    report = migrate_graph_from_age(target)
    kinds = {k: len(v) for k, v in report.by_kind().items()}
    assert kinds == {
        "natural_key_too_long": 1,
        "endpoint_skipped": 1,
        "duplicate_node": 1,
        "duplicate_edge": 2,
        "namespace_backfilled": 1,
    }
    assert report.mismatched() == [A]
    ns_a = report.namespaces[A]
    assert (ns_a.age_nodes, ns_a.dup_nodes, ns_a.skipped_nodes, ns_a.table_nodes) == (5, 1, 1, 3)
    assert (ns_a.age_edges, ns_a.dup_edges, ns_a.skipped_edges, ns_a.table_edges) == (5, 2, 1, 2)
    assert report.namespaces["nexus"].ok  # узел без namespace — в namespace по умолчанию
    (too_long,) = report.by_kind()["natural_key_too_long"]
    assert too_long.detail.startswith("natural_key_too_long:")
    nodes, _ = _graph_dump(target)
    assert not any(nk.startswith("k:") for _, _, nk in nodes)  # не обрезан и не записан
    assert nodes[(A, "note", "a")]["type"] == "note"  # из дублей — первый по id AGE
    conn = dbmod.connect(target)
    try:
        g_store = GraphStore(conn, g, target.default_namespace)
        ((label, eid),) = g_store._all(
            "SELECT b.label, e.dst_id FROM {E} e JOIN {N} b ON b.id = e.dst_id "
            "WHERE e.fact_id = 'fact-x'"
        )
        assert (label, eid) == ("contact", b_contact)
        (fact_row,) = FactStore(g_store).facts(subject="a", predicate="KNOWS", namespaces=[A])
        assert fact_row["observation_ids"] == ["o1"] and fact_row["object"] == "b"
    finally:
        conn.close()

    # CLI: отчёт и код выхода 1.
    result = _invoke_cli(target)
    assert result.exit_code == 1, result.output
    assert "natural_key_too_long: 1" in result.output and A in result.output


def test_stale_age_catalog_after_restore_does_not_block(env):
    """OID каталога AGE от «старого кластера» (как после pg_dump): перенос идёт, отчёт говорит."""
    _, target, age = env
    g = target.graph_name
    a = _create_vertex(age, g, "note", {"natural_key": "a", "namespace": A})
    b = _create_vertex(age, g, "person", {"natural_key": "b", "namespace": A})
    _create_edge(age, g, "KNOWS", a, b, {"fact_id": "f1", "namespace": A, "valid_from": ""})
    with age.cursor() as cur:
        cur.execute("SELECT graphid FROM ag_graph WHERE name = %s", (g,))
        (graphid,) = cur.fetchone()
        stale = 4_000_000_000  # OID, которого в кластере нет
        cur.execute("ALTER TABLE ag_label DROP CONSTRAINT fk_graph_oid")
        cur.execute("UPDATE ag_label SET graph = %s WHERE graph = %s", (stale, graphid))
        cur.execute("UPDATE ag_graph SET graphid = %s WHERE name = %s", (stale, g))
        cur.execute(
            "ALTER TABLE ag_label ADD CONSTRAINT fk_graph_oid FOREIGN KEY (graph) "
            "REFERENCES ag_graph (graphid)"
        )
    # Новая сессия (кэш каталога AGE — на сессию): cypher() по битому каталогу падает.
    fresh = _age_connect(target)
    try:
        with pytest.raises(psycopg.Error):
            _cypher(fresh, g, "MATCH (n) RETURN n")
    finally:
        fresh.close()

    report = migrate_graph_from_age(target)
    assert report.ok, report
    assert "OID другого кластера" in report.catalog_warning
    nodes, edges = _graph_dump(target)
    assert set(nodes) == {(A, "note", "a"), (A, "person", "b")}
    assert len(edges) == 1 and '"fact_id": "f1"' in edges[0][0]

    result = _invoke_cli(target)
    assert result.exit_code == 0, result.output
    assert "Каталог AGE" in result.output


def test_no_age_graph_is_a_clear_error(settings):
    with pytest.raises(AgeGraphNotFound):
        migrate_graph_from_age(settings)
    result = _invoke_cli(settings)
    assert result.exit_code == 2


def _invoke_cli(settings):
    import platform_memory.cli.main as cli

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(cli, "get_settings", lambda: settings)
        return CliRunner().invoke(cli.app, ["migrate-graph-from-age"])


def _fix_catalog(age: psycopg.Connection, graph: str) -> None:
    """Вернуть каталогу AGE OID схемы графа (иначе drop_graph его не найдёт)."""
    with age.cursor() as cur:
        cur.execute("SELECT graphid, namespace::oid FROM ag_graph WHERE name = %s", (graph,))
        row = cur.fetchone()
        if row is None or row[0] == row[1]:
            return
        cur.execute("ALTER TABLE ag_label DROP CONSTRAINT fk_graph_oid")
        cur.execute("UPDATE ag_label SET graph = %s WHERE graph = %s", (row[1], row[0]))
        cur.execute("UPDATE ag_graph SET graphid = namespace::oid WHERE name = %s", (graph,))
        cur.execute(
            "ALTER TABLE ag_label ADD CONSTRAINT fk_graph_oid FOREIGN KEY (graph) "
            "REFERENCES ag_graph (graphid)"
        )

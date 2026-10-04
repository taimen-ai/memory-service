"""Граф на таблицах PostgreSQL (MEM-ADR-023): схема, уникальность, каскад, обход,
слияние дублей, purge по наблюдению, темпоральные факты, обычная роль и соседство с
таблицами меток прежнего графа AGE в той же схеме.
"""

from __future__ import annotations

import json
import threading
import uuid

import psycopg
import pytest

from platform_memory.core import db as dbmod
from platform_memory.core.models import Edge, Node
from platform_memory.core.scopes import ForeignObjectError
from platform_memory.graph.facts import FactStore
from platform_memory.graph.store import GraphStore

pytestmark = pytest.mark.integration

NS = "tenant:t1:ws:w1"
OTHER = "tenant:t1:ws:other"


def _node(graph: GraphStore, key: str, kind: str = "entity", ns: str = NS, **props) -> None:
    graph.upsert_node(Node(type=kind, natural_key=key, title=key, properties=props, namespace=ns))


def _link(graph: GraphStore, src: str, dst: str, kind: str = "LINKS_TO", ns: str = NS) -> bool:
    return graph.upsert_edge(Edge(type=kind, src=src, dst=dst, namespace=ns), "run-1")


def _count(graph: GraphStore, table: str, where: str = "TRUE", params=()) -> int:
    with graph.conn.cursor() as cur:
        cur.execute(graph.sql("SELECT count(*) FROM {" + table + "} WHERE " + where), params)
        return int(cur.fetchone()[0])


# --- схема ---


def test_schema_is_two_tables_in_graph_schema(stores):
    conn, graph, _, _, _ = stores
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relkind = 'r'",
            (graph.graph,),
        )
        assert {r[0] for r in cur.fetchall()} == {"graph_nodes", "graph_edges"}
    assert dbmod.graph_exists(conn, graph.graph)
    graph.ensure_schema()  # повтор — без ошибок и без изменений
    assert dbmod.graph_exists(conn, graph.graph)


def test_node_dict_keeps_previous_shape(stores):
    """Свойства узла — прежняя карта: служебные поля, вложенный props, без пустых полей."""
    _, graph, _, _, _ = stores
    _node(graph, "doc:1", "note", status="draft")
    node = graph.get_node("doc:1", [NS])
    assert node is not None
    assert node["natural_key"] == "doc:1" and node["namespace"] == NS and node["type"] == "note"
    assert node["title"] == "doc:1" and node["origin"] == "vault"
    assert node["props"] == {"status": "draft"}
    assert "valid_to" not in node and "source_id" not in node  # NULL — свойства нет


# --- уникальность ---


def test_node_unique_by_namespace_label_key(stores):
    _, graph, _, _, _ = stores
    _node(graph, "k", "note", v=1)
    _node(graph, "k", "note", v=2)  # повтор — обновление, не второй узел
    _node(graph, "k", "person")  # другой вид — свой узел того же ключа
    _node(graph, "k", "note", ns=OTHER)  # другой namespace — свой узел
    assert _count(graph, "N", "natural_key = 'k'") == 3
    assert {n["type"] for n in graph.nodes_by_keys(["k"], [NS])} == {"note", "person"}
    assert next(n for n in graph.nodes_by_keys(["k"], [NS]) if n["type"] == "note")["props"] == {
        "v": 2
    }


def test_upsert_keeps_properties_it_does_not_write(stores):
    """Перезапись узла не стирает разметку сообществ; None удаляет только своё свойство."""
    _, graph, _, _, _ = stores
    _node(graph, "a")
    assert graph.set_node_communities({"a": 7}, names={7: "Тема"}, namespace=NS) == 1
    _node(graph, "a", extra=True)
    node = graph.get_node("a", [NS])
    assert node["community"] == 7 and node["community_name"] == "Тема"
    assert "cohesion" not in node
    assert [n["natural_key"] for n in graph.community_members(7, namespaces=[NS])] == ["a"]


def test_structural_edge_unique_and_fact_edges_coexist(stores):
    _, graph, facts, _, _ = stores
    _node(graph, "a")
    _node(graph, "b")
    assert _link(graph, "a", "b") and _link(graph, "a", "b")
    assert _count(graph, "E", "fact_id IS NULL") == 1
    # Факты одного типа между той же парой сосуществуют, различаясь интервалами.
    facts.assert_fact("a", "links_to", "b", namespace=NS, valid_from="2026-01-01T00:00:00Z")
    facts.assert_fact("a", "links_to", "b", namespace=NS, valid_from="2026-02-01T00:00:00Z")
    assert _count(graph, "E") == 3
    assert not _link(graph, "a", "nope")  # нет конца — нет ребра


def test_fact_unique_per_namespace(stores):
    conn, graph, facts, _, _ = stores
    _node(graph, "a")
    _node(graph, "b")
    first = facts.assert_fact("a", "owns", "b", namespace=NS, observation_id="o1")
    again = facts.assert_fact("a", "owns", "b", namespace=NS, observation_id="o2")
    assert first["fact_id"] == again["fact_id"] and again["reinforced"] is True
    assert _count(graph, "E", "fact_id IS NOT NULL") == 1
    assert facts.get_fact(first["fact_id"], [NS])["observation_ids"] == ["o1", "o2"]
    with pytest.raises(psycopg.errors.UniqueViolation), conn.cursor() as cur:
        cur.execute(
            graph.sql(
                "INSERT INTO {E} (namespace, type, src_id, dst_id, fact_id) "
                "SELECT namespace, type, src_id, dst_id, fact_id FROM {E} WHERE fact_id = %s"
            ),
            (first["fact_id"],),
        )


def test_create_facts_does_not_overwrite_existing_fact_id(stores):
    _, graph, facts, _, _ = stores
    for key in ("s", "d"):
        graph.upsert_entities(
            "component",
            [{"k": key, "t": key, "sp": "x", "props": {}}],
            NS,
            valid_from="",
            last_seen="",
        )
    row = {
        "fid": "f1",
        "s": "s",
        "d": "d",
        "vf": "2026-01-01T00:00:00Z",
        "sp": "x",
        "a": {"n": None},
    }
    assert facts.create_facts(
        "calls", "component", "component", [row], namespace=NS, observed_at=""
    ) == {"f1"}
    assert (
        facts.create_facts("calls", "component", "component", [row], namespace=NS, observed_at="")
        == set()
    )
    fact = facts.get_fact("f1", [NS])
    # Вложенный null в атрибутах связи сохраняется как есть (не вычищается).
    assert fact["attributes"] == {"n": None} and fact["observation_ids"] == []


# --- каскад ---


def test_delete_node_cascades_edges_both_directions(stores):
    _, graph, facts, _, _ = stores
    for key in ("a", "b", "c"):
        _node(graph, key)
    _link(graph, "a", "b")
    _link(graph, "c", "a")
    fid = facts.assert_fact("b", "owns", "a", namespace=NS)["fact_id"]
    assert _count(graph, "E") == 3
    assert graph.delete_node("a", NS) is not None
    assert _count(graph, "E") == 0
    assert facts.get_fact(fid, [NS]) is None
    assert graph.get_node("b", [NS]) is not None  # концы-соседи живы


def test_prune_removes_stale_vault_nodes_with_their_edges(stores):
    _, graph, _, _, _ = stores
    from platform_memory.core.models import Provenance

    def vault(key: str, run: str) -> None:
        graph.upsert_node(
            Node(
                type="note",
                natural_key=key,
                title=key,
                namespace=NS,
                provenance=Provenance(source_path=f"{key}.md", last_seen=run),
            )
        )

    vault("old", "run-0")
    vault("fresh", "run-1")
    graph.write_fact("agent:fact", "note", "Факт агента", links=["old"], namespace=NS)
    graph.upsert_edge(Edge(type="LINKS_TO", src="fresh", dst="old", namespace=NS), "run-1")
    nodes, edges = graph.prune("run-1", namespace=NS)
    assert (nodes, edges) == (1, 0)  # ребро fresh->old видели в прогоне, агентское — не vault
    assert graph.get_node("old", [NS]) is None and graph.get_node("agent:fact", [NS])
    assert _count(graph, "E") == 0  # рёбра к удалённому узлу ушли каскадом


# --- обход ---


@pytest.fixture
def chain(stores):
    """a - b - c - d и ветка b - x; в чужом namespace — узел y, ребро к нему из a."""
    conn, graph, _, _, _ = stores
    for key in ("a", "b", "c", "d", "x"):
        _node(graph, key)
    _node(graph, "y", ns=OTHER)
    _link(graph, "a", "b")
    _link(graph, "c", "b")  # обход идёт по рёбрам в обе стороны
    _link(graph, "c", "d")
    _link(graph, "b", "x")
    with conn.cursor() as cur:  # ребро через границу namespace (API такого не создаёт)
        cur.execute(
            graph.sql(
                "INSERT INTO {E} (namespace, type, src_id, dst_id) SELECT %s, 'LINKS_TO', a.id, y.id "
                "FROM {N} a, {N} y WHERE a.natural_key = 'a' AND y.natural_key = 'y'"
            ),
            (NS,),
        )
    return graph


def _keys(nodes) -> set[str]:
    return {n["natural_key"] for n in nodes}


def test_expand_one_and_two_hops(chain):
    assert _keys(chain.expand_neighbors(["a"], hops=1, namespaces=[NS])) == {"b"}
    assert _keys(chain.expand_neighbors(["a"], hops=2, namespaces=[NS])) == {"b", "c", "x"}
    assert _keys(chain.expand_neighbors(["a"], hops=5, namespaces=[NS])) == {"b", "c", "x"}
    # Чужой namespace не выдаётся, даже если ребро к нему есть.
    assert _keys(chain.expand_neighbors(["a"], hops=1, namespaces=[NS, OTHER])) == {"b", "y"}
    # Стартовый узел, достигнутый от другого старта, выдаётся; от самого себя — нет.
    assert _keys(chain.expand_neighbors(["a", "c"], hops=2, namespaces=[NS])) == {
        "a",
        "b",
        "c",
        "d",
        "x",
    }
    assert chain.expand_neighbors(["a"], hops=2, limit=1, namespaces=[NS])[0]["natural_key"] == "b"
    assert "_gid" not in chain.expand_neighbors(["a"], namespaces=[NS])[0]


def test_neighborhood_subgraph_hops_and_edges(chain):
    sub = chain.neighborhood_subgraph(["a"], namespaces=[NS], hops=2)
    hops = {n["natural_key"]: n["hop"] for n in sub["nodes"]}
    assert hops == {"a": 0, "b": 1, "c": 2, "x": 2}
    assert {(e["src"], e["dst"]) for e in sub["edges"]} == {("a", "b"), ("c", "b"), ("b", "x")}


def test_typed_neighbors_by_ids(stores):
    _, graph, facts, _, _ = stores
    for key in ("svc", "f1", "f2"):
        graph.upsert_entities(
            "component",
            [{"k": key, "t": key, "sp": "", "props": {}}],
            NS,
            valid_from="",
            last_seen="",
        )
    facts.assert_fact("f1", "part_of", "svc", namespace=NS)
    closed = facts.assert_fact(
        "f2", "part_of", "svc", namespace=NS, valid_from="2026-01-01T00:00:00Z"
    )
    facts.invalidate_fact(closed["fact_id"], namespace=NS, valid_to="2026-02-01T00:00:00Z")
    (svc,) = graph.lookup_nodes({"component": ["svc"]}, namespaces=[NS])
    labels = graph.graph_labels()
    assert ("component", "v") in labels.values() and ("PART_OF", "e") in labels.values()
    rows, cut = graph.typed_neighbors({svc["_gid"]: NS}, "part_of", ["in"], labels=labels)
    assert [r[2]["natural_key"] for r in rows] == ["f1"] and cut is False
    rows, _ = graph.typed_neighbors(
        {svc["_gid"]: NS}, "part_of", ["in"], labels=labels, as_of="2026-01-15T00:00:00Z"
    )
    assert sorted(r[2]["natural_key"] for r in rows) == ["f1", "f2"]
    rows, _ = graph.typed_neighbors(
        {svc["_gid"]: NS}, "part_of", ["in"], labels=labels, as_of="2026-02-15T00:00:00Z"
    )
    assert [r[2]["natural_key"] for r in rows] == ["f1"]
    assert graph.typed_neighbors({svc["_gid"]: NS}, "nope", ["in"], labels=labels) == ([], False)


# --- слияние дублей ---


def test_merge_nodes_moves_edges_and_drops_duplicate(stores):
    _, graph, _, _, _ = stores
    for key in ("acme", "acme-dup", "p", "q"):
        _node(graph, key)
    _link(graph, "acme-dup", "p")
    _link(graph, "q", "acme-dup")
    _link(graph, "acme", "p")  # уже есть у выжившего — не дублируется
    _link(graph, "acme-dup", "acme")  # связь с выжившим — петлёй не становится
    out = graph.merge_nodes("acme", "acme-dup", NS)
    assert out == {"ok": True, "survivor": "acme", "duplicate": "acme-dup", "rewired": 2}
    assert graph.get_node("acme-dup", [NS]) is None
    edges = {(e["src"], e["dst"]) for e in graph.all_edges(namespaces=[NS])}
    assert edges == {("acme", "p"), ("q", "acme")}
    assert graph.merge_nodes("acme", "acme-dup", NS)["reason"] == "duplicate_missing"
    assert graph.merge_nodes("acme", "acme", NS)["reason"] == "same_node"


# --- redact / purge по наблюдению (ADR-001/002, ADR-016 §8) ---


def test_scrub_and_purge_by_observation_id(stores):
    _, graph, facts, _, _ = stores
    _node(graph, "obs:1:a", "observation_content", observation_id="obs-1", content="секрет")
    _node(graph, "obs:1:b", "observation_content", observation_id="obs-1", content="секрет")
    _node(graph, "obs:2", "observation_content", observation_id="obs-2", content="другое")
    _node(graph, "obs:1:x", "observation_content", ns=OTHER, observation_id="obs-1", content="x")
    _link(graph, "obs:1:a", "obs:2")
    assert facts.scrub_derived_nodes("obs-1", NS) == 2
    assert graph.get_node("obs:1:a", [NS])["props"] == {"observation_id": "obs-1", "redacted": True}
    assert graph.get_node("obs:1:x", [OTHER])["props"]["content"] == "x"  # чужой namespace
    assert facts.scrub_derived_nodes("obs-1", NS, purge=True) == 2
    assert graph.get_node("obs:1:a", [NS]) is None and _count(graph, "E") == 0
    assert graph.get_node("obs:2", [NS])["props"]["content"] == "другое"


def test_mark_evidence_lost_only_when_last_observation_goes(stores):
    _, graph, facts, _, _ = stores
    _node(graph, "a")
    _node(graph, "b")
    _node(graph, "c")
    shared = facts.assert_fact("a", "owns", "b", namespace=NS, observation_id="o1")["fact_id"]
    facts.assert_fact("a", "owns", "b", namespace=NS, observation_id="o2")
    single = facts.assert_fact("a", "owns", "c", namespace=NS, observation_id="o1")["fact_id"]
    assert [f["fact_id"] for f in facts.facts_for_observation("o1", [NS])] == [shared, single]
    assert facts.mark_evidence_lost("o1", NS) == 1
    assert facts.get_fact(shared, [NS])["observation_ids"] == ["o2"]
    assert facts.get_fact(single, [NS])["evidence_lost"] is True
    assert "evidence_lost" not in facts.get_fact(shared, [NS])
    assert [f["fact_id"] for f in facts.facts(subject="a", namespaces=[NS])] == [shared]
    assert len(facts.facts(subject="a", namespaces=[NS], include_evidence_lost=True)) == 2


# --- темпоральные факты (ADR-016 §4) ---


def test_temporal_facts_supersede_and_as_of(stores):
    _, graph, facts, _, _ = stores
    for key in ("alice", "x", "y"):
        _node(graph, key)
    old = facts.assert_fact(
        "alice", "works_on", "x", namespace=NS, valid_from="2026-01-01T03:00:00+03:00"
    )["fact_id"]
    new = facts.assert_fact(
        "alice", "works_on", "y", namespace=NS, valid_from="2026-03-01T00:00:00Z", supersedes=old
    )
    assert new["superseded"] is True
    closed = facts.get_fact(old, [NS])
    assert closed["valid_from"] == "2026-01-01T00:00:00Z"  # нормализовано к UTC
    assert closed["valid_to"] == "2026-03-01T00:00:00Z"
    assert closed["superseded_by"] == new["fact_id"]
    current = facts.facts(subject="alice", namespaces=[NS])
    assert [f["object"] for f in current] == ["y"] and current[0]["valid_to"] is None
    assert current[0]["supersedes"] == old and current[0]["predicate"] == "WORKS_ON"
    then = facts.facts(subject="alice", namespaces=[NS], as_of="2026-02-01T00:00:00Z")
    assert [f["object"] for f in then] == ["x"]
    assert facts.facts(subject="alice", namespaces=[NS], as_of="2025-12-31T00:00:00Z") == []
    history = facts.facts(
        subject="alice", predicate="works_on", namespaces=[NS], include_closed=True
    )
    assert sorted(f["object"] for f in history) == ["x", "y"]
    assert facts.facts(subject="alice", namespaces=[OTHER]) == []


# --- конкурентная запись без advisory-локов ---


def _parallel(settings, n: int, work) -> list[BaseException | None]:
    barrier = threading.Barrier(n)
    results: list[BaseException | None] = [None] * n

    def run(i: int) -> None:
        conn = dbmod.connect(settings, autocommit=True)
        try:
            graph = GraphStore(conn, settings.graph_name, settings.default_namespace)
            barrier.wait()
            work(i, graph, FactStore(graph))
        except BaseException as exc:  # noqa: BLE001 — результат потока проверяет тест
            results[i] = exc
        finally:
            conn.close()

    threads = [threading.Thread(target=run, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    return results


def test_concurrent_reinforcement_keeps_every_observation(stores, settings):
    _, graph, facts, _, _ = stores
    _node(graph, "a")
    _node(graph, "b")
    errors = _parallel(
        settings,
        8,
        lambda i, _g, f: f.assert_fact("a", "owns", "b", namespace=NS, observation_id=f"o{i}"),
    )
    assert errors == [None] * 8
    (fact,) = facts.facts(subject="a", namespaces=[NS])
    assert sorted(fact["observation_ids"]) == sorted(f"o{i}" for i in range(8))


def test_concurrent_guarded_first_write_is_serialized(stores, settings):
    """Две записи нового узла с разной видимостью: вторая видит первую и получает отказ."""
    _, graph, _, _, _ = stores
    for round_no in range(5):
        key = f"secret:{round_no}"

        def write(i: int, g: GraphStore, _f: FactStore) -> None:
            scope = f"workspace:w{i}"
            g.write_fact(
                key,
                "note",
                "x",
                properties={"scopes": [scope]},
                namespace=NS,
                allowed_scopes=[scope],
            )

        errors = _parallel(settings, 2, write)
        assert sum(isinstance(e, ForeignObjectError) for e in errors) == 1, errors
        assert sum(e is None for e in errors) == 1, errors
        (node,) = graph.nodes_by_keys([key], [NS])
        assert len(node["props"]["scopes"]) == 1


# --- обычная роль, соседство с AGE ---


def test_graph_works_under_ordinary_role(settings):
    """Сервису не нужен суперпользователь: схема графа и запись — под обычной ролью."""
    conn = dbmod.connect(settings, autocommit=True)
    role = None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
            if cur.fetchone()[0]:
                # Тестовая база под суперпользователем: проверяем от роли без привилегий.
                role = f"t_plain_{uuid.uuid4().hex[:8]}"
                cur.execute(f'CREATE ROLE "{role}" NOSUPERUSER NOLOGIN')
                cur.execute(f'GRANT CREATE ON DATABASE "{conn.info.dbname}" TO "{role}"')
                cur.execute(f'SET ROLE "{role}"')
            cur.execute("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
            assert cur.fetchone()[0] is False
        graph = GraphStore(conn, settings.graph_name, settings.default_namespace)
        graph.ensure_schema()
        facts = FactStore(graph)
        _node(graph, "a")
        _node(graph, "b")
        fid = facts.assert_fact("a", "owns", "b", namespace=NS)["fact_id"]
        assert facts.get_fact(fid, [NS])["object"] == "b"
        assert _keys(graph.expand_neighbors(["a"], namespaces=[NS])) == {"b"}
    finally:
        dbmod.drop_graph(conn, settings.graph_name)
        if role:
            with conn.cursor() as cur:
                cur.execute("RESET ROLE")
                cur.execute(f'DROP OWNED BY "{role}"')
                cur.execute(f'DROP ROLE "{role}"')
        conn.close()


def test_coexists_with_age_label_tables_in_same_schema(settings):
    """Схема графа AGE с таблицами меток: наши таблицы встают рядом, drop их не трогает."""
    conn = dbmod.connect(settings, autocommit=True)
    schema = settings.graph_name
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            # Как у AGE: родительские таблицы и таблицы меток по имени вида/связи.
            for table in ("_ag_label_vertex", "_ag_label_edge", "endpoint", "PART_OF"):
                cur.execute(f'CREATE TABLE "{schema}"."{table}" (id bigint, properties text)')
            cur.execute(f"INSERT INTO \"{schema}\".endpoint VALUES (1, '{{}}')")
        graph = GraphStore(conn, schema, settings.default_namespace)
        graph.ensure_schema()
        _node(graph, "GET /x", "endpoint")
        assert graph.get_node("GET /x", [NS])["type"] == "endpoint"
        dbmod.drop_graph(conn, schema)
        assert not dbmod.graph_exists(conn, schema)
        with conn.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM "{schema}".endpoint')
            assert cur.fetchone()[0] == 1  # данные «AGE» на месте, схема не удалена
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_label_named_like_graph_table_is_refused(settings):
    """Метка AGE с именем graph_nodes — явный отказ, а не запись в чужую таблицу."""
    conn = dbmod.connect(settings, autocommit=True)
    schema = settings.graph_name
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'CREATE TABLE "{schema}".graph_nodes (id bigint, properties text)')
        with pytest.raises(dbmod.GraphSchemaConflict, match="graph_nodes"):
            dbmod.ensure_graph(conn, schema)
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


# --- ревью TASK-001284 (TASK-001285) ---


def _plain_role(conn: psycopg.Connection, *, create_on_db: bool) -> str | None:
    """Перейти в роль без суперпользователя; вернуть её имя (None — уже обычная роль)."""
    with conn.cursor() as cur:
        cur.execute("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
        if not cur.fetchone()[0]:
            return None
        role = f"t_plain_{uuid.uuid4().hex[:8]}"
        cur.execute(f'CREATE ROLE "{role}" NOSUPERUSER NOLOGIN')
        if create_on_db:
            cur.execute(f'GRANT CREATE ON DATABASE "{conn.info.dbname}" TO "{role}"')
        return role


def _drop_role(conn: psycopg.Connection, role: str | None) -> None:
    if not role:
        return
    with conn.cursor() as cur:
        cur.execute("RESET ROLE")
        cur.execute(f'DROP OWNED BY "{role}"')
        cur.execute(f'DROP ROLE "{role}"')


def test_owner_of_existing_schema_needs_no_create_on_database(settings):
    """Схема уже создана для роли: CREATE SCHEMA не зовётся, права на базу не нужны (п.1)."""
    conn = dbmod.connect(settings, autocommit=True)
    schema = settings.graph_name
    role = None
    try:
        role = _plain_role(conn, create_on_db=False)
        if role is None:
            pytest.skip("тестовая база не под суперпользователем — роль без CREATE не завести")
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{role}"')
            cur.execute(f'SET ROLE "{role}"')
        graph = GraphStore(conn, schema, settings.default_namespace)
        graph.ensure_schema()
        _node(graph, "a")
        assert graph.get_node("a", [NS]) is not None
    finally:
        with conn.cursor() as cur:
            cur.execute("RESET ROLE")
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        _drop_role(conn, role)
        conn.close()


def test_long_natural_key_is_refused_before_index(stores):
    """Ключ длиннее предела — GraphKeyTooLong с кодом, а не ProgramLimitExceeded (п.2)."""
    _, graph, facts, _, _ = stores
    longest_ns = "n" + "s" * 199
    label = "k" * dbmod.MAX_LABEL_BYTES
    key = "я" * (dbmod.MAX_NATURAL_KEY_BYTES // 2)  # ровно предел: 2 байта на букву
    # Предельные namespace, вид и ключ вместе помещаются в строку индекса.
    graph.upsert_node(Node(type=label, natural_key=key, title="t", namespace=longest_ns))
    assert graph.get_node(key, [longest_ns])["type"] == label
    with pytest.raises(dbmod.GraphKeyTooLong) as err:
        _node(graph, key + "x")
    assert err.value.code == "natural_key_too_long"
    assert str(err.value).startswith("natural_key_too_long:")
    with pytest.raises(dbmod.GraphKeyTooLong, match="^label_too_long"):
        _node(graph, "short", "k" * (dbmod.MAX_LABEL_BYTES + 1))
    with pytest.raises(dbmod.GraphKeyTooLong, match="^natural_key_too_long"):
        graph.upsert_entities(
            "note", [{"k": "ok"}, {"k": "x" * 3000}], NS, valid_from="", last_seen=""
        )
    assert graph.get_node("ok", [NS]) is None  # батч отвергнут целиком
    _node(graph, "a")
    _node(graph, "b")
    with pytest.raises(dbmod.GraphKeyTooLong, match="^label_too_long"):
        facts.assert_fact("a", "p" * 300, "b", namespace=NS)
    with pytest.raises(dbmod.GraphKeyTooLong, match="^label_too_long"):
        _link(graph, "a", "b", "L" * 300)


def test_key_length_constraint_backs_up_the_code_check(stores):
    """Запись в обход кода получает CheckViolation с именем ограничения (п.2)."""
    conn, graph, _, _, _ = stores
    with pytest.raises(psycopg.errors.CheckViolation, match="graph_nodes_key_len"):
        with conn.cursor() as cur:
            cur.execute(
                graph.sql("INSERT INTO {N} (namespace, label, natural_key) VALUES (%s, %s, %s)"),
                (NS, "note", "x" * 3000),
            )


def test_long_natural_key_http_400_with_code(settings, stores, monkeypatch):
    """HTTP: длинный ключ — 400 с кодом natural_key_too_long в detail, не 500 (п.2)."""
    import platform_memory.server.app as app_mod
    from fastapi.testclient import TestClient

    monkeypatch.setattr(app_mod, "get_settings", lambda: settings)
    client = TestClient(app_mod.app, raise_server_exceptions=False)
    resp = client.post(
        "/api/brain/facts",
        json={"natural_key": "k" * 3000, "type": "note", "title": "t", "scope": {"namespace": NS}},
    )
    assert resp.status_code == 400, resp.text
    assert resp.json()["detail"].startswith("natural_key_too_long:")


def test_facts_by_subject_take_the_end_from_the_same_namespaces(stores):
    """Конец факта ищется в namespaces выборки, а не по ключу во всех базах (п.3)."""
    conn, graph, facts, _, _ = stores
    _node(graph, "a")
    _node(graph, "b")
    _node(graph, "a", ns=OTHER)
    facts.assert_fact("a", "owns", "b", namespace=NS)
    # Ребро-факт NS, чей субъект — узел другой базы (запись в обход API).
    with conn.cursor() as cur:
        cur.execute(
            graph.sql(
                "INSERT INTO {E} (namespace, type, src_id, dst_id, fact_id) "
                "SELECT %s, 'OWNS', a.id, b.id, 'fact-foreign' FROM {N} a, {N} b "
                "WHERE a.namespace = %s AND a.natural_key = 'a' "
                "AND b.namespace = %s AND b.natural_key = 'b'"
            ),
            (NS, OTHER, NS),
        )
    got = facts.facts(subject="a", namespaces=[NS])
    assert [f["fact_id"] for f in got] != [] and "fact-foreign" not in {f["fact_id"] for f in got}
    assert "fact-foreign" not in {f["fact_id"] for f in facts.facts(obj="b", namespaces=[OTHER])}


def test_guarded_upsert_does_not_block_edges_to_the_node(settings, stores):
    """Защищённый upsert держит FOR NO KEY UPDATE: вставка ребра к узлу не ждёт (п.4)."""
    _, graph, _, _, _ = stores
    _node(graph, "hub")
    _node(graph, "leaf")
    other = dbmod.connect(settings, autocommit=True)
    holder = dbmod.connect(settings, autocommit=True)
    try:
        held = GraphStore(holder, settings.graph_name, settings.default_namespace)
        writer = GraphStore(other, settings.graph_name, settings.default_namespace)
        with other.cursor() as cur:
            cur.execute("SET lock_timeout = '2s'")
        with holder.transaction():
            held.upsert_node(
                Node(type="entity", natural_key="hub", title="hub", namespace=NS),
                guard_scopes=True,
            )
            # Транзакция с блокировкой строки узла ещё открыта.
            assert writer.upsert_edge(Edge(type="LINKS_TO", src="leaf", dst="hub", namespace=NS))
    finally:
        other.close()
        holder.close()


def test_schema_version_mark_and_catch_up(stores):
    """Отметка версии на таблице узлов; схема без неё (TASK-001284) догоняется (п.5)."""
    conn, graph, _, _, _ = stores
    assert dbmod.graph_schema_version(conn, graph.graph) == dbmod.GRAPH_SCHEMA_VERSION
    with conn.cursor() as cur:
        # Схема первой версии: без отметки и без ограничений длины.
        cur.execute(graph.sql("COMMENT ON TABLE {N} IS NULL"))
        cur.execute(graph.sql("ALTER TABLE {N} DROP CONSTRAINT graph_nodes_key_len"))
        cur.execute(graph.sql("ALTER TABLE {E} DROP CONSTRAINT graph_edges_type_len"))
    _node(graph, "kept")
    assert dbmod.graph_schema_version(conn, graph.graph) == 1
    graph.ensure_schema()
    assert dbmod.graph_schema_version(conn, graph.graph) == dbmod.GRAPH_SCHEMA_VERSION
    with conn.cursor() as cur:
        cur.execute(
            "SELECT conname FROM pg_constraint WHERE conrelid IN (to_regclass(%s), to_regclass(%s))"
            " AND contype = 'c'",
            (f'"{graph.graph}".graph_nodes', f'"{graph.graph}".graph_edges'),
        )
        assert {r[0] for r in cur.fetchall()} == {"graph_nodes_key_len", "graph_edges_type_len"}
    assert graph.get_node("kept", [NS]) is not None  # данные пережили догон
    # Схема новее кода — не трогается.
    with conn.cursor() as cur:
        cur.execute(graph.sql("COMMENT ON TABLE {N} IS 'platform-memory graph schema v99'"))
    graph.ensure_schema()
    assert dbmod.graph_schema_version(conn, graph.graph) == 99


def test_age_labels_named_like_graph_tables_are_not_a_graph(settings):
    """Метки AGE graph_nodes и graph_edges: не «граф есть», отказ и drop их не трогает (п.6)."""
    conn = dbmod.connect(settings, autocommit=True)
    schema = settings.graph_name
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            for table in ("graph_nodes", "graph_edges"):
                cur.execute(f'CREATE TABLE "{schema}".{table} (id bigint, properties text)')
                cur.execute(f"INSERT INTO \"{schema}\".{table} VALUES (1, '{{}}')")
        assert not dbmod.graph_exists(conn, schema)
        with pytest.raises(dbmod.GraphSchemaConflict):
            dbmod.ensure_graph(conn, schema)
        with pytest.raises(dbmod.GraphSchemaConflict):
            dbmod.graph_schema_version(conn, schema)
        dbmod.drop_graph(conn, schema)
        with conn.cursor() as cur:
            for table in ("graph_nodes", "graph_edges"):
                cur.execute(f'SELECT count(*) FROM "{schema}".{table}')
                assert cur.fetchone()[0] == 1  # таблицы меток AGE на месте
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_observation_with_too_long_entity_key_reports_assertion_error(settings, stores):
    """Наблюдение принято, assertion с длинным ключом — в ошибках, а не 500 (ревью 1284)."""
    from platform_memory import retain_observation

    result = retain_observation(
        settings,
        {
            "kind": "note",
            "content": "сущность с очень длинным id",
            "assertions": [
                {"assert": "entity", "entity": {"type": "person", "id": "z" * 3000}},
                {"assert": "entity", "entity": {"type": "person", "id": "ok"}},
            ],
        },
        namespace=NS,
    )
    _, graph, _, _, obs = stores
    record = obs.get(result["observation_id"], namespaces=[NS])
    assert result["status"] != "processed"
    assert "natural_key_too_long" in json.dumps(record.to_payload(), ensure_ascii=False)
    assert graph.get_node("person:ok", [NS]) is not None

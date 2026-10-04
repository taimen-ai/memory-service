"""Тексты гибридного запроса бенчмарка ``bench.py engines``: запасные источники.

Крупнейший namespace копии staging был без чанков: пустой список запросов давал
``ZeroDivisionError`` в ``hybrid`` (TASK-001370).
"""

from __future__ import annotations

import pytest

from benchmarks import engines
from platform_memory.core.models import Chunk, Node
from platform_memory.core.observations import Observation

pytestmark = pytest.mark.integration

NS = "tenant:t1:ws:bench"


def test_queries_from_chunks(settings, stores):
    conn, _g, _f, index, obs = stores
    obs.insert(Observation(namespace=NS, content="наблюдение не нужно").canonicalize())
    index.add_chunks(
        [
            Chunk(node_key="d", text=f"текст чанка {i}", source_path="d.md", order=i, namespace=NS)
            for i in range(3)
        ],
        [[0.1] * settings.embedding_dim] * 3,
    )
    queries, dim = engines._queries(conn, settings, NS, 10, 42)
    assert sorted(queries) == [f"текст чанка {i}" for i in range(3)]
    assert dim == settings.embedding_dim
    # Детерминизм по seed.
    assert engines._queries(conn, settings, NS, 10, 42)[0] == queries


def test_queries_fall_back_to_observations(settings, stores):
    conn, graph, _f, _i, obs = stores
    obs.insert(Observation(namespace=NS, data={"k": 1}).canonicalize())  # без content
    obs.insert(Observation(namespace=NS, content="  первое   наблюдение ").canonicalize())
    graph.upsert_node(Node(type="task", natural_key="T-1", title="t", namespace=NS))
    queries, _dim = engines._queries(conn, settings, NS, 10, 42)
    assert queries == ["первое наблюдение"]


def test_queries_fall_back_to_graph_nodes(settings, stores):
    conn, graph, _f, _i, _obs = stores
    for key in ("T-1", "T-2"):
        graph.upsert_node(Node(type="task", natural_key=key, title=key, namespace=NS))
    queries, _dim = engines._queries(conn, settings, NS, 10, 42)
    assert sorted(queries) == ["T-1", "T-2"]
    assert engines._queries(conn, settings, NS, 1, 42)[0] in (["T-1"], ["T-2"])


def test_queries_empty_namespace_is_explicit_error(settings, stores):
    conn, _g, _f, _i, _obs = stores
    with pytest.raises(SystemExit, match="нет ни чанков"):
        engines._queries(conn, settings, NS, 10, 42)


def test_queries_without_observations_table(settings, stores):
    conn, graph, _f, _i, _obs = stores
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE public."{settings.observations_table}"')
    graph.upsert_node(Node(type="task", natural_key="T-9", title="t", namespace=NS))
    assert engines._queries(conn, settings, NS, 10, 42)[0] == ["T-9"]

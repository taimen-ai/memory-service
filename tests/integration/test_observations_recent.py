"""Канал recent наблюдений (Context Compiler): порядок по колонке и чтение по индексу.

Раньше ``ORDER BY ingested_at`` попадал на текстовый алиас ``ingested_at::text`` из
списка выборки: сортировка по тексту и ``Seq Scan`` всей таблицы (TASK-001370).
"""

from __future__ import annotations

from typing import Any

import pytest
from psycopg import sql

from platform_memory.core.observations import Observation

pytestmark = pytest.mark.integration

NS = "tenant:t1:ws:w1"
OTHER = "tenant:t1:ws:w2"


def _obs(ns: str, ext: str, *, occurred: str = "", scopes=(), kind: str = "note") -> Observation:
    return Observation(
        namespace=ns,
        source_system="test",
        external_id=ext,
        kind=kind,
        occurred_at=occurred,
        scopes=list(scopes),
        content=f"наблюдение {ext}",
    ).canonicalize()


def _set_ingested(conn, table: str, oid: str, ts: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "UPDATE {t} SET ingested_at = %s::timestamptz WHERE observation_id = %s"
            ).format(t=sql.Identifier("public", table)),
            (ts, oid),
        )


def _plan_nodes(plan: dict[str, Any]) -> list[dict[str, Any]]:
    out = [plan]
    for child in plan.get("Plans", []):
        out += _plan_nodes(child)
    return out


def test_recent_empty_table_and_zero_limit(settings, stores):
    _conn, _g, _f, _i, obs = stores
    assert obs.list_recent([NS]) == []
    obs.insert(_obs(NS, "a"))
    assert obs.list_recent([NS], limit=0) == []


def test_recent_orders_by_ingested_column_not_text(settings, stores):
    """Переход на зимнее время: по тексту ``01:30-04`` > ``01:10-05``, по времени — наоборот."""
    conn, _g, _f, _i, obs = stores
    with conn.cursor() as cur:
        cur.execute("SET TimeZone = 'America/New_York'")
    earlier, _ = obs.insert(_obs(NS, "earlier"))
    later, _ = obs.insert(_obs(NS, "later"))
    _set_ingested(conn, settings.observations_table, earlier, "2026-11-01 05:30:00+00")  # 01:30-04
    _set_ingested(conn, settings.observations_table, later, "2026-11-01 06:10:00+00")  # 01:10-05
    assert [r.observation_id for r in obs.list_recent([NS])] == [later, earlier]


def test_recent_merges_namespaces_with_order_and_limit(settings, stores):
    conn, _g, _f, _i, obs = stores
    a, _ = obs.insert(_obs(NS, "a", occurred="2026-10-01T10:00:00Z"))
    b, _ = obs.insert(_obs(OTHER, "b", occurred="2026-10-02T10:00:00Z"))
    c, _ = obs.insert(_obs(NS, "c", occurred="2026-09-30T10:00:00Z"))
    d, _ = obs.insert(_obs(OTHER, "d"))  # без event time — в конце
    e, _ = obs.insert(_obs("tenant:t1:ws:foreign", "e", occurred="2026-10-03T10:00:00Z"))

    got = [r.observation_id for r in obs.list_recent([NS, OTHER, NS])]
    assert got == [b, a, c, d]
    assert e not in got
    assert [r.observation_id for r in obs.list_recent([NS, OTHER], limit=2)] == [b, a]
    # Повтор — тот же ответ.
    assert [r.observation_id for r in obs.list_recent([NS, OTHER])] == got


def test_recent_filters_still_apply(settings, stores):
    _conn, _g, _f, _i, obs = stores
    open_, _ = obs.insert(_obs(NS, "open", occurred="2026-10-01T00:00:00Z"))
    team, _ = obs.insert(_obs(NS, "team", occurred="2026-10-02T00:00:00Z", scopes=["team:a"]))
    ws, _ = obs.insert(
        _obs(NS, "ws", occurred="2026-10-03T00:00:00Z", scopes=["workspace:x"], kind="event")
    )
    assert [r.observation_id for r in obs.list_recent([NS], scopes=["team:a"])] == [team, open_]
    assert [r.observation_id for r in obs.list_recent([NS], allowed_scopes=[])] == [team, open_]
    assert [r.observation_id for r in obs.list_recent([NS], allowed_scopes=["workspace:x"])] == [
        ws,
        team,
        open_,
    ]
    assert [r.observation_id for r in obs.list_recent([NS], kinds=["event"])] == [ws]
    assert obs.list_recent([NS], status="processed") == []


def test_recent_plan_uses_index(settings, stores):
    """План канала recent — упорядоченное чтение индекса ``_ns_recent``, без Seq Scan."""
    conn, _g, _f, _i, obs = stores
    obs.ensure_schema()  # повторно — идемпотентно
    tbl = sql.Identifier("public", settings.observations_table)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "INSERT INTO {t} (observation_id, namespace, kind, occurred_at, scopes, content) "
                "SELECT 'o' || g, CASE WHEN g %% 50 = 0 THEN %s ELSE 'tenant:big' END, 'note', "
                "CASE WHEN g %% 3 = 0 THEN '' ELSE '2026-10-01T00:00:' || lpad((g %% 60)::text, "
                "2, '0') || 'Z' END, "
                "CASE WHEN g %% 2 = 0 THEN '[\"workspace:x\"]'::jsonb ELSE '[]'::jsonb END, "
                "'текст ' || g FROM generate_series(1, 20000) g"
            ).format(t=tbl),
            (NS,),
        )
        cur.execute(sql.SQL("ANALYZE {t}").format(t=tbl))
        for nss in ([NS], [NS, "tenant:big"]):
            query, params = obs._recent_query(nss, scopes=["team:a"], limit=10, allowed_scopes=[])
            cur.execute(sql.SQL("EXPLAIN (FORMAT JSON) ") + query, params)
            nodes = _plan_nodes(cur.fetchone()[0][0]["Plan"])
            scans = [n for n in nodes if n.get("Relation Name") == settings.observations_table]
            assert scans, nodes
            assert all(n["Node Type"] == "Index Scan" for n in scans), scans
            assert {n["Index Name"] for n in scans} == {f"{settings.observations_table}_ns_recent"}
    assert len(obs.list_recent(["tenant:big"], limit=10, allowed_scopes=[])) == 10

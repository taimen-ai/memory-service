"""Канал resolve компилятора на живой БД (MEM-ADR-020, амендмент «канал resolve»).

Снимок-фикстура software-delivery (``tests/fixtures/software_delivery``) плюс сущности
целей (эндпоинты goals, событие ``goal.created``, ADR ``CP-0062``) сверяются в
namespace workspace; затем ``POST /api/memory/context`` с описанием задачи в query
возвращает контракты в ``related_entities`` с цитатой ``repo@sha:path:line``. Узел с
чужим workspace-scope не виден при ``allowedScopes``; namespace не протекает; без
совпадений выдача прежняя; поиск идёт по индексам журнала.
"""

from __future__ import annotations

import copy
import json
from contextlib import contextmanager
from pathlib import Path

import pytest
from psycopg import sql

from platform_memory.context import build_context
from platform_memory.context import compiler as compiler_mod
from platform_memory.domain.reconcile import SnapshotLedger, reconcile
from platform_memory.domain.registry import open_registry
from platform_memory import retain_observation

pytestmark = pytest.mark.integration

NS = "tenant:t1:ws:w1"
OTHER_NS = "tenant:t2:ws:w9"
FIXTURES = Path(__file__).parent.parent / "fixtures" / "software_delivery"
SHA = "f6023272c0ffee00000000000000000000000001"
QUERY = "Правки в POST /api/v1/goals/{goal_id}, событие goal.created, CP-ADR-0062"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _prov(path: str, line: int) -> dict:
    return {"repo": "control-plane", "sha": SHA, "path": path, "line": line}


def _goals_snapshot() -> dict:
    """Снимок control-plane из фикстуры, дополненный сущностями целей."""
    snap = copy.deepcopy(_load("control-plane.snapshot.json"))
    snap["snapshotId"] = "control-plane@goals"
    api = "src/control_plane/api/v1/goals.py"
    snap["entities"] += [
        {
            "kind": "endpoint",
            "key": "POST /api/v1/goals/{}",
            "attributes": {"path": "/api/v1/goals/{goal_id}", "method": "POST"},
            "title": "POST /api/v1/goals/{}",
            "provenance": _prov(api, 40),
        },
        {
            "kind": "endpoint",
            "key": "GET /api/v1/goals/{}",
            "attributes": {"path": "/api/v1/goals/{goal_id}", "method": "GET"},
            "title": "GET /api/v1/goals/{}",
            "provenance": _prov(api, 20),
        },
        {
            "kind": "event",
            "key": "goal.created",
            "attributes": {},
            "title": "goal.created",
            "provenance": _prov("src/control_plane/application/goals/service.py", 77),
        },
        {
            "kind": "adr",
            "key": "CP-0062",
            "attributes": {"status": "Accepted"},
            "title": "ADR-0062: Цели и приёмка",
            "provenance": _prov("docs/decisions/CP-0062-goals.md", 1),
        },
        {
            "kind": "client_method",
            "key": "control-plane:client.ControlPlaneClient.get_claimability",
            "attributes": {},
            "title": "client.ControlPlaneClient.get_claimability",
            "provenance": _prov("client/src/control_plane_client/client.py", 500),
        },
        {"kind": "source_file", "key": f"control-plane:{api}", "attributes": {"path": api}},
    ]
    snap["relations"] += [
        {
            "relation": "defined_in",
            "from": {"kind": "endpoint", "key": "POST /api/v1/goals/{}"},
            "to": {"kind": "source_file", "key": f"control-plane:{api}"},
        },
        {
            "relation": "part_of",
            "from": {"kind": "source_file", "key": f"control-plane:{api}"},
            "to": {"kind": "component", "key": "control-plane"},
        },
    ]
    return snap


def _foreign_snapshot() -> dict:
    """Снимок другого источника со своим ADR того же номера (виден только workspace:other)."""
    return {
        "pack": "software-delivery@1",
        "source": "git:taimen",
        "scope": "taimen",
        "snapshotId": "taimen@1",
        "observedAt": "2026-09-23T08:45:00+00:00",
        "entities": [
            {
                "kind": "adr",
                "key": "TAI-0062",
                "attributes": {"status": "Proposed"},
                "title": "ADR-0062: чужое решение",
                "provenance": {"repo": "taimen", "sha": "abc", "path": "docs/TAI-0062.md"},
            }
        ],
        "relations": [],
    }


def _setup_ns(conn, settings, ns: str) -> None:
    reg = open_registry(conn, settings)
    reg.ensure_schema()
    reg.register(_load("pack.json"))
    reg.put_settings(ns, strict=True, packages=["software-delivery"])


@pytest.fixture
def seeded(stores, settings):
    conn = stores[0]
    _setup_ns(conn, settings, NS)
    reconcile(
        settings, snapshot=_goals_snapshot(), namespace=NS, scopes=["workspace:w1"], conn=conn
    )
    reconcile(
        settings, snapshot=_foreign_snapshot(), namespace=NS, scopes=["workspace:other"], conn=conn
    )
    return conn


def _entities(pack) -> dict[str, dict]:
    section = next(s for s in pack.to_payload()["sections"] if s["kind"] == "related_entities")
    return {i["id"]: i for i in section["items"]}


def test_context_finds_contracts_over_http(seeded, test_settings, monkeypatch):
    from fastapi.testclient import TestClient

    import platform_memory.server.app as mod

    settings = test_settings.model_copy_with(
        server_api_key="", server_api_keys_pii="", api_keys="", iam_enabled=False
    )
    monkeypatch.setattr(mod, "get_settings", lambda: settings)
    resp = TestClient(mod.app).post(
        "/api/memory/context",
        json={
            "query": QUERY,
            "anchors": ["task:TASK-1", "workspace:w1"],
            "scope": {"namespaces": [NS]},
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    section = next(s for s in body["sections"] if s["kind"] == "related_entities")
    items = {i["id"]: i for i in section["items"]}
    endpoint = items["node:POST /api/v1/goals/{}"]
    event = items["node:goal.created"]
    adr = items["node:CP-0062"]
    for item in (endpoint, event, adr):
        assert item["signals"]["evidence"] == "resolved"
        assert item["source_path"].startswith(f"control-plane@{SHA}:")
        assert item["provenance"]["snapshot"]["snapshot_id"] == "control-plane@goals"
    assert endpoint["source_path"] == f"control-plane@{SHA}:src/control_plane/api/v1/goals.py:40"
    assert endpoint["signals"]["resolve"]["method"] == "normalized"
    assert event["signals"]["resolve"]["method"] == "natural_key"
    assert adr["signals"]["resolve"]["method"] == "alias"
    assert adr["title"].startswith("ADR-0062")
    # Точный метод: GET того же пути не разрешён; чужой ADR-0062 не разрешён (CP-ADR-0062).
    assert "node:GET /api/v1/goals/{}" not in items
    assert "node:TAI-0062" not in items
    # Окрестность разрешённого эндпоинта — через graph expansion.
    assert "node:control-plane:src/control_plane/api/v1/goals.py" in items
    # Цитаты — и в sources пакета.
    assert any(s["node_key"] == "CP-0062" for s in body["sources"])
    stats = body["stats"]
    assert stats["channels"]["resolve"] == 3
    assert {r["key"] for r in stats["resolved"]} == {
        "POST /api/v1/goals/{}",
        "goal.created",
        "CP-0062",
    }

    trace = (
        TestClient(mod.app)
        .get(f"/api/memory/context/trace/{body['trace_id']}", params={"namespace": NS})
        .json()
    )
    decisions = trace["decisions"]
    inputs = {c["input"]: c for c in decisions["resolve"]["candidates"]}
    assert inputs["CP-ADR-0062"]["resolved"][0]["key"] == "CP-0062"
    assert "ADR-0062" not in inputs  # поглощён более точным CP-ADR-0062
    assert "POST /api/v1/goals/{goal_id}" in trace["request"]["resolve_candidates"]


def test_resolved_rank_above_recent_observations(seeded, settings):
    retain_observation(
        settings,
        {
            "kind": "note",
            "occurred_at": "2026-09-23T09:00:00Z",
            "content": "Свежая заметка: правки в goal.created и CP-ADR-0062 обсуждали",
        },
        namespace=NS,
    )
    pack = build_context(settings, {"query": QUERY, "namespaces": [NS]}, conn=seeded)
    items = [i for s in pack.sections for i in s.items]
    obs = [i for i in items if i.kind == "observation"]
    resolved = [i for i in items if i.signals.get("evidence") == "resolved"]
    assert obs and resolved
    assert min(i.score for i in resolved) > max(i.score for i in obs)


def test_alias_and_suffix_forms(seeded, settings):
    pack = build_context(
        settings,
        {
            "query": "ADR-0062, путь /api/v1/goals/{id} и метод get_claimability",
            "namespaces": [NS],
            "strategy": "exact",
        },
        conn=seeded,
    )
    items = _entities(pack)
    # ADR-0062 — все ряды; путь без метода — все методы; метод клиента — по суффиксу.
    assert {"node:CP-0062", "node:TAI-0062"} <= set(items)
    assert {"node:POST /api/v1/goals/{}", "node:GET /api/v1/goals/{}"} <= set(items)
    assert items["node:GET /api/v1/goals/{}"]["signals"]["resolve"]["method"] == "suffix"
    method = "node:control-plane:client.ControlPlaneClient.get_claimability"
    assert items[method]["signals"]["resolve"]["method"] == "suffix"


def test_foreign_workspace_scope_hidden(seeded, settings):
    request = {"query": "ADR-0062", "namespaces": [NS], "allowed_scopes": ["workspace:w1"]}
    items = _entities(build_context(settings, request, conn=seeded))
    assert "node:CP-0062" in items
    assert "node:TAI-0062" not in items
    request["allowed_scopes"] = ["workspace:other"]
    items = _entities(build_context(settings, request, conn=seeded))
    assert "node:TAI-0062" in items
    assert "node:CP-0062" not in items


def test_namespace_does_not_leak(seeded, settings):
    _setup_ns(seeded, settings, OTHER_NS)
    pack = build_context(settings, {"query": QUERY, "namespaces": [OTHER_NS]}, conn=seeded)
    assert pack.stats["channels"]["resolve"] == 0
    assert not pack.stats["resolved"]
    assert not _entities(pack)


def test_no_matches_keeps_previous_output(seeded, settings, monkeypatch):
    retain_observation(
        settings,
        {"kind": "note", "occurred_at": "2026-09-23T09:00:00Z", "content": "деплой в пятницу"},
        namespace=NS,
    )
    request = {"query": "что с деплоем в пятницу, M1.3 work_items", "namespaces": [NS]}
    with_resolve = build_context(settings, request, conn=seeded)
    assert with_resolve.stats["channels"]["resolve"] == 0
    monkeypatch.setattr(compiler_mod, "_resolve_channel", lambda *a, **kw: ([], [], []))
    without = build_context(settings, request, conn=seeded)

    def _shape(pack):
        return [(i.id, round(i.score, 9)) for s in pack.sections for i in s.items]

    assert _shape(with_resolve) == _shape(without)


class _RecordingConn:
    """Соединение, запоминающее запросы ``cursor().execute`` (для EXPLAIN тех же запросов)."""

    def __init__(self, conn):
        self._conn = conn
        self.executed: list[tuple] = []

    @contextmanager
    def cursor(self):
        with self._conn.cursor() as cur:
            outer = self

            class _Cur:
                def execute(self, query, params=None):
                    outer.executed.append((query, params))
                    return cur.execute(query, params)

                def __getattr__(self, name):
                    return getattr(cur, name)

            yield _Cur()


def test_resolve_uses_ledger_indexes(seeded, settings):
    """Ключ, псевдоним и суффикс ключа — по индексам журнала, без seq scan (~20k версий)."""
    items = f'public."{settings.snapshots_table}_items"'
    with seeded.cursor() as cur:
        cur.execute(
            f"INSERT INTO {items} (namespace, source, item_type, kind, item_key, version_id, "
            "content_hash, graph_ref, payload, valid_from, opened_in) "
            "SELECT %s, 'git:filler', 'entity', 'adr', 'F-' || g, 'v' || g, 'h', 'F-' || g, "
            "jsonb_build_object('aliases', jsonb_build_array('FA-' || g)), '2026-09-01', 's' "
            "FROM generate_series(1, 20000) g",
            (NS,),
        )
        cur.execute(f"ANALYZE {items}")
    # EXPLAIN — ровно на тех запросах, что строит SnapshotLedger (ключ, aliases,
    # перевёрнутый ключ для суффикса), с фильтром видимости, как у канала.
    ledger = SnapshotLedger(_RecordingConn(seeded), settings.snapshots_table)
    kw = {"allowed_scopes": ["workspace:w1"]}
    ledger.entities_by_keys([NS], ["CP-0062", "goal.created"], **kw)
    ledger.entities_by_aliases([NS], ["ADR-0062"], **kw)
    ledger.entities_by_suffixes(
        [NS], [".get_claimability", " /api/v1/goals/{}"], kinds=[["client_method"], None], **kw
    )
    assert len(ledger.conn.executed) == 3
    plans = []
    with seeded.cursor() as cur:
        for query, params in ledger.conn.executed:
            cur.execute(sql.SQL("EXPLAIN ") + query, params)
            plans.append("\n".join(r[0] for r in cur.fetchall()))
    assert "key_idx" in plans[0]
    assert "aliases_idx" in plans[1]
    assert "rkey_idx" in plans[2]
    assert not any("Seq Scan" in p for p in plans)
    # И сам канал на наполненном журнале разрешает то же.
    pack = build_context(settings, {"query": QUERY, "namespaces": [NS]}, conn=seeded)
    assert pack.stats["channels"]["resolve"] == 3


def test_suffix_kinds_filter_in_sql(seeded, settings):
    """Виды суффикса — фильтр запроса журнала (до лимита на суффикс, ревью 367 п.3)."""
    ledger = SnapshotLedger(seeded, settings.snapshots_table)
    kw = {"per_suffix": 5}
    assert ledger.entities_by_suffixes([NS], [".get_claimability"], kinds=[["table"]], **kw) == []
    got = ledger.entities_by_suffixes([NS], [".get_claimability"], kinds=[["client_method"]], **kw)
    assert [r["kind"] for _, r in got] == ["client_method"]
    assert ledger.entities_by_suffixes([NS], [".get_claimability"], **kw) == got

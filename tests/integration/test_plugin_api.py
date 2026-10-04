"""Интеграционные тесты plugin-facing API (recall/search/retain/audit/trace/health).

Вызов хендлеров напрямую (без сети). Проверяем контракт plugin-facing API и
формирование подграфа трейса задачи на реальной БД (PostgreSQL + pgvector).
"""

from __future__ import annotations

import pytest

from platform_memory.core import db as dbmod
from platform_memory.graph import GraphStore
from platform_memory.ingest import ingest_vault

pytestmark = pytest.mark.integration


@pytest.fixture
def app_with_vault(test_settings, clean_db, monkeypatch):
    """Сервер с настройками теста и заингесченным mini-vault."""
    import platform_memory.server.app as appmod

    monkeypatch.setattr(appmod, "get_settings", lambda: test_settings)
    ingest_vault(test_settings, test_settings.vault_path, reset=True)
    return appmod


def test_recall_returns_memories_with_sources(app_with_vault):
    appmod = app_with_vault
    res = appmod.brain_recall(appmod.RecallRequest(query="имя продукта Альфа", budget="mid"))
    assert res["count"] >= 1
    assert res["memories"], "recall должен вернуть фрагменты памяти"
    # каждая «память» несёт цитату на источник (provenance)
    assert all("source_path" in m and "node_key" in m for m in res["memories"])
    assert res["sources"]
    assert any(s["source_path"].endswith(".md") for s in res["sources"])
    assert res["context"]


def test_recall_budget_controls_breadth(app_with_vault):
    appmod = app_with_vault
    low = appmod.brain_recall(appmod.RecallRequest(query="продукт", budget="low"))
    high = appmod.brain_recall(appmod.RecallRequest(query="продукт", budget="high"))
    # high-бюджет тянет не меньше фрагментов, чем low
    assert high["count"] >= low["count"]


def test_search_structural_and_semantic(app_with_vault):
    appmod = app_with_vault
    structural = appmod.brain_search(
        appmod.SearchRequest(query="решения", filters={"type": "decision"})
    )
    assert structural["mode"] == "structural"
    assert any(n["natural_key"] == "BIZ-001" for n in structural["results"])

    semantic = appmod.brain_search(appmod.SearchRequest(query="имя продукта Альфа"))
    assert semantic["mode"] == "semantic"
    assert semantic["count"] >= 1


def test_retain_is_idempotent_by_external_id(app_with_vault, test_settings):
    appmod = app_with_vault
    req = appmod.RetainRequest(
        content="Решено: назвать продукт Альфа.",
        type="decision",
        external_id="agent-fact-001",
        provenance={"trace_id": "issue-42", "actor": "Агент-А", "run_id": "run-7"},
    )
    first = appmod.brain_retain(req)
    second = appmod.brain_retain(req)  # повторно — без дубля
    assert first["natural_key"] == second["natural_key"] == "agent-fact-001"
    assert first["retained"] is True

    conn = dbmod.connect(test_settings)
    try:
        node = GraphStore(conn, test_settings.graph_name).get_node("agent-fact-001")
    finally:
        conn.close()
    assert node is not None
    assert node["origin"] == "agent"
    assert node["props"]["content"].startswith("Решено")


def test_audit_builds_trace_subgraph(app_with_vault):
    appmod = app_with_vault
    trace = "issue-77"
    appmod.brain_audit(
        appmod.AuditRequest(
            trace_id=trace,
            action="agent.run.started",
            actor="Агент-А",
            actor_id="agent-1",
            run_id="run-1",
            ts="2026-06-08T10:00:00",
            payload={"issueId": trace},
        )
    )
    appmod.brain_audit(
        appmod.AuditRequest(
            trace_id=trace,
            action="agent.run.finished",
            actor="Агент-А",
            actor_id="agent-1",
            run_id="run-1",
            ts="2026-06-08T10:05:00",
            payload={"status": "completed", "cost_cents": 12},
        )
    )
    # retain в том же трейсе — факт попадает в подграф трейса
    appmod.brain_retain(
        appmod.RetainRequest(
            content="Вывод: задача выполнена.",
            type="note",
            external_id="fact-issue-77",
            provenance={"trace_id": trace, "actor": "Агент-А"},
        )
    )

    sub = appmod.brain_trace(trace)
    assert sub["event_count"] == 2
    assert sub["fact_count"] >= 1
    # события отсортированы по времени
    ts_values = [(e.get("props") or {}).get("ts") for e in sub["events"]]
    assert ts_values == sorted(ts_values)
    actions = {(e.get("props") or {}).get("action") for e in sub["events"]}
    assert actions == {"agent.run.started", "agent.run.finished"}


def test_audit_is_idempotent_on_repeat_delivery(app_with_vault):
    appmod = app_with_vault
    trace = "issue-88"
    payload = {
        "trace_id": trace,
        "action": "approval.created",
        "actor": "system",
        "ts": "2026-06-08T11:00:00",
        "payload": {"approval_id": "ap-1"},
    }
    appmod.brain_audit(appmod.AuditRequest(**payload))
    appmod.brain_audit(appmod.AuditRequest(**payload))  # та же доставка — без дубля
    sub = appmod.brain_trace(trace)
    assert sub["event_count"] == 1


def test_audit_distinguishes_events_by_run_id(app_with_vault):
    appmod = app_with_vault
    trace = "issue-99"
    base = dict(
        trace_id=trace,
        action="agent.status_changed",
        actor="A",
        ts="2026-06-08T12:00:00",
        payload={"x": 1},
    )
    # одинаковые поля, но разный run_id → два разных события (не схлопываются)
    appmod.brain_audit(appmod.AuditRequest(**base, run_id="run-A"))
    appmod.brain_audit(appmod.AuditRequest(**base, run_id="run-B"))
    sub = appmod.brain_trace(trace)
    assert sub["event_count"] == 2


def test_x_run_id_header_threads_into_audit(app_with_vault):
    """Сквозной X-Run-Id: заголовок становится run_id события, если тело его не задало."""
    appmod = app_with_vault
    trace = "issue-xrid"
    appmod.brain_audit(
        appmod.AuditRequest(trace_id=trace, action="run.step", actor="A", ts="2026-06-08T12:00:00"),
        x_run_id="run-XRID",
    )
    sub = appmod.brain_trace(trace)
    run_ids = {(e.get("props") or {}).get("run_id") for e in sub["events"]}
    assert "run-XRID" in run_ids


def test_body_run_id_overrides_x_run_id_header(app_with_vault):
    """Тело приоритетнее заголовка (обратная совместимость)."""
    appmod = app_with_vault
    trace = "issue-xrid2"
    appmod.brain_audit(
        appmod.AuditRequest(
            trace_id=trace,
            action="run.step",
            actor="A",
            run_id="run-BODY",
            ts="2026-06-08T12:00:00",
        ),
        x_run_id="run-HEADER",
    )
    sub = appmod.brain_trace(trace)
    run_ids = {(e.get("props") or {}).get("run_id") for e in sub["events"]}
    assert "run-BODY" in run_ids and "run-HEADER" not in run_ids


def test_x_run_id_header_used_as_fact_trace_fallback(app_with_vault):
    """retain без явного trace берёт run_id из X-Run-Id → факт попадает в трейс прогона."""
    appmod = app_with_vault
    appmod.brain_retain(
        appmod.RetainRequest(content="факт без явного трейса", type="note", external_id="fact-hdr"),
        x_run_id="run-HDRFACT",
    )
    sub = appmod.brain_trace("run-HDRFACT")
    assert sub["fact_count"] >= 1


def test_health_ok(app_with_vault):
    appmod = app_with_vault
    health = appmod.brain_health()
    assert health["ok"] is True
    assert health["nodes"] >= 1


def test_concurrent_audit_same_trace_does_not_fail(test_settings, clean_db):
    """Регрессия: параллельные audit-события одного trace_id апсертят общий якорь
    pc_trace — upsert (INSERT … ON CONFLICT) не падает на гонке (при AGE падал с
    'Entity failed to be updated' без advisory-лока)."""
    from concurrent.futures import ThreadPoolExecutor

    conn0 = dbmod.connect(test_settings)
    try:
        GraphStore(conn0, test_settings.graph_name).ensure_schema()
    finally:
        conn0.close()

    def one_audit(i: int) -> None:
        conn = dbmod.connect(test_settings)
        try:
            GraphStore(conn, test_settings.graph_name).record_audit(
                "issue-race", f"agent-{i}", "run.step", payload={"i": i}, run_id=f"run-{i}"
            )
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(one_audit, range(16)))  # упавший поток поднимет исключение здесь

    conn = dbmod.connect(test_settings)
    try:
        store = GraphStore(conn, test_settings.graph_name)
        sub = store.trace_subgraph("issue-race")
        assert sub["event_count"] == 16
    finally:
        conn.close()


def test_retained_fact_survives_reingest(app_with_vault, test_settings):
    """Регрессия: write-back агента (retain) — узел И чанк — переживает повторный
    полный ingest vault (mark-and-sweep не должен подметать agent-данные)."""
    appmod = app_with_vault
    appmod.brain_retain(
        appmod.RetainRequest(
            content="Факт от агента: пилот продлён до июля.",
            type="note",
            external_id="agent-fact-survive",
        )
    )
    before = appmod.brain_recall(appmod.RecallRequest(query="пилот продлён до июля", budget="low"))
    assert any(m["node_key"] == "agent-fact-survive" for m in before["memories"])

    ingest_vault(test_settings, test_settings.vault_path)  # повторный ingest без reset

    after = appmod.brain_recall(appmod.RecallRequest(query="пилот продлён до июля", budget="low"))
    assert any(m["node_key"] == "agent-fact-survive" for m in after["memories"]), (
        "retain-чанк уничтожен mark-and-sweep при ре-ингесте"
    )

"""Temporal-факты на реальной БД (таблицы графа): creation, supersession, история, конфликты."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration

T0 = "2026-01-01T00:00:00Z"
T1 = "2026-06-01T00:00:00Z"
T2 = "2026-08-01T00:00:00Z"


def _seed_entities(facts, ns="nexus"):
    facts.ensure_entity("person:alice", entity_type="person", title="Alice", namespace=ns)
    facts.ensure_entity("project:x", entity_type="project", title="Project X", namespace=ns)
    facts.ensure_entity("project:y", entity_type="project", title="Project Y", namespace=ns)


def test_fact_creation_and_current_query(stores):
    _conn, _graph, facts, _index, _obs = stores
    _seed_entities(facts)
    result = facts.assert_fact(
        "person:alice", "WORKS_ON", "project:x", valid_from=T0, observation_id="obs-1"
    )
    assert result["fact_id"].startswith("fact-")
    current = facts.facts(subject="person:alice")
    assert len(current) == 1
    fact = current[0]
    assert fact["predicate"] == "WORKS_ON"
    assert fact["object"] == "project:x"
    assert fact["valid_from"] == T0
    assert fact["valid_to"] is None
    assert fact["observation_ids"] == ["obs-1"]
    assert fact["evidence"] == "asserted"


def test_reinforcement_multiple_sources(stores):
    _conn, _graph, facts, _index, _obs = stores
    _seed_entities(facts)
    first = facts.assert_fact(
        "person:alice",
        "WORKS_ON",
        "project:x",
        valid_from=T0,
        observation_id="obs-1",
        confidence=0.6,
    )
    second = facts.assert_fact(
        "person:alice",
        "WORKS_ON",
        "project:x",
        valid_from=T0,
        observation_id="obs-2",
        confidence=0.9,
    )
    assert first["fact_id"] == second["fact_id"]
    assert second["reinforced"] is True
    current = facts.facts(subject="person:alice")
    assert len(current) == 1  # не дублируется
    assert sorted(current[0]["observation_ids"]) == ["obs-1", "obs-2"]
    assert current[0]["confidence"] == 0.9  # максимум, не перезапись вниз


def test_supersession_preserves_history(stores):
    _conn, _graph, facts, _index, _obs = stores
    _seed_entities(facts)
    old = facts.assert_fact("person:alice", "WORKS_ON", "project:x", valid_from=T0)
    new = facts.assert_fact(
        "person:alice",
        "WORKS_ON",
        "project:y",
        valid_from=T1,
        supersedes=old["fact_id"],
        observation_id="obs-2",
    )
    assert new["superseded"] is True

    # Текущий срез: только новый факт.
    current = facts.facts(subject="person:alice")
    assert [f["object"] for f in current] == ["project:y"]

    # История не уничтожена: полная выборка содержит оба, старый закрыт.
    full = facts.facts(subject="person:alice", include_closed=True)
    assert len(full) == 2
    old_fact = next(f for f in full if f["object"] == "project:x")
    assert old_fact["valid_to"] == T1
    assert old_fact["superseded_by"] == new["fact_id"]

    # Исторический запрос as_of до supersession видит старый факт.
    past = facts.facts(subject="person:alice", as_of="2026-03-01T00:00:00Z")
    assert [f["object"] for f in past] == ["project:x"]
    # А на момент после — новый.
    now = facts.facts(subject="person:alice", as_of=T2)
    assert [f["object"] for f in now] == ["project:y"]


def test_invalidate_without_replacement(stores):
    _conn, _graph, facts, _index, _obs = stores
    _seed_entities(facts)
    fact = facts.assert_fact("person:alice", "WORKS_ON", "project:x", valid_from=T0)
    assert facts.invalidate_fact(fact["fact_id"], valid_to=T1, observation_id="obs-close")
    assert facts.facts(subject="person:alice") == []  # действующих нет
    closed = facts.facts(subject="person:alice", include_closed=True)
    assert closed[0]["valid_to"] == T1
    assert "obs-close" in closed[0]["observation_ids"]


def test_contradictory_evidence_both_versions_marked(stores):
    _conn, _graph, facts, _index, _obs = stores
    _seed_entities(facts)
    facts.assert_fact("person:alice", "WORKS_ON", "project:x", valid_from=T0)
    facts.assert_fact("person:alice", "WORKS_ON", "project:y", valid_from=T1)
    from platform_memory.graph.facts import FactStore

    got = FactStore.find_conflicts(facts.facts(subject="person:alice"))
    assert all(f.get("conflict") for f in got)
    assert len(got) == 2  # обе версии остаются — не тихое переписывание истории


def test_evidence_lost_hides_fact_from_retrieval(stores):
    _conn, _graph, facts, _index, _obs = stores
    _seed_entities(facts)
    facts.assert_fact(
        "person:alice", "WORKS_ON", "project:x", valid_from=T0, observation_id="obs-only"
    )
    lost = facts.mark_evidence_lost("obs-only")
    assert lost == 1
    assert facts.facts(subject="person:alice") == []  # выпал из retrieval
    kept = facts.facts(subject="person:alice", include_evidence_lost=True)
    assert len(kept) == 1  # но не удалён из графа (аудит)
    assert kept[0]["evidence_lost"] is True


def test_timezone_offset_normalized_in_fact(stores):
    """Adversarial №1: '13:00+03:00' и '10:00Z' — одно утверждение, один fact_id."""
    _conn, _graph, facts, _index, _obs = stores
    _seed_entities(facts)
    a = facts.assert_fact(
        "person:alice", "WORKS_ON", "project:x", valid_from="2026-08-01T13:00:00+03:00"
    )
    b = facts.assert_fact(
        "person:alice", "WORKS_ON", "project:x", valid_from="2026-08-01T10:00:00Z"
    )
    assert a["fact_id"] == b["fact_id"]
    assert b["reinforced"] is True
    # as_of после нормализованного valid_from видит факт.
    got = facts.facts(subject="person:alice", as_of="2026-08-01T11:00:00Z")
    assert got and got[0]["valid_from"] == "2026-08-01T10:00:00Z"


def test_namespace_isolation_of_facts(stores):
    _conn, _graph, facts, _index, _obs = stores
    _seed_entities(facts, ns="bank")
    facts.assert_fact("person:alice", "WORKS_ON", "project:x", namespace="bank", valid_from=T0)
    assert facts.facts(subject="person:alice", namespaces=["bank"])
    assert facts.facts(subject="person:alice", namespaces=["nexus"]) == []

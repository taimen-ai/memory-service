"""Наблюдения до TASK-001056: миграция колонки видимости пишущего и их переобработка.

- ``ensure_schema`` добавляет ``writer_visibility`` с ``lock_timeout``: долгий читатель
  таблицы не ставит приём в очередь за ALTER — повтор, затем
  :class:`SchemaMigrationBusy` (HTTP ``503``);
- ``redrive_legacy_observations`` (``cb redrive-legacy``) — переобработка записей без
  сохранённой видимости с видимостью, явно заявленной оператором (MEM-ADR-019).
"""

from __future__ import annotations

import pytest

from platform_memory.core import db as dbmod
from platform_memory.core.observations import Observation
from platform_memory.observations import store as store_mod
from platform_memory.observations.ingest import process_observations, redrive_legacy_observations
from platform_memory.observations.store import SchemaMigrationBusy
from tests.integration import test_visibility_writes as writes
from tests.integration.test_visibility_reads import NS, _observation

pytestmark = pytest.mark.integration

W1_SCOPE = writes.W1_SCOPE
W2_SCOPE = writes.W2_SCOPE
_VICTIM = [{"assert": "entity", "entity": {"type": "person", "id": "victim", "title": "HIJACKED"}}]


def _drop_writer_column(conn, settings) -> None:
    with conn.cursor() as cur:
        cur.execute(f'ALTER TABLE "{settings.observations_table}" DROP COLUMN writer_visibility')


def _has_writer_column(conn, settings) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.columns WHERE table_name = %s "
            "AND column_name = 'writer_visibility'",
            (settings.observations_table,),
        )
        return cur.fetchone() is not None


@pytest.fixture()
def reader(settings, stores):
    """Соединение-читатель без autocommit: первый SELECT открывает долгую транзакцию."""
    conn = dbmod.connect(settings, autocommit=False)
    conn.commit()  # транзакции ещё нет; commit — на случай, если подключение её откроет
    yield conn
    conn.close()


@pytest.fixture()
def fast_migration(monkeypatch):
    """Короткое ожидание лока миграции; паузы между попытками — через ``on_pause``."""
    pauses: list[float] = []
    monkeypatch.setattr(store_mod.ObservationStore, "MIGRATION_LOCK_TIMEOUT_MS", 200)
    monkeypatch.setattr(store_mod.ObservationStore, "MIGRATION_ATTEMPTS", 2)
    monkeypatch.setattr(store_mod.time, "sleep", pauses.append)
    return pauses


def test_migration_gives_up_on_long_reader(settings, stores, reader, fast_migration):
    """Читатель держит таблицу — ALTER не ждёт бесконечно, а отказывает понятно."""
    conn, *_, obs_store = stores
    _drop_writer_column(conn, settings)
    before = conn.execute("SHOW lock_timeout").fetchone()[0]
    reader.execute(f'SELECT count(*) FROM "{settings.observations_table}"')

    with pytest.raises(SchemaMigrationBusy, match="writer_visibility"):
        obs_store.ensure_schema()
    assert len(fast_migration) == 1  # одна пауза между двумя попытками
    # Лок не остался за миграцией: читатель и другие запросы идут дальше.
    reader.execute(f'SELECT count(*) FROM "{settings.observations_table}"').fetchone()
    assert not _has_writer_column(conn, settings)
    # lock_timeout — только на время миграции, не на всё соединение.
    assert conn.execute("SHOW lock_timeout").fetchone()[0] == before


def test_migration_retries_after_reader_finishes(
    settings, stores, reader, fast_migration, monkeypatch
):
    """Читатель завершился между попытками — миграция проходит повтором."""
    conn, *_, obs_store = stores
    _drop_writer_column(conn, settings)
    reader.execute(f'SELECT count(*) FROM "{settings.observations_table}"')

    def pause(seconds: float) -> None:
        fast_migration.append(seconds)
        reader.commit()  # долгий читатель закончил работу

    monkeypatch.setattr(store_mod.time, "sleep", pause)
    obs_store.ensure_schema()
    assert _has_writer_column(conn, settings)
    assert len(fast_migration) == 1


def _hold_migration_lock(settings, obs_store):
    """Другой сеанс взял advisory-лок миграции — он и пробует ALTER."""
    other = dbmod.connect(settings, autocommit=True)
    other.execute("SELECT pg_advisory_lock(hashtext(%s))", (obs_store.migration_lock_name,))
    return other


def test_only_lock_holder_waits_for_table(settings, stores, fast_migration, monkeypatch):
    """Пока ALTER пробует другой сеанс, этот не встаёт в очередь к таблице."""
    conn, *_, obs_store = stores
    _drop_writer_column(conn, settings)
    other = _hold_migration_lock(settings, obs_store)
    try:
        # Лок таблицы свободен, но ALTER без advisory-лока не пробуется.
        with pytest.raises(SchemaMigrationBusy, match="writer_visibility"):
            obs_store.ensure_schema()
        assert not _has_writer_column(conn, settings)

        def pause(seconds: float) -> None:
            fast_migration.append(seconds)
            other.execute(
                "SELECT pg_advisory_unlock(hashtext(%s))", (obs_store.migration_lock_name,)
            )

        monkeypatch.setattr(store_mod.time, "sleep", pause)
        obs_store.ensure_schema()  # держатель отпустил лок — миграция проходит
        assert _has_writer_column(conn, settings)
    finally:
        other.close()


def test_migration_done_by_lock_holder_is_picked_up(settings, stores, fast_migration, monkeypatch):
    """Колонку добавил сеанс с локом — остальные видят её в каталоге после паузы."""
    conn, *_, obs_store = stores
    _drop_writer_column(conn, settings)
    other = _hold_migration_lock(settings, obs_store)
    try:

        def pause(seconds: float) -> None:
            fast_migration.append(seconds)
            other.execute(
                f'ALTER TABLE "{settings.observations_table}" '
                "ADD COLUMN IF NOT EXISTS writer_visibility jsonb"
            )

        monkeypatch.setattr(store_mod.time, "sleep", pause)
        obs_store.ensure_schema()
        assert _has_writer_column(conn, settings)
        assert len(fast_migration) == 1
    finally:
        other.close()


# --- переобработка наблюдений без сохранённой видимости ------------------------------


def _legacy(conn, settings, obs_store, ext_id: str, scopes: list[str]) -> str:
    """Наблюдение, принятое до TASK-001056: ``writer_visibility`` NULL, после redrive — failed."""
    obs = Observation.from_payload(
        {**_observation(ext_id, scopes, f"OBS-{ext_id}"), "assertions": _VICTIM}, namespace=NS
    )
    oid, _dup = obs_store.insert(obs)
    with conn.cursor() as cur:
        cur.execute(
            f'UPDATE "{settings.observations_table}" SET writer_visibility = NULL '
            "WHERE observation_id = %s",
            (oid,),
        )
    return oid


def _writer_visibility(conn, settings, oid: str):
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT writer_visibility FROM "{settings.observations_table}" '
            "WHERE observation_id = %s",
            (oid,),
        )
        return cur.fetchone()[0]


@pytest.mark.parametrize(
    ("writer", "ok"),
    [
        ([W1_SCOPE], True),  # оператор заявил воркспейс пишущего
        (None, True),  # service-режим без ограничения
        ([W2_SCOPE], False),  # scope записи вне заявленной видимости — не трогается
        ([], False),
    ],
)
def test_redrive_legacy_with_declared_visibility(settings, stores, writer, ok):
    conn, graph, _facts, _index, obs_store = stores
    graph.upsert_node(writes._entity("person:victim", [W1_SCOPE]))
    oid = _legacy(conn, settings, obs_store, "legacy", [W1_SCOPE])
    # Обычный redrive: записи без видимости — уровень namespace, приватное — failed.
    assert process_observations(settings, namespace=NS)["statuses"] == {"failed": 1}

    report = redrive_legacy_observations(settings, writer_scopes=writer, namespace=NS)

    record = obs_store.get(oid, namespaces=[NS])
    node = graph.get_node("person:victim", namespaces=[NS])
    if ok:
        assert report["stamped"] == 1 and report["skipped"] == []
        assert report["statuses"] == {"processed": 1}
        assert record.status == "processed"
        assert node["title"] == "HIJACKED"
        assert node["props"]["scopes"] == [W1_SCOPE]
        stored = _writer_visibility(conn, settings, oid)
        # Заявленная оператором видимость отличима от записанной при приёме.
        assert stored["scopes"] == writer
        assert stored["declared_by"] == "operator" and stored["actor"] == "operator"
        assert stored["declared_at"]
        assert report["candidates"][0]["source"]["system"] == "tracker"
    else:
        assert report["stamped"] == 0
        assert report["skipped"] == [{"observation_id": oid, "scope": W1_SCOPE}]
        assert record.status == "failed"
        assert node["title"] != "HIJACKED"
        assert _writer_visibility(conn, settings, oid) is None  # можно повторить


def test_redrive_legacy_leaves_recorded_visibility_alone(settings, stores):
    """Записи с сохранённой видимостью пишущего оператор не переписывает."""
    conn, graph, _facts, _index, obs_store = stores
    graph.upsert_node(writes._entity("person:victim", [W1_SCOPE]))
    obs = Observation.from_payload(
        {**_observation("modern", [W1_SCOPE], "OBS"), "assertions": _VICTIM}, namespace=NS
    )
    oid, _dup = obs_store.insert(obs, writer_scopes=[W2_SCOPE])

    report = redrive_legacy_observations(settings, writer_scopes=None, namespace=NS)
    assert report == {
        "stamped": 0,
        "skipped": [],
        "candidates": [],
        "processed": 0,
        "statuses": {},
    }
    assert _writer_visibility(conn, settings, oid) == {"scopes": [W2_SCOPE]}
    assert graph.get_node("person:victim", namespaces=[NS])["title"] != "HIJACKED"


def test_redrive_legacy_only_selected_ids(settings, stores):
    conn, _graph, _facts, _index, obs_store = stores
    first = _legacy(conn, settings, obs_store, "a", [])
    second = _legacy(conn, settings, obs_store, "b", [])

    report = redrive_legacy_observations(
        settings, writer_scopes=[W1_SCOPE], namespace=NS, observation_ids=[second]
    )
    assert report["stamped"] == 1
    assert _writer_visibility(conn, settings, first) is None
    assert _writer_visibility(conn, settings, second)["scopes"] == [W1_SCOPE]


def test_redrive_legacy_writes_audit_event(settings, stores):
    """Заявление видимости — событие аудита в трейсе наблюдения, со scopes записи."""
    conn, graph, _facts, _index, obs_store = stores
    oid = _legacy(conn, settings, obs_store, "audited", [W1_SCOPE])

    redrive_legacy_observations(settings, writer_scopes=[W1_SCOPE], namespace=NS, actor="alice")

    events = graph.trace_subgraph(oid, namespace=NS)["events"]
    assert [e["props"]["action"] for e in events] == ["observation_writer_visibility_declared"]
    event = events[0]["props"]
    assert event["actor"] == "alice" and event["actor_kind"] == "operator"
    payload = event["payload"]
    assert payload["writer_scopes"] == [W1_SCOPE]
    assert payload["declared_by"] == "operator"
    assert payload["source"]["system"] == "tracker"
    assert payload["scopes"] == [W1_SCOPE]
    assert event["ts"] == _writer_visibility(conn, settings, oid)["declared_at"]
    # Событие видно только видящему наблюдение (MEM-ADR-019).
    assert graph.trace_subgraph(oid, namespace=NS, allowed_scopes=[W2_SCOPE])["events"] == []


def test_redrive_legacy_dry_run_writes_nothing(settings, stores):
    """--dry-run: показывает источник записей, но ни метки, ни проекции, ни аудита."""
    conn, graph, _facts, _index, obs_store = stores
    graph.upsert_node(writes._entity("person:victim", [W1_SCOPE]))
    oid = _legacy(conn, settings, obs_store, "dry", [W1_SCOPE])

    report = redrive_legacy_observations(
        settings, writer_scopes=[W1_SCOPE], namespace=NS, dry_run=True
    )
    assert report["stamped"] == 0 and report["statuses"] == {}
    assert [c["observation_id"] for c in report["candidates"]] == [oid]
    assert report["candidates"][0]["source"]["system"] == "tracker"
    assert _writer_visibility(conn, settings, oid) is None
    assert graph.get_node("person:victim", namespaces=[NS])["title"] != "HIJACKED"
    assert graph.trace_subgraph(oid, namespace=NS)["events"] == []

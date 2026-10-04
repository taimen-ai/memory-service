"""Ingest наблюдений: приём, идемпотентность, детерминированная проекция (ADR-016).

Конвейер (§29 ТЗ):

```
retain_observation
      │ вставка raw observation (идемпотентно по source identity)
      ├── structured assertions ──► детерминированная проекция (без LLM):
      │       entity  → узел графа (origin='agent')
      │       fact    → temporal-ребро FactStore (evidence='asserted')
      │       text    → узел + чанк векторного индекса (цитируемый источник)
      └── unstructured content ──► FTS/trigram observations (сразу же находим);
                                   LLM-extraction — только явно (consolidation)
```

ACK не ждёт LLM. Ошибка проекции не теряет raw observation: статус
``partially_processed``/``failed`` + повторная обработка (redrive) идемпотентна.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from typing import Any

import psycopg

from platform_memory.core import db as dbmod
from platform_memory.core.config import Settings
from platform_memory.core.models import Chunk
from platform_memory.core.namespaces import resolve_namespace
from platform_memory.core.scopes import (
    MAX_ALLOWED_SCOPES,
    ForeignObjectError,
    audit_scopes,
    check_write_scopes,
    has_visibility_scope,
    is_visible,
    meta_with_scopes,
    node_scopes,
    resolve_scopes,
)
from platform_memory.core.observations import (
    STATUS_FAILED,
    STATUS_PARTIAL,
    STATUS_PROCESSED,
    Observation,
    entity_ref_key,
    entity_ref_type,
)
from platform_memory.graph.facts import FactStore
from platform_memory.graph.store import GraphStore
from platform_memory.domain.registry import attach_kind_guard
from platform_memory.index import VectorIndex, build_embedder
from platform_memory.index.embeddings import Embedder
from platform_memory.observations.store import ObservationRecord, ObservationStore

ASSERT_KINDS = frozenset({"entity", "fact", "text"})

# Локальные алиасы (каноника — core.observations, там же ими пользуется compiler).
_entity_key = entity_ref_key
_entity_type = entity_ref_type


class AssertionProjector:
    """Детерминированная проекция assertions одного наблюдения в граф и индекс."""

    def __init__(
        self,
        graph: GraphStore,
        facts: FactStore,
        index: VectorIndex,
        embedder: Embedder | None,
        settings: Settings,
        allowed_scopes: Sequence[str] | None = None,
        redrive: bool = False,
    ):
        """Собрать проектор поверх открытых сторов (соединение — вызывающего).

        ``allowed_scopes`` — видимость пишущего (None — без ограничения): узел или
        чанк ключа вне неё assertion не перезаписывает, ошибка assertion'а
        (MEM-ADR-019). При ``redrive`` это видимость того, кто запустил redrive, а
        пишет он с видимостью пишущего, сохранённой в записи, суженной ею
        (:func:`redrive_scopes`).
        ``warnings`` последней проекции — assertions, чьё содержимое легло в общий
        узел namespace, а не в узел со scopes наблюдения.
        """
        self.graph = graph
        self.facts = facts
        self.index = index
        self.embedder = embedder
        self.settings = settings
        self.allowed_scopes = allowed_scopes
        self.redrive = redrive
        self.warnings: list[str] = []

    def project(self, record: ObservationRecord) -> list[str]:
        """Спроецировать assertions наблюдения; вернуть список ошибок (пусто = ок).

        Каждый assertion обрабатывается независимо: ошибка одного не блокирует
        остальные (частичная обработка фиксируется статусом наблюдения).
        Повторный вызов идемпотентен: upsert узлов, reinforce фактов, upsert
        чанков по стабильным ключам.
        """
        errors: list[str] = []
        self.warnings = []
        source_path = str(record.provenance.get("uri") or "") or (
            f"memory:observation/{record.observation_id}"
        )
        allowed = self.allowed_scopes
        if self.redrive:
            allowed = redrive_scopes(record.writer_visibility, self.allowed_scopes)
        for i, assertion in enumerate(record.assertions):
            try:
                kind = str(assertion.get("assert", "")).strip()
                if kind not in ASSERT_KINDS:
                    raise ValueError(f"Неизвестный вид assertion {kind!r}")
                if kind == "entity":
                    key = self._project_entity(assertion, record, source_path, allowed)
                    self._warn_if_shared(i, key, record)
                elif kind == "fact":
                    self._project_fact(assertion, record, source_path, allowed)
                else:
                    key = self._project_text(assertion, record, source_path, i, allowed)
                    self._warn_if_shared(i, key, record)
            except Exception as exc:  # ошибка одного assertion не теряет остальные
                errors.append(f"assertion[{i}]: {exc}")
        return errors

    def _warn_if_shared(self, i: int, key: str, record: ObservationRecord) -> None:
        """Предупредить, если приватное наблюдение записало содержимое в общий узел.

        Узел без scope видимости им и остаётся (``merge_scopes``): его содержимое
        видит любой, кто читает namespace, а не только воркспейс наблюдения.
        """
        if not has_visibility_scope(record.scopes):
            return
        node = self.graph.get_node(key, namespaces=[record.namespace]) or {}
        if not has_visibility_scope(node_scopes(node)):
            self.warnings.append(
                f"assertion[{i}]: узел {key} общий для namespace — "
                "его содержимое видно без scopes наблюдения"
            )

    def _project_entity(
        self,
        assertion: dict,
        record: ObservationRecord,
        source_path: str,
        allowed: Sequence[str] | None,
    ) -> str:
        entity = assertion.get("entity") or {}
        key = _entity_key(entity)
        self.facts.ensure_entity(
            key,
            entity_type=_entity_type(entity),
            title=str(entity.get("title", "")) or key,
            properties=dict(entity.get("properties") or {}),
            namespace=record.namespace,
            source_path=source_path,
            scopes=record.scopes,
            allowed_scopes=allowed,
        )
        return key

    def _project_fact(
        self,
        assertion: dict,
        record: ObservationRecord,
        source_path: str,
        allowed: Sequence[str] | None,
    ):
        fact = assertion.get("fact") or {}
        subject = _entity_key(fact.get("subject"))
        obj = _entity_key(fact.get("object"))
        predicate = str(fact.get("predicate", "")).strip()
        if not predicate:
            raise ValueError("fact без predicate")
        # Концы факта создаются placeholder'ами — проекция не зависит от порядка.
        for ref, key in ((fact.get("subject"), subject), (fact.get("object"), obj)):
            if self.graph.get_node(key, namespaces=[record.namespace]) is None:
                self.facts.ensure_entity(
                    key,
                    entity_type=_entity_type(ref),
                    namespace=record.namespace,
                    source_path=source_path,
                    scopes=record.scopes,
                    allowed_scopes=allowed,
                )
        self.facts.assert_fact(
            subject,
            predicate,
            obj,
            namespace=record.namespace,
            observed_at=record.occurred_at,
            valid_from=str(fact.get("valid_from", "") or ""),
            valid_to=fact.get("valid_to"),
            evidence="asserted",
            confidence=float(fact.get("confidence", 1.0)),
            observation_id=record.observation_id,
            supersedes=str(fact.get("supersedes", "") or ""),
            scopes=record.scopes,
            source_path=source_path,
            allowed_scopes=allowed,
        )

    def _project_text(
        self,
        assertion: dict,
        record: ObservationRecord,
        source_path: str,
        i: int,
        allowed: Sequence[str] | None,
    ) -> str:
        text = assertion.get("text") or {}
        content = str(text.get("content", "")).strip()
        if not content:
            raise ValueError("text без content")
        node_key = str(text.get("key", "") or f"{record.observation_id}:text:{i}")
        title = str(text.get("title", "")) or (record.kind or "observation")
        node_type = str(text.get("type", "") or "note")
        chunk = Chunk(
            node_key=node_key,
            text=content,
            source_path=source_path,
            title=title,
            namespace=record.namespace,
        )
        # Эмбеддинг — до лока ключа: провайдер может быть сетевым.
        emb = self.embedder.embed_one(chunk.embedding_input()) if self.embedder else None
        # Проверка чанков и узла ключа и запись — под одним локом ключа (MEM-ADR-019).
        with self.index.key_write_lock(node_key, record.namespace):
            chunk_scopes = self.index.guard_chunk_write(node_key, record.namespace, allowed)
            self.facts.ensure_entity(
                node_key,
                entity_type=node_type,
                title=title,
                properties={"content": content, "observation_id": record.observation_id},
                namespace=record.namespace,
                source_path=source_path,
                scopes=record.scopes,  # Контентный узел текста — не сущность домена: вид не проверяется.
                check_kind=False,
                allowed_scopes=allowed,
            )
            if emb is None:
                raise ValueError("эмбеддер недоступен — чанк не проиндексирован")
            written = self.graph.get_node(node_key, namespaces=[record.namespace]) or {}
            chunk.meta = meta_with_scopes(
                {"observation_id": record.observation_id},
                record.scopes,
                node_scopes(written),
                chunk_scopes,
            )
            self.index.add_chunks([chunk], [emb])
        return node_key


def redrive_scopes(
    writer_visibility: dict[str, Any] | None, caller: Sequence[str] | None
) -> list[str] | None:
    """Видимость повторной проекции наблюдения (MEM-ADR-019).

    Берётся видимость пишущего, сохранённая при приёме (``{"scopes": None}`` — без
    ограничения), а не scopes самой записи: наблюдения до TASK-001052 принимались без
    проверки scopes против пишущего. Запись без сохранённой видимости (до
    TASK-001056) — уровень namespace (``[]``): перезаписывается только общее, а
    приватные assertions такой записи получают ошибку. Видимость запустившего
    redrive (``caller``, None — без ограничения) сужает результат: redrive не видит
    больше ни пишущего, ни запустившего.
    """
    raw = writer_visibility.get("scopes", []) if isinstance(writer_visibility, dict) else []
    if raw is None:
        writer = None
    elif isinstance(raw, list):
        writer = [s for s in raw if isinstance(s, str)]
    else:
        writer = []  # повреждённое значение — как запись без видимости
    if caller is None:
        return writer
    if writer is None:
        return list(caller)
    keep = {str(c) for c in caller}
    return [s for s in writer if s in keep]


def _process_record(
    stores: tuple[ObservationStore, AssertionProjector],
    record: ObservationRecord,
) -> str:
    """Обработать одно наблюдение: проекция + статус. Вернуть итоговый статус."""
    obs_store, projector = stores
    if not record.assertions:
        status = STATUS_PROCESSED
        obs_store.set_status(record.observation_id, status, namespace=record.namespace)
        return status
    errors = projector.project(record)
    if not errors:
        status = STATUS_PROCESSED
    elif len(errors) < len(record.assertions):
        status = STATUS_PARTIAL
    else:
        status = STATUS_FAILED
    obs_store.set_status(
        record.observation_id, status, detail="; ".join(errors)[:2000], namespace=record.namespace
    )
    return status


def _open_stores(
    settings: Settings,
    conn: psycopg.Connection,
    embedder: Embedder | None,
    *,
    allowed_scopes: Sequence[str] | None = None,
    redrive: bool = False,
) -> tuple[ObservationStore, AssertionProjector]:
    graph = GraphStore(conn, settings.graph_name, settings.default_namespace)
    attach_kind_guard(graph, settings)  # строгий режим видов namespace (MEM-ADR-020)
    # Граф обязан существовать до проекции assertions: первый вызов API может
    # прийти раньше init-db (дёшево: одна проверка каталога + create при нужде).
    dbmod.ensure_graph(conn, graph.graph)
    facts = FactStore(graph)
    index = VectorIndex(
        conn, settings.chunks_table, settings.embedding_dim, settings.default_namespace
    )
    obs_store = ObservationStore(conn, settings.observations_table, settings.default_namespace)
    projector = AssertionProjector(
        graph, facts, index, embedder, settings, allowed_scopes=allowed_scopes, redrive=redrive
    )
    return obs_store, projector


def retain_observation(
    settings: Settings,
    observation: Observation | dict[str, Any],
    *,
    namespace: str = "",
    embedder: Embedder | None = None,
    conn: psycopg.Connection | None = None,
    allowed_scopes: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Принять одно наблюдение: вставка (идемпотентно) + детерминированная проекция.

    Возвращает ``{observation_id, duplicate, status}``. Дубликат по source
    identity не перезаписывает существующую запись и не проецируется повторно.
    ``allowed_scopes`` — видимость пишущего (None — без ограничения): scopes
    видимости наблюдения — только из неё (иначе :class:`ForeignObjectError` до
    записи), assertion не перезаписывает чужой узел, чанк или факт (MEM-ADR-019);
    она же сохраняется в записи для redrive. Повтор source identity наблюдения,
    невидимого пишущему, — :class:`ForeignObjectError` без id и статуса чужой
    записи. Assertions, записавшие содержимое в общий узел, — в ``warnings``.
    """
    ns = resolve_namespace(namespace, settings.default_namespace)
    if isinstance(observation, dict):
        obs = Observation.from_payload(observation, namespace=ns)
    else:
        obs = observation
        obs.namespace = ns
        obs.canonicalize()
    check_write_scopes(obs.scopes, allowed_scopes)

    own_conn = conn is None
    conn = conn or dbmod.connect(settings, autocommit=True)
    try:
        if embedder is None and _needs_embedder(obs):
            embedder = build_embedder(settings)
        obs_store, projector = _open_stores(settings, conn, embedder, allowed_scopes=allowed_scopes)
        obs_store.ensure_schema()
        oid, duplicate = obs_store.insert(obs, writer_scopes=allowed_scopes)
        if duplicate:
            existing = obs_store.get(oid, namespaces=[ns])
            if existing is not None and not is_visible(existing.scopes, allowed_scopes):
                # Чужая запись с той же source identity: ни id, ни статус не выдаём.
                raise ForeignObjectError(_source_identity(obs))
            return {
                "observation_id": oid,
                "duplicate": True,
                "status": existing.status if existing else "received",
            }
        record = obs_store.get(oid, namespaces=[ns])
        status = _process_record((obs_store, projector), record)
        result: dict[str, Any] = {"observation_id": oid, "duplicate": False, "status": status}
        if projector.warnings:
            result["warnings"] = list(projector.warnings)
        return result
    finally:
        if own_conn:
            conn.close()


def _source_identity(obs: Observation) -> str:
    """Source identity наблюдения из полей самого пишущего (для отказа без id)."""
    if obs.external_id:
        return "/".join(p for p in (obs.source_system, obs.source_stream, obs.external_id) if p)
    return "observation"


def _needs_embedder(obs: Observation) -> bool:
    return any(str(a.get("assert")) == "text" for a in obs.assertions)


def retain_observations(
    settings: Settings,
    observations: list[Observation | dict[str, Any]],
    *,
    namespace: str = "",
    embedder: Embedder | None = None,
    allowed_scopes: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Batch-приём наблюдений с по-элементными результатами (partial errors).

    Каждый элемент обрабатывается в собственной операции (батч — НЕ одна
    гигантская транзакция): ошибка валидации одного не откатывает остальные.
    Размер батча ограничен ``CB_OBSERVATIONS_MAX_BATCH``.
    """
    if len(observations) > settings.observations_max_batch:
        raise ValueError(
            f"Батч больше лимита: {len(observations)} > {settings.observations_max_batch}"
        )
    conn = dbmod.connect(settings, autocommit=True)
    results: list[dict[str, Any]] = []
    accepted = duplicates = failed = 0
    try:
        if embedder is None:
            needs = any(
                _needs_embedder(o) for o in observations if isinstance(o, Observation)
            ) or any(
                str(a.get("assert")) == "text"
                for o in observations
                if isinstance(o, dict)
                for a in (o.get("assertions") or [])
            )
            embedder = build_embedder(settings) if needs else None
        for i, payload in enumerate(observations):
            try:
                result = retain_observation(
                    settings,
                    payload,
                    namespace=namespace,
                    embedder=embedder,
                    conn=conn,
                    allowed_scopes=allowed_scopes,
                )
                results.append({"index": i, **result})
                if result["duplicate"]:
                    duplicates += 1
                else:
                    accepted += 1
            except Exception as exc:  # ошибка элемента не роняет батч
                failed += 1
                results.append({"index": i, "error": str(exc)})
    finally:
        conn.close()
    return {
        "results": results,
        "accepted": accepted,
        "duplicates": duplicates,
        "failed": failed,
    }


def process_observations(
    settings: Settings,
    *,
    namespace: str = "",
    limit: int = 100,
    embedder: Embedder | None = None,
    allowed_scopes: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Redrive: повторно обработать наблюдения в статусах received/partial/failed.

    Безопасно и идемпотентно (upsert/reinforce по стабильным ключам) — это
    dead-letter-механизм ADR-016 §30. Перезаписать существующий узел, чанк или факт
    redrive может, только если тот виден пишущему наблюдения (видимость сохранена
    в записи) и запустившему redrive (:func:`redrive_scopes`, MEM-ADR-019).
    ``allowed_scopes`` — видимость запустившего (None — без ограничения): он
    переигрывает и считает только видимые ему наблюдения.
    """
    ns = resolve_namespace(namespace, settings.default_namespace)
    conn = dbmod.connect(settings, autocommit=True)
    counts: dict[str, int] = {}
    try:
        if embedder is None:
            embedder = build_embedder(settings)
        obs_store, projector = _open_stores(
            settings, conn, embedder, allowed_scopes=allowed_scopes, redrive=True
        )
        if not obs_store.table_exists():
            return {"processed": 0, "statuses": {}}
        obs_store.ensure_schema()  # колонка writer_visibility у таблиц до TASK-001056
        records = obs_store.list_unprocessed([ns], limit=limit, allowed_scopes=allowed_scopes)
        for record in records:
            status = _process_record((obs_store, projector), record)
            counts[status] = counts.get(status, 0) + 1
        return {"processed": len(records), "statuses": counts}
    finally:
        conn.close()


def _record_source(record: ObservationRecord) -> dict[str, str]:
    return {
        "system": record.source_system,
        "stream": record.source_stream,
        "external_id": record.external_id,
    }


def redrive_legacy_observations(
    settings: Settings,
    *,
    writer_scopes: Sequence[str] | None,
    namespace: str = "",
    observation_ids: Sequence[str] = (),
    limit: int = 500,
    actor: str = "operator",
    dry_run: bool = False,
    embedder: Embedder | None = None,
) -> dict[str, Any]:
    """Переобработать наблюдения до TASK-001056 с видимостью, заявленной оператором.

    Такие записи не несут видимости пишущего, и обычный redrive проецирует их на
    уровне namespace: приватные assertions навсегда ``failed`` (MEM-ADR-019).
    Оператор (CLI, полный допуск к БД) явно заявляет ``writer_scopes`` — видимость
    пишущего (None — без ограничения). Она сохраняется в записи с меткой решения
    (``declared_by: operator``, ``declared_at``, ``actor``) — заявленную видимость
    не спутать с записанной при приёме; повторный redrive пишет с ней же. На каждую
    запись — событие аудита ``observation_writer_visibility_declared`` (трейс —
    ``observation_id``, scopes наблюдения), затем запись переобрабатывается.
    Наблюдение со scope видимости вне заявленной видимости не трогается и
    попадает в ``skipped``: заявить воркспейс записи может только тот, чья
    видимость его включает, — как ``check_write_scopes`` при приёме.

    ``candidates`` — источник и scopes каждой подходящей записи: оператор сверяет
    ``source.system`` до заявления видимости (заявление переигрывает запись от
    имени заявленного пишущего). ``dry_run`` — только ``candidates``/``skipped``,
    без записи.
    """
    ns = resolve_namespace(namespace, settings.default_namespace)
    writer = (
        None if writer_scopes is None else resolve_scopes(writer_scopes, limit=MAX_ALLOWED_SCOPES)
    )
    conn = dbmod.connect(settings, autocommit=True)
    counts: dict[str, int] = {}
    skipped: list[dict[str, str]] = []
    candidates: list[dict[str, Any]] = []
    stamped = 0
    try:
        obs_store = ObservationStore(conn, settings.observations_table, settings.default_namespace)
        records: list[ObservationRecord] = []
        if obs_store.table_exists():
            obs_store.ensure_schema()
            records = obs_store.list_legacy_unprocessed(
                [ns], observation_ids=observation_ids, limit=limit
            )
        projector = None
        if records and not dry_run:
            if embedder is None:
                embedder = build_embedder(settings)
            _, projector = _open_stores(settings, conn, embedder, allowed_scopes=None, redrive=True)
        for record in records:
            try:
                check_write_scopes(record.scopes, writer)
            except ForeignObjectError as exc:
                skipped.append({"observation_id": record.observation_id, "scope": exc.natural_key})
                continue
            source = _record_source(record)
            candidates.append(
                {
                    "observation_id": record.observation_id,
                    "source": source,
                    "scopes": list(record.scopes),
                }
            )
            if projector is None:
                continue
            declared_at = dt.datetime.now(dt.UTC).isoformat()
            mark = {"declared_by": "operator", "declared_at": declared_at, "actor": actor}
            if not obs_store.set_legacy_writer_visibility(
                record.observation_id, writer, namespace=record.namespace, mark=mark
            ):
                continue  # видимость уже записана параллельным приёмом/оператором
            stamped += 1
            # След решения — до проекции: сбой проекции не оставит заявление без аудита.
            projector.graph.record_audit(
                record.observation_id,
                actor,
                "observation_writer_visibility_declared",
                payload={
                    "observation_id": record.observation_id,
                    "writer_scopes": writer,
                    "declared_by": "operator",
                    "source": source,
                    # scopes наблюдения — чтобы трейс не показал событие чужому (MEM-ADR-019)
                    **audit_scopes(record.scopes),
                },
                ts=declared_at,
                actor_kind="operator",
                namespace=record.namespace,
            )
            record.writer_visibility = {"scopes": writer, **mark}
            status = _process_record((obs_store, projector), record)
            counts[status] = counts.get(status, 0) + 1
        return {
            "stamped": stamped,
            "skipped": skipped,
            "candidates": candidates,
            "processed": stamped,
            "statuses": counts,
        }
    finally:
        conn.close()

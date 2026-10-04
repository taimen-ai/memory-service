"""ObservationStore — append-only журнал наблюдений в PostgreSQL (ADR-016).

Реляционная таблица (НЕ граф): уникальность source identity, статусы обработки
и батчи — сильные стороны Postgres. Immutable по содержимому: после вставки
меняются только status/status_detail/processed_at (и redaction — ADR-016 §8).

Retrieval-поверхность: русский FTS по content + trigram-совпадение точных
идентификаторов (pg_trgm; при отсутствии прав на CREATE EXTENSION деградирует
до последовательного ILIKE — корректно, но медленнее).
"""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from platform_memory.core.namespaces import resolve_namespace, resolve_namespaces
from platform_memory.core.observations import (
    STATUS_RECEIVED,
    STATUS_REDACTED,
    STATUSES,
    Observation,
)
from platform_memory.core.scopes import observation_visibility_sql, resolve_scopes

# Документ FTS: kind + content (русская конфигурация, как у чанков).
_FTS_DOC = "to_tsvector('russian', coalesce(kind,'') || ' ' || content)"
_FTS_QUERY = (
    "to_tsquery('russian', array_to_string(tsvector_to_array(to_tsvector('russian', %s)), ' | '))"
)


@dataclass(slots=True)
class ObservationRecord:
    """Сохранённое наблюдение: каноническая форма + серверные поля."""

    observation_id: str
    namespace: str
    source_system: str = ""
    source_stream: str = ""
    external_id: str = ""
    kind: str = ""
    occurred_at: str = ""
    ingested_at: str = ""
    actor: dict[str, str] | None = None
    subject: dict[str, str] | None = None
    scopes: list[str] = field(default_factory=list)
    content: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    assertions: list[dict[str, Any]] = field(default_factory=list)
    provenance: dict[str, Any] = field(default_factory=dict)
    content_hash: str = ""
    status: str = STATUS_RECEIVED
    status_detail: str = ""
    score: float = 0.0  # релевантность при поиске (не хранится)
    # Видимость пишущего на момент приёма (MEM-ADR-019): {"scopes": [...]} или
    # {"scopes": None} — без ограничения; None — не записана (наблюдение до
    # TASK-001056). Читает только redrive; в API-ответы не выдаётся — это
    # членство пишущего в воркспейсах, а не свойство наблюдения.
    writer_visibility: dict[str, Any] | None = None

    def to_payload(self) -> dict[str, Any]:
        """Представление для API-ответов (без внутреннего PK)."""
        return {
            "observation_id": self.observation_id,
            "namespace": self.namespace,
            "source": {
                "system": self.source_system,
                "stream": self.source_stream,
                "external_id": self.external_id,
            },
            "kind": self.kind,
            "occurred_at": self.occurred_at,
            "ingested_at": self.ingested_at,
            "actor": self.actor,
            "subject": self.subject,
            "scopes": list(self.scopes),
            "content": self.content,
            "data": self.data,
            "assertions": self.assertions,
            "provenance": self.provenance,
            "status": self.status,
            "status_detail": self.status_detail,
        }


_COLUMNS = (
    "observation_id, namespace, source_system, source_stream, external_id, kind, "
    "occurred_at, ingested_at::text, actor, subject, scopes, content, data, assertions, "
    "provenance, content_hash, status, status_detail"
)


class SchemaMigrationBusy(RuntimeError):
    """Аддитивная миграция таблицы наблюдений не дождалась лока — повторить позже."""


class ObservationStore:
    """Хранилище наблюдений: идемпотентная вставка, статусы, lexical retrieval."""

    # Ожидание лока аддитивной миграции (``_add_writer_visibility``). Пока ALTER
    # ждёт лок, за ним стоят все новые запросы к таблице: ожидание короткое (≤ 1 с),
    # а попыток много, и между ними — пауза, в которую очередь рассасывается.
    MIGRATION_LOCK_TIMEOUT_MS = 500
    MIGRATION_ATTEMPTS = 10
    MIGRATION_RETRY_PAUSE = 1.0  # секунд

    def __init__(self, conn: psycopg.Connection, table: str, default_namespace: str = ""):
        """Инициализировать стор на соединении, таблице и default-namespace."""
        self.conn = conn
        self.table = table
        self.default_namespace = resolve_namespace(default_namespace)

    @property
    def _tbl(self) -> sql.Identifier:
        return sql.Identifier("public", self.table)

    # --- схема ---

    def ensure_schema(self) -> None:
        """Создать таблицу наблюдений и индексы (идемпотентно, аддитивно).

        pg_trgm ставится по возможности: без прав на CREATE EXTENSION стор
        работает, identifier-поиск деградирует до ILIKE без индекса.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {tbl} (
                        id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        observation_id text NOT NULL,
                        namespace      text NOT NULL,
                        source_system  text NOT NULL DEFAULT '',
                        source_stream  text NOT NULL DEFAULT '',
                        external_id    text NOT NULL DEFAULT '',
                        kind           text NOT NULL DEFAULT '',
                        occurred_at    text NOT NULL DEFAULT '',
                        ingested_at    timestamptz NOT NULL DEFAULT now(),
                        actor          jsonb,
                        subject        jsonb,
                        scopes         jsonb NOT NULL DEFAULT '[]'::jsonb,
                        content        text NOT NULL DEFAULT '',
                        data           jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                        assertions     jsonb NOT NULL DEFAULT '[]'::jsonb,
                        provenance     jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                        content_hash   text NOT NULL DEFAULT '',
                        status         text NOT NULL DEFAULT 'received',
                        status_detail  text NOT NULL DEFAULT '',
                        processed_at   timestamptz,
                        writer_visibility jsonb
                    )
                    """
                ).format(tbl=self._tbl)
            )
            # Аддитивная миграция таблиц до TASK-001056: у прежних строк — NULL.
            # ALTER берёт эксклюзивный лок даже с IF NOT EXISTS, а ensure_schema
            # зовётся на каждый приём — поэтому сначала дешёвая проверка каталога.
            if not self._has_writer_visibility():
                self._add_writer_visibility()
            cur.execute(
                sql.SQL(
                    "CREATE UNIQUE INDEX IF NOT EXISTS {ix} ON {tbl} (namespace, observation_id)"
                ).format(ix=sql.Identifier(f"{self.table}_ns_obs_key"), tbl=self._tbl)
            )
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {ix} ON {tbl} (namespace, kind)").format(
                    ix=sql.Identifier(f"{self.table}_ns_kind"), tbl=self._tbl
                )
            )
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {ix} ON {tbl} (namespace, occurred_at)").format(
                    ix=sql.Identifier(f"{self.table}_ns_occurred"), tbl=self._tbl
                )
            )
            # Канал recent: фильтр по namespace и порядок list_recent — top-N по индексу
            # без сортировки и полного просмотра namespace.
            cur.execute(
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {ix} ON {tbl} "
                    "(namespace, (occurred_at = ''), occurred_at DESC, ingested_at DESC)"
                ).format(ix=sql.Identifier(f"{self.table}_ns_recent"), tbl=self._tbl)
            )
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {ix} ON {tbl} (namespace, status)").format(
                    ix=sql.Identifier(f"{self.table}_ns_status"), tbl=self._tbl
                )
            )
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {ix} ON {tbl} USING gin (scopes)").format(
                    ix=sql.Identifier(f"{self.table}_scopes_gin"), tbl=self._tbl
                )
            )
            cur.execute(
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {ix} ON {tbl} USING gin (" + _FTS_DOC + ")"
                ).format(ix=sql.Identifier(f"{self.table}_fts"), tbl=self._tbl)
            )
        self._ensure_trgm()

    def _add_writer_visibility(self) -> None:
        """``ALTER TABLE … ADD COLUMN writer_visibility`` с ограниченным ожиданием лока.

        ALTER ждёт ACCESS EXCLUSIVE, пока открыт любой читатель таблицы (например,
        ``pg_dump``), а все новые запросы к ней встают в очередь за ALTER — без
        ``lock_timeout`` долгий читатель останавливает приём наблюдений целиком.
        Поэтому попытка ждёт не дольше ``MIGRATION_LOCK_TIMEOUT_MS``, попыток
        ``MIGRATION_ATTEMPTS`` с паузой ``MIGRATION_RETRY_PAUSE`` (очередь между
        ними рассасывается), затем — :class:`SchemaMigrationBusy`.

        ALTER пробует только сеанс, взявший advisory-лок миграции таблицы: без него
        попытки параллельных запросов шли бы внахлёст и держали очередь за ALTER
        непрерывно. Остальные не встают в очередь к таблице, а после паузы
        проверяют каталог — колонку мог добавить сеанс с локом.
        """
        for attempt in range(1, self.MIGRATION_ATTEMPTS + 1):
            if attempt > 1:
                time.sleep(self.MIGRATION_RETRY_PAUSE)
                if self._has_writer_visibility():
                    return
            try:
                with self.conn.transaction(), self.conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
                        (self.migration_lock_name,),
                    )
                    if not cur.fetchone()[0]:
                        continue  # ALTER уже пробует другой сеанс
                    # set_config(…, true) — только до конца транзакции (SET LOCAL).
                    cur.execute(
                        "SELECT set_config('lock_timeout', %s, true)",
                        (f"{self.MIGRATION_LOCK_TIMEOUT_MS}ms",),
                    )
                    cur.execute(
                        sql.SQL(
                            "ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS writer_visibility jsonb"
                        ).format(tbl=self._tbl)
                    )
                return
            except psycopg.errors.LockNotAvailable:
                continue
        raise SchemaMigrationBusy(
            f"Миграция {self.table}.writer_visibility не получила лок таблицы за "
            f"{self.MIGRATION_ATTEMPTS} попыток по {self.MIGRATION_LOCK_TIMEOUT_MS} мс: "
            "таблицу держит долгая транзакция (pg_stat_activity). Повторите позже."
        )

    @property
    def migration_lock_name(self) -> str:
        """Имя advisory-лока миграции таблицы (ключ — ``hashtext`` от него)."""
        return f"platform_memory:migrate:{self.table}"

    def _has_writer_visibility(self) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' "
                "AND table_name = %s AND column_name = 'writer_visibility'",
                (self.table,),
            )
            return cur.fetchone() is not None

    def _ensure_trgm(self) -> None:
        """Поставить pg_trgm и trigram-индекс, если хватает прав (иначе — без них)."""
        try:
            with self.conn.transaction():
                with self.conn.cursor() as cur:
                    cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
                    cur.execute(
                        sql.SQL(
                            "CREATE INDEX IF NOT EXISTS {ix} ON {tbl} "
                            "USING gin (content gin_trgm_ops)"
                        ).format(ix=sql.Identifier(f"{self.table}_content_trgm"), tbl=self._tbl)
                    )
        except psycopg.errors.InsufficientPrivilege:
            pass

    def table_exists(self) -> bool:
        """Проверить, существует ли таблица наблюдений."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"public.{self.table}",))
            return cur.fetchone()[0] is not None

    # --- запись ---

    def insert(
        self, obs: Observation, *, writer_scopes: Sequence[str] | None = None
    ) -> tuple[str, bool]:
        """Идемпотентно вставить наблюдение; вернуть (observation_id, duplicate).

        Повтор того же external observation (или того же содержимого без
        external_id) не создаёт дубликат: детерминированный observation_id
        конфликтует по (namespace, observation_id) и вставка пропускается.
        ``writer_scopes`` — видимость пишущего (None — без ограничения): хранится в
        записи, по ней redrive решает, что можно перезаписать (MEM-ADR-019).
        """
        ns = resolve_namespace(obs.namespace, self.default_namespace)
        obs.namespace = ns
        oid = obs.observation_id()
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {tbl} (observation_id, namespace, source_system, source_stream, "
                    "external_id, kind, occurred_at, actor, subject, scopes, content, data, "
                    "assertions, provenance, content_hash, status, writer_visibility) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (namespace, observation_id) DO NOTHING"
                ).format(tbl=self._tbl),
                (
                    oid,
                    ns,
                    obs.source_system,
                    obs.source_stream,
                    obs.external_id,
                    obs.kind,
                    obs.occurred_at,
                    Jsonb(obs.actor) if obs.actor else None,
                    Jsonb(obs.subject) if obs.subject else None,
                    Jsonb(obs.scopes),
                    obs.content,
                    Jsonb(obs.data),
                    Jsonb(obs.assertions),
                    Jsonb(obs.provenance),
                    obs.content_hash(),
                    STATUS_RECEIVED,
                    Jsonb({"scopes": list(writer_scopes) if writer_scopes is not None else None}),
                ),
            )
            duplicate = cur.rowcount == 0
        return oid, duplicate

    def set_status(self, observation_id: str, status: str, detail: str = "", namespace: str = ""):
        """Обновить статус обработки (единственное мутируемое, кроме redaction)."""
        if status not in STATUSES:
            raise ValueError(f"Неизвестный статус {status!r}")
        ns = resolve_namespace(namespace, self.default_namespace)
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "UPDATE {tbl} SET status = %s, status_detail = %s, processed_at = now() "
                    "WHERE namespace = %s AND observation_id = %s"
                ).format(tbl=self._tbl),
                (status, detail[:2000], ns, observation_id),
            )

    def redact(self, observation_id: str, namespace: str = "") -> bool:
        """Зачистить содержимое наблюдения, сохранив запись и id (аудит-след)."""
        ns = resolve_namespace(namespace, self.default_namespace)
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "UPDATE {tbl} SET content = '', data = '{{}}'::jsonb, "
                    "assertions = '[]'::jsonb, status = %s, status_detail = %s, "
                    "processed_at = now() "
                    "WHERE namespace = %s AND observation_id = %s"
                ).format(tbl=self._tbl),
                (STATUS_REDACTED, "redacted", ns, observation_id),
            )
            return cur.rowcount > 0

    def purge(self, observation_id: str, namespace: str = "") -> bool:
        """Физически удалить наблюдение (безвозвратно). Аудит пишет вызывающий."""
        ns = resolve_namespace(namespace, self.default_namespace)
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM {tbl} WHERE namespace = %s AND observation_id = %s").format(
                    tbl=self._tbl
                ),
                (ns, observation_id),
            )
            return cur.rowcount > 0

    # --- чтение ---

    def _row_record(self, row: tuple) -> ObservationRecord:
        writer = row[18] if len(row) > 18 and isinstance(row[18], dict) else None
        return ObservationRecord(
            observation_id=row[0],
            namespace=row[1],
            source_system=row[2],
            source_stream=row[3],
            external_id=row[4],
            kind=row[5],
            occurred_at=row[6],
            ingested_at=str(row[7] or ""),
            actor=row[8],
            subject=row[9],
            scopes=list(row[10] or []),
            content=row[11],
            data=row[12] if isinstance(row[12], dict) else {},
            assertions=list(row[13] or []),
            provenance=row[14] if isinstance(row[14], dict) else {},
            content_hash=row[15],
            status=row[16],
            status_detail=row[17],
            writer_visibility=writer,
        )

    def get(self, observation_id: str, namespaces: Sequence[str] = ()) -> ObservationRecord | None:
        """Вернуть наблюдение по id в области видимости namespaces, либо None."""
        nss = resolve_namespaces(namespaces, self.default_namespace)
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT " + _COLUMNS + " FROM {tbl} "
                    "WHERE observation_id = %s AND namespace = ANY(%s)"
                ).format(tbl=self._tbl),
                (observation_id, nss),
            )
            row = cur.fetchone()
        return self._row_record(row) if row else None

    @staticmethod
    def _scope_clause(
        scopes: list[str], allowed: Sequence[str] | None = None
    ) -> tuple[str, list[Any]]:
        """Предикат scope-фильтра: пересечение непусто ИЛИ элемент без scopes;
        плюс видимость по principal (MEM-ADR-019), если ``allowed`` задан."""
        clause, params = "", []
        if scopes:
            clause += " AND (scopes = '[]'::jsonb OR scopes ?| %s)"
            params.append(scopes)
        if allowed is not None:
            clause += " AND " + observation_visibility_sql("scopes")
            params.append(list(allowed))
        return clause, params

    def list_recent(
        self,
        namespaces: Sequence[str] = (),
        *,
        scopes: Sequence[str] = (),
        kinds: Sequence[str] = (),
        status: str = "",
        limit: int = 50,
        allowed_scopes: Sequence[str] | None = None,
    ) -> list[ObservationRecord]:
        """Свежие наблюдения (read-after-ingest канал Context Compiler'а).

        Сортировка: сначала event time (occurred_at, пустые в конце), затем
        время приёма. Фильтры: scopes (OR-пересечение), kinds, status.
        """
        query, params = self._recent_query(
            namespaces,
            scopes=scopes,
            kinds=kinds,
            status=status,
            limit=limit,
            allowed_scopes=allowed_scopes,
        )
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            return [self._row_record(r) for r in cur.fetchall()]

    def _recent_query(
        self,
        namespaces: Sequence[str] = (),
        *,
        scopes: Sequence[str] = (),
        kinds: Sequence[str] = (),
        status: str = "",
        limit: int = 50,
        allowed_scopes: Sequence[str] | None = None,
    ) -> tuple[sql.Composed, tuple[Any, ...]]:
        """SQL канала recent: top-``limit`` каждого namespace по индексу ``_ns_recent``,
        затем слияние namespaces с тем же порядком.

        Порядок — по колонке ``ingested_at`` (timestamptz), а не по одноимённому
        текстовому алиасу ``ingested_at::text`` из списка выборки. Подзапрос на
        namespace нужен, потому что ``namespace = ANY(…)`` не даёт упорядоченного
        чтения индекса: был бы полный просмотр и сортировка.
        """
        nss = resolve_namespaces(namespaces, self.default_namespace)
        clause, params = self._scope_clause(resolve_scopes(scopes), allowed_scopes)
        extra = ""
        if kinds:
            extra += " AND kind = ANY(%s)"
            params.append(list(kinds))
        if status:
            extra += " AND status = %s"
            params.append(status)
        query = sql.SQL(
            "SELECT " + _COLUMNS + " FROM unnest(%s::text[]) AS n(ns) CROSS JOIN LATERAL ("
            "SELECT * FROM {tbl} o WHERE o.namespace = n.ns" + clause + extra + " "
            "ORDER BY (o.occurred_at = '') ASC, o.occurred_at DESC, o.ingested_at DESC "
            "LIMIT %s) r "
            "ORDER BY (r.occurred_at = '') ASC, r.occurred_at DESC, r.ingested_at DESC "
            "LIMIT %s"
        ).format(tbl=self._tbl)
        return query, (nss, *params, int(limit), int(limit))

    def search_lexical(
        self,
        query_text: str,
        identifiers: Sequence[str] = (),
        *,
        namespaces: Sequence[str] = (),
        scopes: Sequence[str] = (),
        limit: int = 20,
        allowed_scopes: Sequence[str] | None = None,
    ) -> list[ObservationRecord]:
        """Lexical-поиск: русский FTS по content/kind + точные идентификаторы (ILIKE).

        Redacted-наблюдения не возвращаются. Результаты каналов сливаются с
        приоритетом identifier-совпадений (точное вхождение сильнее стемминга).
        """
        nss = resolve_namespaces(namespaces, self.default_namespace)
        scope_clause, scope_params = self._scope_clause(resolve_scopes(scopes), allowed_scopes)
        found: dict[str, ObservationRecord] = {}

        if identifiers:
            like_clause = " OR ".join(["content ILIKE %s"] * len(identifiers))
            like_params = [f"%{token}%" for token in identifiers]
            with self.conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT " + _COLUMNS + " FROM {tbl} "
                        "WHERE namespace = ANY(%s) AND status <> 'redacted' "
                        "AND (" + like_clause + ")" + scope_clause + " "
                        "ORDER BY ingested_at DESC LIMIT %s"
                    ).format(tbl=self._tbl),
                    (nss, *like_params, *scope_params, int(limit)),
                )
                for row in cur.fetchall():
                    rec = self._row_record(row)
                    rec.score = 1.0  # точное вхождение идентификатора
                    found[rec.observation_id] = rec

        if query_text.strip():
            with self.conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT " + _COLUMNS + ", ts_rank(" + _FTS_DOC + ", q) AS rank "
                        "FROM {tbl}, " + _FTS_QUERY + " q "
                        "WHERE " + _FTS_DOC + " @@ q "
                        "AND namespace = ANY(%s) AND status <> 'redacted'" + scope_clause + " "
                        "ORDER BY rank DESC LIMIT %s"
                    ).format(tbl=self._tbl),
                    (query_text, nss, *scope_params, int(limit)),
                )
                for row in cur.fetchall():
                    rec = self._row_record(row[:-1])
                    if rec.observation_id not in found:
                        rec.score = float(row[-1])
                        found[rec.observation_id] = rec

        ordered = sorted(found.values(), key=lambda r: r.score, reverse=True)
        return ordered[: int(limit)]

    # --- обслуживание ---

    def list_unprocessed(
        self,
        namespaces: Sequence[str] = (),
        *,
        limit: int = 100,
        allowed_scopes: Sequence[str] | None = None,
    ) -> list[ObservationRecord]:
        """Наблюдения, ждущие (пере)обработки: received / partially_processed / failed.

        Несут ``writer_visibility`` — видимость пишущего для redrive. ``allowed_scopes``
        — только видимые вызывающему (None — все), фильтр в SQL до ``limit``.
        """
        nss = resolve_namespaces(namespaces, self.default_namespace)
        clause, params = self._scope_clause([], allowed_scopes)
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT " + _COLUMNS + ", writer_visibility FROM {tbl} "
                    "WHERE namespace = ANY(%s) "
                    "AND status IN ('received', 'partially_processed', 'failed')" + clause + " "
                    "ORDER BY ingested_at ASC LIMIT %s"
                ).format(tbl=self._tbl),
                (nss, *params, int(limit)),
            )
            return [self._row_record(r) for r in cur.fetchall()]

    def list_legacy_unprocessed(
        self,
        namespaces: Sequence[str] = (),
        *,
        observation_ids: Sequence[str] = (),
        limit: int = 100,
    ) -> list[ObservationRecord]:
        """Необработанные наблюдения без сохранённой видимости пишущего (до TASK-001056).

        ``observation_ids`` — только эти (пусто — все такие в namespaces).
        """
        nss = resolve_namespaces(namespaces, self.default_namespace)
        extra, params = ("", [])
        if observation_ids:
            extra, params = (" AND observation_id = ANY(%s)", [list(observation_ids)])
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT " + _COLUMNS + ", writer_visibility FROM {tbl} "
                    "WHERE namespace = ANY(%s) AND writer_visibility IS NULL "
                    "AND status IN ('received', 'partially_processed', 'failed')" + extra + " "
                    "ORDER BY ingested_at ASC LIMIT %s"
                ).format(tbl=self._tbl),
                (nss, *params, int(limit)),
            )
            return [self._row_record(r) for r in cur.fetchall()]

    def set_legacy_writer_visibility(
        self,
        observation_id: str,
        writer_scopes: Sequence[str] | None,
        namespace: str = "",
        *,
        mark: dict[str, Any] | None = None,
    ) -> bool:
        """Записать видимость пишущего в наблюдение, у которого её нет (оператор).

        ``mark`` — поля следа решения рядом со ``scopes`` (кто и когда заявил):
        заявленная видимость отличима от записанной при приёме. Сохранённую
        видимость не перезаписывает: только ``writer_visibility IS NULL``.
        True — запись обновлена.
        """
        ns = resolve_namespace(namespace, self.default_namespace)
        value = {
            "scopes": list(writer_scopes) if writer_scopes is not None else None,
            **(mark or {}),
        }
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "UPDATE {tbl} SET writer_visibility = %s "
                    "WHERE namespace = %s AND observation_id = %s AND writer_visibility IS NULL"
                ).format(tbl=self._tbl),
                (Jsonb(value), ns, observation_id),
            )
            return cur.rowcount > 0

    def count(self, namespaces: Sequence[str] = (), *, status: str = "") -> int:
        """Число наблюдений (опционально по статусу)."""
        nss = resolve_namespaces(namespaces, self.default_namespace)
        extra, params = ("", [])
        if status:
            extra, params = (" AND status = %s", [status])
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT count(*) FROM {tbl} WHERE namespace = ANY(%s)" + extra).format(
                    tbl=self._tbl
                ),
                (nss, *params),
            )
            return int(cur.fetchone()[0])

    def counts_by_status(
        self, namespaces: Sequence[str] = (), *, allowed_scopes: Sequence[str] | None = None
    ) -> dict[str, int]:
        """Свод статусов обработки — для stats/аудита ingestion state."""
        nss = resolve_namespaces(namespaces, self.default_namespace)
        clause, params = self._scope_clause([], allowed_scopes)
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT status, count(*) FROM {tbl} WHERE namespace = ANY(%s)"
                    + clause
                    + " GROUP BY status"
                ).format(tbl=self._tbl),
                (nss, *params),
            )
            return {row[0]: int(row[1]) for row in cur.fetchall()}


def observation_from_json(text: str, *, namespace: str = "") -> Observation:
    """Утилита CLI: собрать Observation из JSON-строки."""
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("Ожидается JSON-объект observation")
    return Observation.from_payload(payload, namespace=namespace)

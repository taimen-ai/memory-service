"""Доступ к Postgres и схема графа памяти (MEM-ADR-023).

Граф хранится в обычных таблицах: имя графа (``CB_GRAPH_NAME``) — схема PostgreSQL,
в ней ``graph_nodes`` и ``graph_edges``. Сервису не нужны ни расширение графа, ни
суперпользователь: подключение ничего не загружает в сессию и не трогает
``search_path``; расширения ``vector`` и ``pg_trgm`` создаёт шаг подготовки базы.

Схему создаёт :func:`ensure_graph` тем же способом, что и остальные таблицы сервиса
(``CREATE … IF NOT EXISTS`` из кода, без миграций). Таблицы не конфликтуют с
таблицами меток прежнего графа AGE в той же схеме: имена меток AGE — виды сущностей
и типы связей; если среди них всё же есть ``graph_nodes``/``graph_edges``, создание
схемы отказывает явной ошибкой, а не пишет в чужую таблицу (:class:`GraphSchemaConflict`).
"""

from __future__ import annotations

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from platform_memory.core.config import Settings

NODES_TABLE = "graph_nodes"
EDGES_TABLE = "graph_edges"

# Версия схемы графа: отметка комментарием на таблице узлов, шаги догона — ``_STEPS``.
# 1 — таблицы TASK-001284; 2 — ограничения длины ключа и метки (TASK-001285).
GRAPH_SCHEMA_VERSION = 2
_MARK_PREFIX = "platform-memory graph schema v"

# Предел ключа узла и метки (вида узла, типа ребра) в байтах UTF-8. Строка B-tree-индекса
# не длиннее ~2,7 КБ (треть страницы): ключ ``(namespace, label, natural_key)`` при
# namespace до 200 символов и метке до 256 байт с ключом до 2048 байт в неё входит.
# Длиннее — отказ записи ``GraphKeyTooLong`` (HTTP 400 ``natural_key_too_long`` /
# ``label_too_long``), а не ``ProgramLimitExceeded`` из индекса.
MAX_NATURAL_KEY_BYTES = 2048
MAX_LABEL_BYTES = 256


class GraphSchemaConflict(RuntimeError):
    """В схеме графа есть таблица с именем ``graph_nodes``/``graph_edges`` чужой структуры."""


class GraphKeyTooLong(ValueError):
    """Ключ узла или метка длиннее предела индекса (MEM-ADR-023, ``MAX_*_BYTES``).

    ``code`` — стабильный код отказа (``natural_key_too_long`` | ``label_too_long``), он
    же — начало текста ошибки: потребитель HTTP API видит его в ``detail``.
    """

    def __init__(self, code: str, what: str, value: str, size: int, limit: int):
        self.code = code
        self.size = size
        self.limit = limit
        preview = value[:80] + ("…" if len(value) > 80 else "")
        super().__init__(f"{code}: {what} длиннее {limit} байт UTF-8 ({size}): {preview!r}")


def check_graph_key(*, natural_key: str | None = None, label: str | None = None) -> None:
    """Проверить длину ключа узла и метки до записи (:class:`GraphKeyTooLong`)."""
    if natural_key is not None:
        size = len(natural_key.encode("utf-8"))
        if size > MAX_NATURAL_KEY_BYTES:
            raise GraphKeyTooLong(
                "natural_key_too_long", "natural_key", natural_key, size, MAX_NATURAL_KEY_BYTES
            )
    if label is not None:
        size = len(label.encode("utf-8"))
        if size > MAX_LABEL_BYTES:
            raise GraphKeyTooLong("label_too_long", "вид/тип связи", label, size, MAX_LABEL_BYTES)


def session_options(settings: Settings) -> dict[str, str]:
    """Параметры libpq для соединения сервиса: выключение JIT у графовых запросов (ADR-010).

    Выключение появилось при AGE: тот отдавал планировщику мусорную оценку
    кардинальности (на графе в 1037 вершин — 1.69 млрд строк), стоимость плана
    перешагивала ``jit_above_cost``, и LLVM компилировал каждый графовый запрос
    заново. На таблицах графа (MEM-ADR-023) оценки обычные; нужен ли ещё
    ``jit=off``, решает замер MEM-ADR-023 п.6, до него поведение прежнее.

    Явно заданный ``options`` в ``CB_DATABASE_URL`` уважается и не перетирается — так остаётся
    возможность настроить сессию вручную (в т.ч. для пулеров, не поддерживающих ``options``).
    """
    if settings.db_jit or "options" in conninfo_to_dict(settings.database_url):
        return {}
    return {"options": "-c jit=off"}


def connect(settings: Settings, *, autocommit: bool = True) -> psycopg.Connection:
    """Открыть подключение сервиса (обычная роль, без подготовки сессии)."""
    return psycopg.connect(
        settings.database_url, autocommit=autocommit, **session_options(settings)
    )


def nodes_table(graph_name: str) -> sql.Identifier:
    """Квалифицированное имя таблицы узлов графа."""
    return sql.Identifier(graph_name, NODES_TABLE)


def edges_table(graph_name: str) -> sql.Identifier:
    """Квалифицированное имя таблицы рёбер графа."""
    return sql.Identifier(graph_name, EDGES_TABLE)


def _graph_tables_sql() -> str:
    """Условие «обе таблицы графа есть и они наши» для параметров ``(узлы, рёбра)``.

    Наши — по колонкам ``natural_key`` (узлы) и ``src_id`` (рёбра): у таблицы метки
    AGE с тем же именем колонки ``id``/``properties``.
    """
    return (
        "to_regclass(%(n)s) IS NOT NULL AND to_regclass(%(e)s) IS NOT NULL "
        "AND EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass(%(n)s) "
        "AND attname = 'natural_key' AND NOT attisdropped) "
        "AND EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass(%(e)s) "
        "AND attname = 'src_id' AND NOT attisdropped)"
    )


def _names(graph_name: str) -> dict[str, str]:
    return {"n": _qualified(graph_name, NODES_TABLE), "e": _qualified(graph_name, EDGES_TABLE)}


def graph_exists(conn: psycopg.Connection, graph_name: str) -> bool:
    """Есть ли в схеме ``graph_name`` обе таблицы графа — и это наши таблицы.

    Одноимённые таблицы меток прежнего графа AGE (метки ``graph_nodes``/``graph_edges``)
    графом на таблицах не считаются.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT " + _graph_tables_sql(), _names(graph_name))
        row = cur.fetchone()
    return bool(row and row[0])


def graph_schema_version(conn: psycopg.Connection, graph_name: str) -> int:
    """Версия схемы графа: 0 — таблиц нет, 1 — до учёта версий, далее — из отметки.

    Чужие таблицы с именами графа — :class:`GraphSchemaConflict`.
    """
    with conn.cursor() as cur:
        _check_no_conflict(cur, graph_name)
        return _stored_version(cur, graph_name)


def ensure_graph(conn: psycopg.Connection, graph_name: str) -> None:
    """Создать схему графа и её таблицы или догнать их до текущей версии (идемпотентно).

    Зовётся на каждом пути записи и чтения, поэтому сначала — один дешёвый запрос:
    отметка версии на таблице узлов (комментарий ``platform-memory graph schema vN``,
    его ставит только этот код — таблица метки AGE с тем же именем отметки не имеет).
    Отметка текущей (или более новой) версии и таблица рёбер на месте — готово.

    Иначе — одна транзакция под advisory-локом имени схемы: проверка, что таблицы
    с именами графа не чужие (:class:`GraphSchemaConflict` — например, метка AGE
    ``graph_nodes``), затем шаги от сохранённой версии до :data:`GRAPH_SCHEMA_VERSION`
    и новая отметка. Два первых запроса к пустой базе иначе столкнулись бы на
    ``CREATE … IF NOT EXISTS`` (``UniqueViolation`` в каталоге). Лок берётся только на
    этом пути — upsert узлов и рёбер локов не берёт (MEM-ADR-023 п.2).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT obj_description(to_regclass(%(n)s), 'pg_class'), "
            "to_regclass(%(e)s) IS NOT NULL",
            _names(graph_name),
        )
        row = cur.fetchone()
    if row and row[1] and _parse_mark(row[0]) >= GRAPH_SCHEMA_VERSION:
        return
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"graph-schema:{graph_name}",)
        )
        _check_no_conflict(cur, graph_name)
        version = _stored_version(cur, graph_name)
        if version >= GRAPH_SCHEMA_VERSION:
            return  # догнал параллельный вызов (или схема новее кода)
        for step_version, step in _STEPS:
            if version < step_version:
                for statement in step(cur, graph_name):
                    cur.execute(statement)
        cur.execute(
            sql.SQL("COMMENT ON TABLE {n} IS {mark}").format(
                n=nodes_table(graph_name), mark=sql.Literal(_mark(GRAPH_SCHEMA_VERSION))
            )
        )


def drop_graph(conn: psycopg.Connection, graph_name: str) -> None:
    """Удалить таблицы графа вместе с данными (для --reset и тестов).

    Удаляются только наши ``graph_nodes``/``graph_edges`` (по структуре: таблица метки
    AGE с тем же именем не трогается); схема — лишь если в ней больше ничего нет.
    Таблицы меток прежнего графа AGE в той же схеме остаются (MEM-ADR-023 п.5:
    данные AGE удаляет отдельный шаг).
    """
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND ((c.relname = %s AND EXISTS (SELECT 1 FROM pg_attribute a "
            "WHERE a.attrelid = c.oid AND a.attname = 'src_id' AND NOT a.attisdropped)) "
            "OR (c.relname = %s AND EXISTS (SELECT 1 FROM pg_attribute a "
            "WHERE a.attrelid = c.oid AND a.attname = 'natural_key' AND NOT a.attisdropped)))",
            (graph_name, EDGES_TABLE, NODES_TABLE),
        )
        ours = {r[0] for r in cur.fetchall()}
        # Рёбра — первыми: на узлы ссылается их внешний ключ.
        for table in (EDGES_TABLE, NODES_TABLE):
            if table in ours:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {t}").format(t=sql.Identifier(graph_name, table))
                )
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = %s) "
            "AND NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n "
            "ON n.oid = c.relnamespace WHERE n.nspname = %s)",
            (graph_name, graph_name),
        )
        row = cur.fetchone()
        if row and row[0]:
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {s}").format(s=sql.Identifier(graph_name)))


def _qualified(schema: str, table: str) -> str:
    """Текст квалифицированного имени для ``to_regclass`` (кавычки — как у Identifier)."""
    return ".".join('"' + part.replace('"', '""') + '"' for part in (schema, table))


def _mark(version: int) -> str:
    return f"{_MARK_PREFIX}{version}"


def _parse_mark(comment: str | None) -> int:
    """Версия из отметки на таблице узлов; нет отметки — 0."""
    if not comment or not comment.startswith(_MARK_PREFIX):
        return 0
    try:
        return int(comment[len(_MARK_PREFIX) :].split()[0])
    except (ValueError, IndexError):
        return 0


def _stored_version(cur: psycopg.Cursor, graph_name: str) -> int:
    """Сохранённая версия схемы (после проверки на чужие таблицы)."""
    names = _names(graph_name)
    cur.execute(
        "SELECT obj_description(to_regclass(%(n)s), 'pg_class'), " + _graph_tables_sql(), names
    )
    row = cur.fetchone()
    if not row or not row[1]:
        return 0
    # Таблицы есть, отметки нет — схема TASK-001284 (до учёта версий).
    return _parse_mark(row[0]) or 1


def _check_no_conflict(cur: psycopg.Cursor, graph_name: str) -> None:
    """Отказать, если ``graph_nodes``/``graph_edges`` в схеме есть, но это не наши таблицы.

    На установке с AGE схема ``graph_name`` уже существует и держит таблицы меток по
    имени метки. Метка ``graph_nodes`` (вид сущности с таким именем) дала бы таблицу
    с колонками ``id``/``properties`` формата AGE — писать в неё нельзя. Проверка идёт
    до любого вывода «граф уже есть».
    """
    cur.execute(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = ANY(%s) AND NOT EXISTS ("
        "SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid "
        "AND a.attname = 'natural_key' AND NOT a.attisdropped) "
        "AND NOT EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid "
        "AND a.attname = 'src_id' AND NOT a.attisdropped)",
        (graph_name, [NODES_TABLE, EDGES_TABLE]),
    )
    clash = [r[0] for r in cur.fetchall()]
    if clash:
        raise GraphSchemaConflict(
            f"В схеме графа {graph_name!r} есть таблицы {clash} другой структуры "
            "(метка графа AGE с тем же именем?) — схема графа на таблицах не создана "
            "(MEM-ADR-023)."
        )


def _schema_ddl(cur: psycopg.Cursor, graph_name: str) -> list[sql.Composed]:
    """Шаг 1: DDL схемы графа (MEM-ADR-023 п.1).

    Узел — ``(namespace, label, natural_key)``; метка — значение колонки, новая метка
    не требует DDL. Колонки — то, по чему идут фильтры и соединения; остальные
    свойства узла (``title``, ``source_path``, ``origin``, вложенная карта ``props``
    со свободными свойствами, разметка сообществ…) — в ``props jsonb`` в прежнем виде.
    ``observation_id`` узла — вычисляемая колонка из ``props.props.observation_id``
    (узлы, порождённые наблюдением: редакция и purge, ADR-001/002).

    Ребро — ``type`` и концы ``src_id``/``dst_id`` с ``on delete cascade`` (семантика
    ``DETACH DELETE``). Рёбра-факты (ADR-016 §4) уникальны по ``(namespace, fact_id)``,
    структурные связи — по ``(src_id, type, dst_id)``. Временны́е поля фактов
    (``valid_from``/``valid_to`` — строки ISO-8601 UTC, сравниваются побайтово:
    ``COLLATE "C"``), ``evidence_lost`` и ``observation_ids`` — колонки; остальное —
    ``props``. ``namespace`` у структурного ребра — namespace его концов.
    """
    s = sql.Identifier(graph_name)
    n, e = nodes_table(graph_name), edges_table(graph_name)

    def ix(name: str) -> sql.Identifier:
        """Имя индекса: ``n_<суффикс>`` — у таблицы узлов, ``e_<суффикс>`` — у рёбер."""
        table = NODES_TABLE if name.startswith("n_") else EDGES_TABLE
        return sql.Identifier(f"{table}_{name[2:]}")

    # Схема уже есть (граф AGE, прежний прогон, создана владельцем заранее) —
    # ``CREATE SCHEMA IF NOT EXISTS`` не зовём: он проверяет право CREATE на базу раньше
    # существования, и роль-владелец готовой схемы без этого права получила бы отказ.
    cur.execute("SELECT to_regnamespace(%s) IS NOT NULL", (_qualified_one(graph_name),))
    row = cur.fetchone()
    create_schema = [] if row and row[0] else [sql.SQL("CREATE SCHEMA {s}").format(s=s)]
    return [
        *create_schema,
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {n} (
                id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                namespace      text NOT NULL,
                label          text NOT NULL,
                natural_key    text NOT NULL,
                type           text,
                valid_from     text COLLATE "C",
                valid_to       text COLLATE "C",
                props          jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                observation_id text GENERATED ALWAYS AS (props -> 'props' ->> 'observation_id')
                               STORED,
                CONSTRAINT {uq} UNIQUE (namespace, label, natural_key)
            )
            """
        ).format(n=n, uq=ix("n_key")),
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {e} (
                id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                namespace       text NOT NULL,
                type            text NOT NULL,
                src_id          bigint NOT NULL REFERENCES {n} (id) ON DELETE CASCADE,
                dst_id          bigint NOT NULL REFERENCES {n} (id) ON DELETE CASCADE,
                fact_id         text,
                observation_ids text[],
                valid_from      text COLLATE "C",
                valid_to        text COLLATE "C",
                evidence_lost   boolean NOT NULL DEFAULT false,
                props           jsonb NOT NULL DEFAULT '{{}}'::jsonb
            )
            """
        ).format(e=e, n=n),
        # Узлы: поиск по ключу без метки, по виду, по наблюдению, по свойствам.
        sql.SQL("CREATE INDEX IF NOT EXISTS {i} ON {n} (namespace, natural_key)").format(
            i=ix("n_ns_key"), n=n
        ),
        sql.SQL("CREATE INDEX IF NOT EXISTS {i} ON {n} (namespace, type)").format(
            i=ix("n_ns_type"), n=n
        ),
        sql.SQL(
            "CREATE INDEX IF NOT EXISTS {i} ON {n} (namespace, observation_id) "
            "WHERE observation_id IS NOT NULL"
        ).format(i=ix("n_ns_obs"), n=n),
        sql.SQL("CREATE INDEX IF NOT EXISTS {i} ON {n} USING gin (props jsonb_path_ops)").format(
            i=ix("n_props_gin"), n=n
        ),
        # Рёбра: уникальность фактов и структурных связей, обход, выборки фактов.
        sql.SQL(
            "CREATE UNIQUE INDEX IF NOT EXISTS {i} ON {e} (namespace, fact_id) "
            "WHERE fact_id IS NOT NULL"
        ).format(i=ix("e_fact_key"), e=e),
        sql.SQL(
            "CREATE UNIQUE INDEX IF NOT EXISTS {i} ON {e} (src_id, type, dst_id) "
            "WHERE fact_id IS NULL"
        ).format(i=ix("e_link_key"), e=e),
        sql.SQL("CREATE INDEX IF NOT EXISTS {i} ON {e} (src_id, type)").format(i=ix("e_src"), e=e),
        sql.SQL("CREATE INDEX IF NOT EXISTS {i} ON {e} (dst_id, type)").format(i=ix("e_dst"), e=e),
        sql.SQL("CREATE INDEX IF NOT EXISTS {i} ON {e} (namespace, type)").format(
            i=ix("e_ns_type"), e=e
        ),
        sql.SQL(
            "CREATE INDEX IF NOT EXISTS {i} ON {e} USING gin (observation_ids) "
            "WHERE observation_ids IS NOT NULL"
        ).format(i=ix("e_obs_gin"), e=e),
        sql.SQL("CREATE INDEX IF NOT EXISTS {i} ON {e} USING gin (props jsonb_path_ops)").format(
            i=ix("e_props_gin"), e=e
        ),
    ]


def _key_limits_ddl(cur: psycopg.Cursor, graph_name: str) -> list[sql.Composed]:
    """Шаг 2: ограничения длины ключа узла и метки (пределы ``MAX_*_BYTES``).

    Код проверяет длину до записи (:func:`check_graph_key`); ограничение в базе —
    страховка для любого другого пишущего. ``NOT VALID``: существующие строки не
    перепроверяются (длиннее индекса их и так нет), шаг не держит таблицу на скане.
    """
    wanted = {
        f"{NODES_TABLE}_key_len": (
            nodes_table(graph_name),
            f"octet_length(natural_key) <= {MAX_NATURAL_KEY_BYTES} "
            f"AND octet_length(label) <= {MAX_LABEL_BYTES}",
        ),
        f"{EDGES_TABLE}_type_len": (
            edges_table(graph_name),
            f"octet_length(type) <= {MAX_LABEL_BYTES}",
        ),
    }
    cur.execute(
        "SELECT conname FROM pg_constraint WHERE conname = ANY(%s) "
        "AND conrelid IN (to_regclass(%s), to_regclass(%s))",
        (list(wanted), _qualified(graph_name, NODES_TABLE), _qualified(graph_name, EDGES_TABLE)),
    )
    present = {r[0] for r in cur.fetchall()}
    return [
        sql.SQL("ALTER TABLE {t} ADD CONSTRAINT {c} CHECK ({expr}) NOT VALID").format(
            t=table, c=sql.Identifier(name), expr=sql.SQL(expr)
        )
        for name, (table, expr) in wanted.items()
        if name not in present
    ]


def _qualified_one(name: str) -> str:
    """Текст имени в кавычках для ``to_regnamespace``."""
    return '"' + name.replace('"', '""') + '"'


# Шаги догона схемы: (версия, функция DDL). Шаг выполняется, если сохранённая версия
# меньше его номера; функции читают каталог и возвращают только недостающее.
_STEPS = ((1, _schema_ddl), (2, _key_limits_ddl))

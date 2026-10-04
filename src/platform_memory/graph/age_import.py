"""Перенос графа из Apache AGE в таблицы графа (MEM-ADR-023 п.5, ``cb migrate-graph-from-age``).

Одноразовый шаг установки, где граф ещё лежит в AGE. Читает таблицы меток графа
``CB_GRAPH_NAME`` и пишет узлы и рёбра в ``graph_nodes``/``graph_edges`` той же схемы.
Данные AGE не меняются.

**Чтение.** Каталог AGE (``ag_graph``/``ag_label``) и ``cypher()`` не используются: у AGE
каждая метка — таблица схемы графа, наследница ``_ag_label_vertex`` (``id graphid,
properties agtype``) или ``_ag_label_edge`` (плюс ``start_id``, ``end_id``). Чтение из
родительской таблицы отдаёт строки всех меток, имя метки — имя таблицы строки
(``tableoid``). Так перенос не зависит от OID в каталоге AGE, которые после
``pg_dump``/``pg_restore`` указывают на старый кластер, и не требует ``LOAD 'age'``:
``agtype`` и ``graphid`` читаются текстом (``::text``) — это их функции вывода. Битый
каталог только отмечается в отчёте (``cypher()`` на такой базе не работает).
Обе таблицы читаются в одной транзакции ``REPEATABLE READ`` — согласованный снимок.

**Сопоставление.** Свойства AGE — карта (текст ``agtype`` — JSON, аннотации типов
``::numeric`` и т.п. вне строк снимаются). Узел: метка — имя таблицы; ``natural_key``,
``namespace``, ``type``, ``valid_from``, ``valid_to`` — колонки, остальное — ``props`` в
прежней раскладке (``title``, ``source_path``, ``origin``, вложенная ``props``, разметка
сообществ…). Узел без ``namespace`` получает namespace по умолчанию, как делал прежний
``ensure_schema``. Ребро: тип — имя таблицы, концы — узлы, в которые перенесены его
вершины AGE; у ребра-факта ``fact_id``, ``namespace``, ``observation_ids``,
``valid_from``, ``valid_to``, ``evidence_lost`` — колонки; namespace структурной связи —
namespace узла-источника. Значение не того типа (например, ``valid_from`` не строка)
колонкой не становится и остаётся в ``props`` как было.

**Идентификаторы.** Строка получает id своей вершины или ребра AGE (graphid — число;
id строк, выданные генератором таблицы, с ними не пересекаются: graphid меток графа не
меньше 2^48). Так ссылки, которые Context API отдавал по id ребра (``edge:<id>`` в
``used.facts`` typed-контекста), и порядок строк по id остаются прежними. После переноса
генераторы id продвигаются за перенесённые — новые строки сервиса идут после них.

**Идемпотентность.** Вставка — ``ON CONFLICT DO NOTHING`` (id и уникальные ключи таблиц):
повторный запуск ничего не дублирует, доносит недостающее и не перетирает строки, уже
изменённые сервисом. Пачки пишутся отдельными транзакциями — прерванный перенос
продолжается повторным запуском.

**Факт на нескольких вершинах.** Прежний код на AGE искал концы факта по ключу без
вида: у ключа с узлами нескольких видов (``person:bob`` — и ``person``, и ``contact``)
один факт становился несколькими рёбрами с тем же ``fact_id``. В таблицах факт уникален
в namespace — переносится одно ребро, между вершинами с наименьшими id (как выбирает
концы новый ``assert_fact``), остальные — ``duplicate_edge`` в отчёте. Поэтому рёбра
читаются в порядке ``(start_id, end_id, id)``.

**Сверка.** В конце — число узлов и рёбер по namespace: в AGE, дубли (в AGE несколько
вершин одного ``(namespace, метка, natural_key)`` или рёбер одного ключа — в таблицах
это одна строка: у вершин первая по id AGE, у рёбер — см. выше), пропущенные и в таблицах. Ожидается: таблицы =
AGE − дубли. Пропущенное (ключ длиннее предела индекса, нет ``natural_key``, свойства не
разбираются, конец ребра пропущен или отсутствует) не обрезается и не чинится молча:
оно в отчёте, и сверка его не прощает — код выхода ненулевой.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from platform_memory.core import db as dbmod
from platform_memory.core.config import Settings
from platform_memory.core.namespaces import resolve_namespace
from platform_memory.core.ontology import sanitize_label

AGE_VERTEX_PARENT = "_ag_label_vertex"
AGE_EDGE_PARENT = "_ag_label_edge"

# Виды записей отчёта, при которых узел или ребро НЕ перенесены (сверка их не прощает).
SKIP_KINDS = frozenset(
    {
        "natural_key_too_long",
        "label_too_long",
        "no_natural_key",
        "unparsable_properties",
        "endpoint_skipped",
        "orphan_edge",
        "unlabeled_edge",
    }
)
# Виды-сведения: перенос идёт, отчёт объясняет.
INFO_KINDS = frozenset(
    {"duplicate_node", "duplicate_edge", "namespace_backfilled", "unlabeled_vertex"}
)


class AgeGraphNotFound(RuntimeError):
    """В схеме ``CB_GRAPH_NAME`` нет таблиц меток графа AGE — переносить нечего."""


@dataclass(slots=True)
class NamespaceCounts:
    """Счётчики сверки одного namespace."""

    age_nodes: int = 0
    dup_nodes: int = 0
    skipped_nodes: int = 0
    table_nodes: int = 0
    age_edges: int = 0
    dup_edges: int = 0
    skipped_edges: int = 0
    table_edges: int = 0

    @property
    def expected_nodes(self) -> int:
        return self.age_nodes - self.dup_nodes

    @property
    def expected_edges(self) -> int:
        return self.age_edges - self.dup_edges

    @property
    def ok(self) -> bool:
        return (
            self.table_nodes == self.expected_nodes
            and self.table_edges == self.expected_edges
            and not self.skipped_nodes
            and not self.skipped_edges
        )


@dataclass(slots=True)
class Anomaly:
    """Запись отчёта: что и почему не перенесено как есть."""

    kind: str
    namespace: str
    label: str
    age_id: int
    detail: str = ""


@dataclass
class MigrationReport:
    """Итог переноса: вставлено, сверка по namespace, записи отчёта, состояние каталога."""

    graph: str
    dry_run: bool = False
    catalog_warning: str = ""
    nodes_inserted: int = 0
    edges_inserted: int = 0
    namespaces: dict[str, NamespaceCounts] = field(default_factory=dict)
    anomalies: list[Anomaly] = field(default_factory=list)

    def ns(self, namespace: str) -> NamespaceCounts:
        return self.namespaces.setdefault(namespace, NamespaceCounts())

    def mismatched(self) -> list[str]:
        """Namespaces, где таблицы не сходятся с AGE (в dry-run — только пропуски)."""
        if self.dry_run:
            return sorted(
                ns for ns, c in self.namespaces.items() if c.skipped_nodes or c.skipped_edges
            )
        return sorted(ns for ns, c in self.namespaces.items() if not c.ok)

    @property
    def ok(self) -> bool:
        return not self.mismatched()

    def by_kind(self) -> dict[str, list[Anomaly]]:
        out: dict[str, list[Anomaly]] = {}
        for a in self.anomalies:
            out.setdefault(a.kind, []).append(a)
        return out


# --- разбор agtype ---

_ANNOTATION = re.compile(r"::[A-Za-z_][A-Za-z0-9_]*")


def _reject_constant(name: str) -> Any:
    raise ValueError(f"нечисловое значение {name} — в jsonb не переносится")


def _strip_annotations(text: str) -> str:
    """Снять аннотации типов agtype (``::numeric``, ``::vertex``…) вне строковых литералов."""
    out: list[str] = []
    i, n = 0, len(text)
    in_string = False
    while i < n:
        ch = text[i]
        if in_string:
            if ch == "\\" and i + 1 < n:
                out.append(text[i : i + 2])
                i += 2
                continue
            if ch == '"':
                in_string = False
            out.append(ch)
            i += 1
            continue
        if ch == '"':
            in_string = True
        elif ch == ":" and text.startswith("::", i):
            m = _ANNOTATION.match(text, i)
            if m:
                i = m.end()
                continue
        out.append(ch)
        i += 1
    return "".join(out)


def parse_agtype(text: str | None) -> Any:
    """Текст ``agtype`` -> Python-значение (ValueError — не разбирается).

    Текст agtype — JSON, у значений бывают аннотации типа (``1.5::numeric``, у
    вершины и ребра целиком — ``::vertex``/``::edge``); строки с ``::`` внутри не
    трогаются. ``NaN``/``Infinity`` в jsonb не переносятся — ошибка разбора.
    """
    if text is None:
        return None
    try:
        return json.loads(text, parse_constant=_reject_constant)
    except json.JSONDecodeError:
        return json.loads(_strip_annotations(text), parse_constant=_reject_constant)


# --- перенос ---


def _qualified(schema: str, table: str) -> str:
    return ".".join('"' + p.replace('"', '""') + '"' for p in (schema, table))


def _require_age_graph(cur: psycopg.Cursor, graph: str) -> None:
    cur.execute(
        "SELECT to_regclass(%s) IS NOT NULL AND to_regclass(%s) IS NOT NULL",
        (_qualified(graph, AGE_VERTEX_PARENT), _qualified(graph, AGE_EDGE_PARENT)),
    )
    row = cur.fetchone()
    if not (row and row[0]):
        raise AgeGraphNotFound(
            f"В схеме {graph!r} нет таблиц меток графа AGE ({AGE_VERTEX_PARENT}, "
            f"{AGE_EDGE_PARENT}) — проверьте CB_GRAPH_NAME; переносить нечего."
        )


def catalog_warning(conn: psycopg.Connection, graph: str) -> str:
    """Пусто — каталог AGE цел (или его нет); иначе — описание битого каталога.

    После ``pg_dump``/``pg_restore`` ``ag_graph.graphid`` и ``ag_label.graph`` несут OID
    старого кластера, а схема графа — новый: ``cypher()`` падает с ``graph with oid …
    does not exist``. Переносу это не мешает (он читает таблицы меток напрямую).
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('ag_catalog.ag_graph') IS NOT NULL")
            row = cur.fetchone()
            if not (row and row[0]):
                return ""
            cur.execute(
                "SELECT graphid::bigint, namespace::oid::bigint, "
                "(SELECT count(*) FROM ag_catalog.ag_label l WHERE l.graph = g.namespace::oid) "
                "FROM ag_catalog.ag_graph g WHERE g.name = %s",
                (graph,),
            )
            row = cur.fetchone()
    except psycopg.Error as exc:
        return f"каталог AGE не прочитан ({exc.__class__.__name__}): {exc}".strip()
    if row is None:
        return f"графа {graph!r} нет в ag_catalog.ag_graph — таблицы меток читаются напрямую"
    graphid, ns_oid, labels = row
    if graphid != ns_oid or not labels:
        return (
            f"каталог AGE указывает на OID другого кластера (ag_graph.graphid={graphid}, "
            f"OID схемы {ns_oid}; меток с OID схемы: {labels}) — так бывает после "
            "pg_dump/pg_restore. Перенос читает таблицы меток напрямую и от этого не "
            "зависит; cypher() на этой базе не работает. Починка каталога (если нужен AGE): "
            "в одной транзакции снять FK fk_graph_oid у ag_catalog.ag_label, "
            "UPDATE ag_catalog.ag_label SET graph = g.namespace::oid FROM ag_catalog.ag_graph g "
            "WHERE ag_label.graph = g.graphid; UPDATE ag_catalog.ag_graph SET graphid = "
            "namespace::oid; вернуть FK."
        )
    return ""


def _stream(
    conn: psycopg.Connection, name: str, query: sql.Composable, batch_size: int
) -> Iterator[tuple[Any, ...]]:
    """Строки запроса серверным курсором (транзакция чтения уже открыта)."""
    with conn.cursor(name=name) as cur:
        cur.itersize = batch_size
        cur.execute(query)
        yield from cur


@dataclass
class _Vertex:
    ns: str
    label: str
    nk: str


class _Importer:
    def __init__(
        self,
        settings: Settings,
        read: psycopg.Connection,
        write: psycopg.Connection,
        report: MigrationReport,
        batch_size: int,
    ):
        self.graph = report.graph
        self.default_ns = resolve_namespace("", settings.default_namespace)
        self.read = read
        self.write = write
        self.report = report
        self.batch_size = max(1, int(batch_size))
        # graphid вершины AGE -> куда перенесена (None — вершина пропущена).
        self.vertices: dict[int, _Vertex | None] = {}
        self.N = dbmod.nodes_table(self.graph)
        self.E = dbmod.edges_table(self.graph)

    def _note(self, kind: str, ns: str, label: str, age_id: int, detail: str = "") -> None:
        self.report.anomalies.append(Anomaly(kind, ns, label, age_id, detail))

    # --- узлы ---

    def nodes(self) -> None:
        query = sql.SQL(
            "SELECT c.relname, v.id::text::bigint, v.properties::text FROM {t} v "
            "JOIN pg_class c ON c.oid = v.tableoid ORDER BY 2"
        ).format(t=sql.Identifier(self.graph, AGE_VERTEX_PARENT))
        seen: set[tuple[str, str, str]] = set()
        batch: list[dict[str, Any]] = []
        for relname, gid, text in _stream(self.read, "age_vertices", query, self.batch_size):
            gid = int(gid)
            row = self._node_row(str(relname), gid, text)
            if row is None:
                self.vertices[gid] = None
                continue
            key = (row["ns"], row["label"], row["nk"])
            self.vertices[gid] = _Vertex(*key)
            counts = self.report.ns(row["ns"])
            counts.age_nodes += 1
            if key in seen:
                counts.dup_nodes += 1
                self._note("duplicate_node", row["ns"], row["label"], gid, row["nk"][:120])
                continue
            seen.add(key)
            batch.append(row)
            if len(batch) >= self.batch_size:
                self._flush_nodes(batch)
        self._flush_nodes(batch)

    def _node_row(self, relname: str, gid: int, text: str | None) -> dict[str, Any] | None:
        try:
            props = parse_agtype(text)
            if not isinstance(props, dict):
                raise ValueError(f"свойства — не карта: {type(props).__name__}")
        except ValueError as exc:
            self._skip_vertex("unparsable_properties", "", relname, gid, str(exc))
            return None
        props = dict(props)
        raw_ns = props.pop("namespace", None)
        ns = raw_ns if isinstance(raw_ns, str) and raw_ns else self.default_ns
        if ns is not raw_ns:
            self._note("namespace_backfilled", ns, relname, gid, f"namespace было {raw_ns!r}")
        raw_nk = props.pop("natural_key", None)
        if isinstance(raw_nk, int) and not isinstance(raw_nk, bool):
            raw_nk = str(raw_nk)
        if not isinstance(raw_nk, str) or not raw_nk:
            self._skip_vertex("no_natural_key", ns, relname, gid, f"natural_key={raw_nk!r}")
            return None
        node_type = props.pop("type") if isinstance(props.get("type"), str) else None
        label = relname
        if relname == AGE_VERTEX_PARENT:  # вершина без метки — метка по виду, как в коде
            label = sanitize_label(node_type)
            self._note("unlabeled_vertex", ns, label, gid, raw_nk[:120])
        try:
            dbmod.check_graph_key(natural_key=raw_nk, label=label)
        except dbmod.GraphKeyTooLong as exc:
            self._skip_vertex(exc.code, ns, label, gid, str(exc))
            return None
        row: dict[str, Any] = {"id": gid, "ns": ns, "label": label, "nk": raw_nk, "type": node_type}
        for col in ("valid_from", "valid_to"):
            row[col] = props.pop(col) if isinstance(props.get(col), str) else None
        row["props"] = props
        return row

    def _skip_vertex(self, kind: str, ns: str, label: str, gid: int, detail: str) -> None:
        counts = self.report.ns(ns or self.default_ns)
        counts.age_nodes += 1
        counts.skipped_nodes += 1
        self._note(kind, ns or self.default_ns, label, gid, detail)

    def _flush_nodes(self, batch: list[dict[str, Any]]) -> None:
        if not batch or self.report.dry_run:
            batch.clear()
            return
        with self.write.transaction(), self.write.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {N} (id, namespace, label, natural_key, type, valid_from, "
                    "valid_to, props) OVERRIDING SYSTEM VALUE "
                    "SELECT r.id, r.ns, r.label, r.nk, r.type, r.valid_from, r.valid_to, "
                    "coalesce(r.props, '{{}}'::jsonb) FROM jsonb_to_recordset(%s) AS r(id bigint, "
                    "ns text, label text, nk text, type text, valid_from text, valid_to text, "
                    "props jsonb) ON CONFLICT DO NOTHING"
                ).format(N=self.N),
                (Jsonb(batch),),
            )
            self.report.nodes_inserted += max(cur.rowcount, 0)
        batch.clear()

    # --- рёбра ---

    def edges(self) -> None:
        query = sql.SQL(
            "SELECT c.relname, e.id::text::bigint, e.start_id::text::bigint, "
            "e.end_id::text::bigint, e.properties::text FROM {t} e "
            "JOIN pg_class c ON c.oid = e.tableoid ORDER BY 3, 4, 2"
        ).format(t=sql.Identifier(self.graph, AGE_EDGE_PARENT))
        seen: set[tuple[Any, ...]] = set()
        batch: list[dict[str, Any]] = []
        for relname, gid, start, end, text in _stream(
            self.read, "age_edges", query, self.batch_size
        ):
            row = self._edge_row(str(relname), int(gid), int(start), int(end), text)
            if row is None:
                continue
            counts = self.report.ns(row["ns"])
            counts.age_edges += 1
            if row["fid"] is not None:
                key: tuple[Any, ...] = ("f", row["ns"], row["fid"])
            else:
                key = ("s", row["sns"], row["sl"], row["sk"], row["type"])
                key += (row["dns"], row["dl"], row["dk"])
            if key in seen:
                counts.dup_edges += 1
                self._note("duplicate_edge", row["ns"], row["type"], int(gid), row["fid"] or "")
                continue
            seen.add(key)
            batch.append(row)
            if len(batch) >= self.batch_size:
                self._flush_edges(batch)
        self._flush_edges(batch)

    def _edge_row(
        self, relname: str, gid: int, start: int, end: int, text: str | None
    ) -> dict[str, Any] | None:
        src, dst = self.vertices.get(start), self.vertices.get(end)
        src_ns = src.ns if src else ""
        try:
            props = parse_agtype(text)
            if not isinstance(props, dict):
                raise ValueError(f"свойства — не карта: {type(props).__name__}")
        except ValueError as exc:
            self._skip_edge("unparsable_properties", src_ns, relname, gid, str(exc))
            return None
        props = dict(props)
        fid = props.get("fact_id") if isinstance(props.get("fact_id"), str) else None
        fact_ns = props.get("namespace") if isinstance(props.get("namespace"), str) else None
        ns = (fact_ns if fid is not None else None) or src_ns
        if relname == AGE_EDGE_PARENT:
            self._skip_edge("unlabeled_edge", ns, relname, gid, "ребро без типа")
            return None
        for end_id, vertex in ((start, src), (end, dst)):
            if vertex is None:
                kind = "endpoint_skipped" if end_id in self.vertices else "orphan_edge"
                self._skip_edge(kind, ns, relname, gid, f"вершина AGE {end_id}")
                return None
        assert src is not None and dst is not None
        try:
            dbmod.check_graph_key(label=relname)
        except dbmod.GraphKeyTooLong as exc:
            self._skip_edge(exc.code, ns, relname, gid, str(exc))
            return None
        if fid is not None:
            props.pop("fact_id")
            if fact_ns is not None:
                props.pop("namespace")
        obs = props.get("observation_ids")
        if isinstance(obs, list) and all(isinstance(o, str) for o in obs):
            props.pop("observation_ids")
        else:
            obs = None
        lost = props.pop("evidence_lost") if isinstance(props.get("evidence_lost"), bool) else False
        row: dict[str, Any] = {
            "id": gid,
            "ns": ns,
            "type": relname,
            "fid": fid,
            "obs": obs,
            "lost": lost,
            "sns": src.ns,
            "sl": src.label,
            "sk": src.nk,
            "dns": dst.ns,
            "dl": dst.label,
            "dk": dst.nk,
        }
        for col in ("valid_from", "valid_to"):
            row[col] = props.pop(col) if isinstance(props.get(col), str) else None
        row["props"] = props
        return row

    def _skip_edge(self, kind: str, ns: str, label: str, gid: int, detail: str) -> None:
        counts = self.report.ns(ns or self.default_ns)
        counts.age_edges += 1
        counts.skipped_edges += 1
        self._note(kind, ns or self.default_ns, label, gid, detail)

    def _flush_edges(self, batch: list[dict[str, Any]]) -> None:
        if not batch or self.report.dry_run:
            batch.clear()
            return
        with self.write.transaction(), self.write.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {E} (id, namespace, type, src_id, dst_id, fact_id, "
                    "observation_ids, valid_from, valid_to, evidence_lost, props) "
                    "OVERRIDING SYSTEM VALUE SELECT r.id, r.ns, r.type, a.id, b.id, r.fid, CASE WHEN r.obs IS NULL THEN NULL "
                    "ELSE ARRAY(SELECT jsonb_array_elements_text(r.obs)) END, r.valid_from, "
                    "r.valid_to, coalesce(r.lost, false), coalesce(r.props, '{{}}'::jsonb) "
                    "FROM jsonb_to_recordset(%s) AS r(id bigint, ns text, type text, fid text, "
                    "obs jsonb, "
                    "lost boolean, sns text, sl text, sk text, dns text, dl text, dk text, "
                    "valid_from text, valid_to text, props jsonb) "
                    "JOIN {N} a ON a.namespace = r.sns AND a.label = r.sl AND a.natural_key = r.sk "
                    "JOIN {N} b ON b.namespace = r.dns AND b.label = r.dl AND b.natural_key = r.dk "
                    "ON CONFLICT DO NOTHING"
                ).format(E=self.E, N=self.N),
                (Jsonb(batch),),
            )
            self.report.edges_inserted += max(cur.rowcount, 0)
        batch.clear()

    def bump_identities(self) -> None:
        """Продвинуть генераторы id таблиц за перенесённые id AGE.

        Новые строки сервиса получают id больше всех перенесённых — порядок по id
        остаётся порядком записи. Только вперёд: повтор переноса генератор не откатывает.
        """
        if self.report.dry_run:
            return
        with self.write.cursor() as cur:
            for table, name in ((self.N, dbmod.NODES_TABLE), (self.E, dbmod.EDGES_TABLE)):
                cur.execute(
                    "SELECT pg_get_serial_sequence(%s, 'id')", (_qualified(self.graph, name),)
                )
                seq = cur.fetchone()[0]
                cur.execute(
                    sql.SQL(
                        "SELECT setval(%(s)s::regclass, greatest(m, "
                        "coalesce(pg_sequence_last_value(%(s)s::regclass), 0))) "
                        "FROM (SELECT max(id) AS m FROM {t}) x WHERE m IS NOT NULL"
                    ).format(t=table),
                    {"s": seq},
                )

    # --- сверка ---

    def count_tables(self) -> None:
        if not dbmod.graph_exists(self.write, self.graph):
            return
        with self.write.cursor() as cur:
            for table, attr in ((self.N, "table_nodes"), (self.E, "table_edges")):
                cur.execute(
                    sql.SQL("SELECT namespace, count(*) FROM {t} GROUP BY 1").format(t=table)
                )
                for ns, cnt in cur.fetchall():
                    setattr(self.report.ns(str(ns)), attr, int(cnt))


def migrate_graph_from_age(
    settings: Settings,
    *,
    dry_run: bool = False,
    batch_size: int = 1000,
    connect: Callable[..., psycopg.Connection] | None = None,
) -> MigrationReport:
    """Перенести граф AGE ``settings.graph_name`` в таблицы графа (см. docstring модуля).

    ``dry_run`` — только прочитать AGE и посчитать (таблицы не создаются и не пишутся).
    :class:`AgeGraphNotFound` — графа AGE нет; :class:`dbmod.GraphSchemaConflict` — в
    схеме метка AGE с именем таблицы графа.
    """
    connect = connect or dbmod.connect
    graph = sanitize_label(settings.graph_name)
    report = MigrationReport(graph=graph, dry_run=dry_run)
    write = connect(settings, autocommit=True)
    read = connect(settings, autocommit=False)
    try:
        read.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        read.read_only = True
        with read.cursor() as cur:
            _require_age_graph(cur, graph)
        report.catalog_warning = catalog_warning(write, graph)
        if dry_run:
            dbmod.graph_schema_version(write, graph)  # только проверка на чужие таблицы
        else:
            dbmod.ensure_graph(write, graph)
        importer = _Importer(settings, read, write, report, batch_size)
        importer.nodes()
        importer.edges()
        read.rollback()
        importer.bump_identities()
        importer.count_tables()
        return report
    finally:
        read.close()
        write.close()

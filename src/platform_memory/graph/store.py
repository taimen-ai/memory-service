"""GraphStore — идемпотентный upsert и обход графа памяти в таблицах PostgreSQL.

Граф — две таблицы схемы ``CB_GRAPH_NAME`` (MEM-ADR-023, DDL — ``core/db.py``):
``graph_nodes`` и ``graph_edges``. Узел — ``(namespace, label, natural_key)``, метка —
значение колонки. Свойства узла — в прежнем виде: служебные поля (``title``,
``source_path``, ``origin``, …) — ключами ``props jsonb``, свободные свойства
frontmatter — вложенной картой под ключом ``props`` этой колонки. Наружу узел
отдаётся словарём свойств, как и раньше: колонки (``natural_key``, ``namespace``,
``type``, ``valid_from``, ``valid_to``) плюс ``props`` таблицы; пустые (NULL) поля не
выдаются — запись ``None`` удаляет свойство.

Upsert — ``INSERT … ON CONFLICT`` по уникальным индексам, без advisory-локов; обход —
итеративно по одному хопу, фронт передаётся массивом (``= ANY(…)``).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections.abc import Callable, Iterable, Sequence
from typing import Any

import psycopg
from psycopg import sql as psql
from psycopg.types.json import Jsonb

from platform_memory.core import db as dbmod
from platform_memory.core.models import Edge, Node, Provenance
from platform_memory.core.namespaces import resolve_namespace, resolve_namespaces
from platform_memory.core.ontology import sanitize_label
from platform_memory.core.scopes import (
    ForeignObjectError,
    check_write_scopes,
    is_visible,
    merge_scopes,
    node_scopes,
    trace_node_scopes,
)


def _jsonable(value: Any) -> Any:
    """Привести значение к JSON-совместимому виду (даты -> ISO-строки и т.п.)."""
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def node_json(alias: str) -> str:
    """SQL-выражение: свойства узла ``alias`` одной картой (колонки + ``props``).

    Колонки со значением NULL в карту не попадают — как свойства, не заданные у узла.
    """
    a = alias
    return (
        f"({a}.props || jsonb_strip_nulls(jsonb_build_object("
        f"'natural_key', {a}.natural_key, 'namespace', {a}.namespace, 'type', {a}.type, "
        f"'valid_from', {a}.valid_from, 'valid_to', {a}.valid_to)))"
    )


def edge_json(alias: str) -> str:
    """SQL-выражение: свойства ребра ``alias`` одной картой (колонки + ``props``).

    ``namespace``, ``fact_id`` и ``observation_ids`` — свойства ребра-факта; у
    структурной связи их нет, как и раньше. ``evidence_lost`` выдаётся, только когда
    свидетельства потеряны.
    """
    a = alias
    return (
        f"({a}.props || jsonb_strip_nulls(jsonb_build_object("
        f"'namespace', CASE WHEN {a}.fact_id IS NOT NULL THEN {a}.namespace END, "
        f"'fact_id', {a}.fact_id, 'observation_ids', to_jsonb({a}.observation_ids), "
        f"'valid_from', {a}.valid_from, 'valid_to', {a}.valid_to, "
        f"'evidence_lost', CASE WHEN {a}.evidence_lost THEN true END)))"
    )


def set_props(values: dict[str, Any]) -> tuple[Jsonb, list[str]]:
    """Семантика ``SET x = v`` для ключей ``props``: заданные заменяются, ``None`` удаляет.

    Вернуть (карту непустых значений, все ключи записи): в SQL —
    ``(props - ключи) || карта``; прочие ключи ``props`` остаются как были.
    """
    return Jsonb({k: v for k, v in values.items() if v is not None}), list(values)


def _gid_nodes(rows: Iterable[Sequence[Any]]) -> list[dict[str, Any]]:
    """Строки (id, свойства) -> свойства узла с ``_gid`` (id строки узла)."""
    out: list[dict[str, Any]] = []
    for gid, props in rows:
        if isinstance(props, dict):
            out.append({**props, "_gid": int(gid)})
    return out


def _edge_valid(rel: dict[str, Any], as_of: str) -> bool:
    """Ребро проходимо на ``as_of``: без ``evidence_lost``, интервал содержит момент.

    Пусто — действующее сейчас (``valid_to`` не задан); ребро без ``valid_from``
    (структурные связи vault) валидно с начала времён.
    """
    if rel.get("evidence_lost") is True:
        return False
    valid_to = rel.get("valid_to")
    if not as_of:
        return valid_to is None
    valid_from = rel.get("valid_from")
    if valid_from not in (None, "") and not str(valid_from) <= as_of:
        return False
    return valid_to is None or str(valid_to) > as_of


# Типы служебного аудит-контура: их нельзя удалять механизмом delete_node,
# иначе самим механизмом удаления можно стереть след удаления.
PROTECTED_TYPES = frozenset({"audit_event", "pc_trace"})


def assert_deletable(node_type: str) -> None:
    """Проверить, что тип узла разрешён к удалению; аудит-контур защищён (ValueError)."""
    if node_type in PROTECTED_TYPES:
        raise ValueError(f"Узлы типа '{node_type}' защищены от удаления (аудит-контур)")


# Ключи ``props``, которые пишет upsert узла (остальные — разметка сообществ и т.п. —
# переживают перезапись).
_NODE_SET_KEYS = ("title", "source_path", "source_id", "confidence", "last_seen", "origin", "props")
_EDGE_SET_KEYS = ("source_path", "last_seen", "origin", "data")


class GraphStore:
    """Граф памяти в таблицах PostgreSQL: схема, upsert узлов/рёбер, обход, статистика."""

    def __init__(self, conn: psycopg.Connection, graph_name: str, default_namespace: str = ""):
        """Сохранить соединение, санитизировать имя графа, зафиксировать default-namespace."""
        self.conn = conn
        # Имя графа — схема PostgreSQL; подставляется только как Identifier.
        self.graph = sanitize_label(graph_name)
        self.default_namespace = resolve_namespace(default_namespace)
        self._nodes = dbmod.nodes_table(self.graph)
        self._edges = dbmod.edges_table(self.graph)
        # Проверка вида сущности на путях записи агента (MEM-ADR-020): callable
        # (namespace, kind, natural_key, attributes) -> каноническое имя вида или
        # UnknownKindError. None — прежнее поведение (любой вид). Подключает
        # ``domain.registry.attach_kind_guard``.
        self.kind_guard: Callable[[str, str, str, dict[str, Any] | None], str] | None = None

    def _ns(self, namespace: str) -> str:
        """Разрешить namespace записи: пустой -> default стора."""
        return resolve_namespace(namespace, self.default_namespace)

    def _nss(self, namespaces: Sequence[str]) -> list[str]:
        """Разрешить список namespaces чтения: пустой -> [default стора]."""
        return resolve_namespaces(namespaces, self.default_namespace)

    def sql(self, text: str) -> psql.Composed:
        """SQL с таблицами графа: ``{N}`` — узлы, ``{E}`` — рёбра."""
        return psql.SQL(text).format(N=self._nodes, E=self._edges)

    def _all(self, query: str, params: Any = None) -> list[tuple[Any, ...]]:
        with self.conn.cursor() as cur:
            cur.execute(self.sql(query), params)
            return cur.fetchall()

    def _run(self, query: str, params: Any = None) -> int:
        """Выполнить запрос записи; вернуть число затронутых строк."""
        with self.conn.cursor() as cur:
            cur.execute(self.sql(query), params)
            return max(cur.rowcount, 0)

    # --- схема ---

    def ensure_schema(self) -> None:
        """Создать схему графа и её таблицы, если их ещё нет (идемпотентно)."""
        dbmod.ensure_graph(self.conn, self.graph)

    def reset(self) -> None:
        """Удалить таблицы графа и создать их заново (полная очистка)."""
        dbmod.drop_graph(self.conn, self.graph)
        dbmod.ensure_graph(self.conn, self.graph)

    # --- upsert ---

    def upsert_node(
        self,
        node: Node,
        *,
        guard_scopes: bool = False,
        allowed_scopes: Sequence[str] | None = None,
    ) -> None:
        """Идемпотентно создать/обновить узел по (namespace, метка, natural_key).

        ``guard_scopes`` — запись через API (MEM-ADR-019): существующий узел с тем же
        ключом вне видимости ``allowed_scopes`` (None — без ограничения) не
        перезаписывается — :class:`ForeignObjectError`; ``props.scopes`` видимого
        узла сливаются по :func:`merge_scopes`, а не заменяются. Без флага —
        прежняя замена ``props`` целиком (ингест vault, служебные узлы).
        """
        label = sanitize_label(node.type)
        dbmod.check_graph_key(natural_key=node.natural_key, label=label)
        prov = node.provenance
        ns = self._ns(node.namespace)
        values: dict[str, Any] = {
            "title": node.title or node.natural_key,
            "source_path": prov.source_path if prov else None,
            "source_id": prov.source_id if prov else None,
            "confidence": prov.confidence if prov else None,
            "last_seen": prov.last_seen if prov else None,
            "origin": node.origin or "vault",
            "props": _jsonable(dict(node.properties)),
        }
        row = {"ns": ns, "label": label, "nk": node.natural_key, "type": node.type or "entity"}
        if not guard_scopes:
            patch, keys = set_props(values)
            self._run(
                "INSERT INTO {N} AS n (namespace, label, natural_key, type, props) "
                "VALUES (%(ns)s, %(label)s, %(nk)s, %(type)s, %(patch)s) "
                "ON CONFLICT (namespace, label, natural_key) DO UPDATE "
                "SET type = EXCLUDED.type, props = (n.props - %(keys)s::text[]) || EXCLUDED.props",
                {**row, "patch": patch, "keys": keys},
            )
            return
        # Проверка видимости и слияние scopes читают узлы ключа и пишут по ним — под
        # блокировкой строк, без advisory-лока. Строка своей метки вставляется первой
        # (пустой): параллельная запись того же узла ждёт на ней до конца транзакции и
        # затем читает уже слитые scopes. Отказ откатывает и эту вставку. Блокировка —
        # ``FOR NO KEY UPDATE``: ключ строки не меняется, и вставка ребра к узлу (её
        # проверка внешнего ключа берёт ``FOR KEY SHARE``) на ней не ждёт.
        with self.conn.transaction():
            created = self._all(
                "INSERT INTO {N} (namespace, label, natural_key, type) "
                "VALUES (%(ns)s, %(label)s, %(nk)s, %(type)s) "
                "ON CONFLICT (namespace, label, natural_key) DO NOTHING RETURNING id",
                row,
            )
            new_id = created[0][0] if created else None
            rows = self._all(
                "SELECT n.id, "
                + node_json("n")
                + " FROM {N} n WHERE n.namespace = %s AND n.natural_key = %s "
                "ORDER BY n.id FOR NO KEY UPDATE",
                (ns, node.natural_key),
            )
            existing = [props for nid, props in rows if nid != new_id]
            values["props"] = self._guarded_props(
                existing, node.natural_key, values["props"], allowed_scopes
            )
            patch, keys = set_props(values)
            self._run(
                "UPDATE {N} n SET type = %(type)s, props = (n.props - %(keys)s::text[]) || %(patch)s "
                "WHERE n.namespace = %(ns)s AND n.label = %(label)s AND n.natural_key = %(nk)s",
                {**row, "patch": patch, "keys": keys},
            )

    @staticmethod
    def _guarded_props(
        existing: list[dict[str, Any]],
        natural_key: str,
        props: dict[str, Any],
        allowed_scopes: Sequence[str] | None,
    ) -> dict[str, Any]:
        """``props`` записи со слитыми scopes; отказ, если узел с ключом чужой или
        запись несёт scope видимости не из ``allowed_scopes`` (:func:`check_write_scopes`).

        Учитываются все узлы ключа в namespace (``existing``), какого бы вида они ни
        были: чтение и удаление по ключу вид не учитывают, а запись другим видом
        создаёт второй узел с тем же ключом — он наследует scopes прежних, а не
        становится общим.
        """
        if any(not is_visible(node_scopes(n), allowed_scopes) for n in existing):
            raise ForeignObjectError(natural_key)
        incoming = node_scopes({"props": props})
        check_write_scopes(incoming, allowed_scopes)
        before = [s for n in existing for s in node_scopes(n)] if existing else None
        merged = merge_scopes(before, incoming)
        out = {k: v for k, v in props.items() if k != "scopes"}
        if merged:
            out["scopes"] = merged
        return out

    def key_visible(
        self, natural_key: str, namespace: str = "", allowed_scopes: Sequence[str] | None = None
    ) -> bool:
        """Видны ли ``allowed_scopes`` все узлы ключа в namespace (нет узлов — True)."""
        if allowed_scopes is None:
            return True
        existing = self._nodes_with_key(natural_key, self._ns(namespace))
        return all(is_visible(node_scopes(n), allowed_scopes) for n in existing)

    def _nodes_with_key(self, natural_key: str, ns: str) -> list[dict[str, Any]]:
        rows = self._all(
            "SELECT " + node_json("n") + " FROM {N} n "
            "WHERE n.namespace = %s AND n.natural_key = %s ORDER BY n.id",
            (ns, natural_key),
        )
        return [r[0] for r in rows]

    # --- батчевые операции сверки снимков (MEM-ADR-020) ---

    def upsert_entities(
        self,
        kind: str,
        rows: Sequence[dict[str, Any]],
        namespace: str,
        *,
        valid_from: str,
        last_seen: str,
    ) -> int:
        """Батч-upsert узлов одного вида одним запросом: ``rows`` — ``{k, t, sp, props}``.

        Узел открывается (``valid_to`` снимается), ``valid_from`` ставится при первом
        появлении. ``origin='agent'`` — запись агента, как у FactStore.ensure_entity.
        Повтор ключа в ``rows`` — действует последняя строка.
        """
        if not rows:
            return 0
        label = sanitize_label(kind)
        dbmod.check_graph_key(label=label)
        items: dict[str, Any] = {}
        keys: list[str] = []
        for r in rows:
            dbmod.check_graph_key(natural_key=str(r["k"]))
            values = {
                "title": r.get("t"),
                "source_path": r.get("sp"),
                "confidence": 1.0,
                "last_seen": last_seen,
                "origin": "agent",
                "props": _jsonable(r.get("props")),
            }
            patch, keys = set_props(values)
            items[str(r["k"])] = {"k": str(r["k"]), "patch": patch.obj}
        return self._run(
            "INSERT INTO {N} AS n (namespace, label, natural_key, type, valid_from, props) "
            "SELECT %(ns)s, %(label)s, r.k, %(type)s, %(vf)s, r.patch "
            "FROM jsonb_to_recordset(%(rows)s) AS r(k text, patch jsonb) "
            "ON CONFLICT (namespace, label, natural_key) DO UPDATE SET type = EXCLUDED.type, "
            "valid_from = coalesce(n.valid_from, EXCLUDED.valid_from), valid_to = NULL, "
            "props = (n.props - %(keys)s::text[]) || EXCLUDED.props",
            {
                "ns": self._ns(namespace),
                "label": label,
                "type": kind or "entity",
                "vf": valid_from,
                "rows": Jsonb(list(items.values())),
                "keys": keys,
            },
        )

    def close_entities(
        self, kind: str, natural_keys: Sequence[str], namespace: str, *, valid_to: str
    ) -> int:
        """Закрыть интервал валидности узлов одного вида (узлы не удаляются)."""
        keys = [k for k in dict.fromkeys(natural_keys) if k]
        if not keys:
            return 0
        return self._run(
            "UPDATE {N} SET valid_to = %s "
            "WHERE namespace = %s AND label = %s AND natural_key = ANY(%s::text[])",
            (valid_to, self._ns(namespace), sanitize_label(kind), keys),
        )

    def existing_keys(self, kind: str, natural_keys: Sequence[str], namespace: str) -> set[str]:
        """Какие из ключей есть узлами вида ``kind`` в namespace (любой интервал).

        Выборка — по уникальному индексу ``(namespace, label, natural_key)``.
        """
        keys = [k for k in dict.fromkeys(natural_keys) if k]
        if not keys:
            return set()
        rows = self._all(
            "SELECT natural_key FROM {N} "
            "WHERE namespace = %s AND label = %s AND natural_key = ANY(%s::text[])",
            (self._ns(namespace), sanitize_label(kind), keys),
        )
        return {str(r[0]) for r in rows}

    def upsert_nodes(self, nodes: Iterable[Node]) -> int:
        """Upsert набора узлов; вернуть количество обработанных."""
        return sum(1 for node in nodes if (self.upsert_node(node) or True))

    def upsert_edge(self, edge: Edge, last_seen: str | None = None) -> bool:
        """Создать/обновить ребро между существующими узлами. False — если узлы не найдены.

        Оба конца ребра ищутся в ОДНОМ namespace (v1: рёбра не пересекают namespaces)
        по ключу, без учёта вида: у ключа с узлами нескольких видов ребро получает
        каждый из них. Структурная связь уникальна по ``(src_id, type, dst_id)``.
        """
        ns = self._ns(edge.namespace)
        etype = sanitize_label(edge.type).upper()
        dbmod.check_graph_key(label=etype)
        patch, keys = set_props(
            {
                "source_path": edge.source_path,
                "last_seen": last_seen,
                "origin": edge.origin or "vault",
                "data": _jsonable(dict(edge.properties)),
            }
        )
        rows = self._all(
            "INSERT INTO {E} AS e (namespace, type, src_id, dst_id, props) "
            "SELECT %(ns)s, %(type)s, a.id, b.id, %(patch)s FROM {N} a "
            "JOIN {N} b ON b.namespace = a.namespace AND b.natural_key = %(dst)s "
            "WHERE a.namespace = %(ns)s AND a.natural_key = %(src)s "
            "ON CONFLICT (src_id, type, dst_id) WHERE fact_id IS NULL DO UPDATE "
            "SET props = (e.props - %(keys)s::text[]) || EXCLUDED.props RETURNING e.id",
            {
                "ns": ns,
                "type": etype,
                "src": edge.src,
                "dst": edge.dst,
                "patch": patch,
                "keys": keys,
            },
        )
        return bool(rows)

    def upsert_edges(self, edges: Iterable[Edge], last_seen: str | None = None) -> tuple[int, int]:
        """Вернуть (создано/обновлено, пропущено из-за отсутствующих узлов)."""
        ok = skipped = 0
        for edge in edges:
            if self.upsert_edge(edge, last_seen):
                ok += 1
            else:
                skipped += 1
        return ok, skipped

    def prune(self, run_token: str, namespace: str = "") -> tuple[int, int]:
        """Mark-and-sweep: удалить ТОЛЬКО vault-узлы/рёбра namespace, не виденные в прогоне.

        Делает полный ingest самоочищающимся (удалённые файлы, исчезнувшие связи,
        «осиротевшие» при смене типа узлы). Скоупится одним namespace — ре-ingest vault
        никогда не подметает данные других namespaces (тенантские данные).
        Узлы/рёбра origin='agent' (write-back агентов) НЕ подметаются; рёбра удалённого
        узла уходят вместе с ним (``on delete cascade``). Возвращает (узлов, рёбер).
        """
        ns = self._ns(namespace)
        # vault-происхождение: origin отсутствует (старые) или = 'vault'.
        stale = (
            "namespace = %(ns)s "
            "AND (props ->> 'last_seen' IS NULL OR props ->> 'last_seen' <> %(run)s) "
            "AND (props ->> 'origin' IS NULL OR props ->> 'origin' = 'vault')"
        )
        params = {"ns": ns, "run": run_token}
        edges = self._run("DELETE FROM {E} WHERE " + stale, params)
        nodes = self._run("DELETE FROM {N} WHERE " + stale, params)
        return nodes, edges

    # --- write-back (факты авторства агента) ---

    def write_fact(
        self,
        natural_key: str,
        node_type: str,
        title: str,
        *,
        properties: dict[str, Any] | None = None,
        links: list[str] | None = None,
        run_id: str | None = None,
        confidence: float = 0.8,
        source: str | None = None,
        trace_id: str | None = None,
        namespace: str = "",
        allowed_scopes: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Записать факт авторства агента (origin='agent'). Переживает ре-ingest (не подметается).

        Перезапись существующего узла проверяет видимость ``allowed_scopes`` и сливает
        ``props.scopes`` (``upsert_node(guard_scopes=True)``, MEM-ADR-019); ``links`` к
        невидимым узлам не создаются, как к отсутствующим.

        Замыкает цикл «mini-app читают И пишут в Brain». Рёбра LINKS_TO создаются к уже
        существующим узлам-целям. Если задан ``trace_id`` — факт привязывается ребром
        ``IN_TRACE`` к якорю трейса задачи (recall→действия→retain в одном подграфе).
        Запись в vault при этом НЕ производится.
        """
        now = dt.datetime.now().isoformat()
        src = source or f"agent:run/{run_id or '-'}"
        ns = self._ns(namespace)
        if self.kind_guard is not None:
            node_type = self.kind_guard(ns, node_type, natural_key, properties)
        node = Node(
            type=node_type,
            natural_key=natural_key,
            title=title,
            properties=properties or {},
            origin="agent",
            namespace=ns,
            provenance=Provenance(
                source_path=src, source_id=run_id, confidence=confidence, last_seen=now
            ),
        )
        self.upsert_node(node, guard_scopes=True, allowed_scopes=allowed_scopes)
        edges_made = 0
        for dst in links or []:
            if not self.key_visible(dst, ns, allowed_scopes):
                continue  # невидимая цель — как отсутствующая: счётчик её не выдаёт
            edge = Edge(
                type="LINKS_TO",
                src=natural_key,
                dst=dst,
                origin="agent",
                source_path=src,
                namespace=ns,
            )
            if self.upsert_edge(edge, now):
                edges_made += 1
        linked_trace = None
        if trace_id:
            anchor = self.ensure_trace_anchor(trace_id, namespace=ns)
            self.upsert_edge(
                Edge(
                    type="IN_TRACE",
                    src=natural_key,
                    dst=anchor,
                    origin="agent",
                    source_path=src,
                    namespace=ns,
                ),
                now,
            )
            linked_trace = anchor
        return {
            "natural_key": natural_key,
            "namespace": ns,
            "origin": "agent",
            "edges": edges_made,
            "trace": linked_trace,
        }

    # --- аудит и трейсы (подграф трейса задачи) ---

    @staticmethod
    def trace_anchor_key(trace_id: str) -> str:
        """natural_key узла-якоря трейса задачи (trace_id = issueId/runId)."""
        return f"pc-trace:{trace_id}"

    def ensure_trace_anchor(
        self, trace_id: str, *, title: str | None = None, namespace: str = ""
    ) -> str:
        """Идемпотентно создать узел-якорь трейса задачи (origin='agent'). Вернуть natural_key.

        К этому якорю привязываются события аудита (``IN_TRACE``) и сохранённые факты —
        так формируется подграф трейса конкретной задачи Paperclip.
        """
        nk = self.trace_anchor_key(trace_id)
        now = dt.datetime.now().isoformat()
        self.upsert_node(
            Node(
                type="pc_trace",
                natural_key=nk,
                title=title or f"Трейс {trace_id}",
                properties={"trace_id": trace_id},
                origin="agent",
                namespace=self._ns(namespace),
                provenance=Provenance(
                    source_path=f"paperclip:trace/{trace_id}",
                    source_id=trace_id,
                    confidence=1.0,
                    last_seen=now,
                ),
            )
        )
        return nk

    def record_audit(
        self,
        trace_id: str,
        actor: str,
        action: str,
        *,
        payload: dict[str, Any] | None = None,
        ts: str | None = None,
        run_id: str | None = None,
        actor_id: str | None = None,
        actor_kind: str = "agent",
        namespace: str = "",
    ) -> dict[str, Any]:
        """Записать событие аудита как узел ``audit_event``, привязанный к якорю трейса.

        Идемпотентно по содержимому (natural_key = ``audit:{trace}:{hash}``) — повторная
        доставка одного события не плодит дубликаты. Если задан ``actor_id`` — событие
        связывается ребром ``PERFORMED_BY`` с узлом-актором (``pc_agent``). origin='agent'.
        Весь подграф трейса живёт в одном namespace вызывающего.
        """
        ts = ts or dt.datetime.now().isoformat()
        ns = self._ns(namespace)
        anchor = self.ensure_trace_anchor(trace_id, namespace=ns)
        digest_src = json.dumps(
            {
                "a": action,
                "t": ts,
                "actor": actor,
                "actor_id": actor_id,
                "kind": actor_kind,
                "run": run_id,
                "p": _jsonable(payload or {}),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        digest = hashlib.sha1(digest_src.encode("utf-8")).hexdigest()[:16]
        nk = f"audit:{trace_id}:{digest}"
        props: dict[str, Any] = {
            "action": action,
            "actor": actor,
            "actor_kind": actor_kind,
            "ts": ts,
            "trace_id": trace_id,
        }
        if run_id:
            props["run_id"] = run_id
        if payload is not None:
            props["payload"] = _jsonable(payload)
        self.upsert_node(
            Node(
                type="audit_event",
                natural_key=nk,
                title=action,
                properties=props,
                origin="agent",
                namespace=ns,
                provenance=Provenance(
                    source_path=f"paperclip:trace/{trace_id}",
                    source_id=run_id or trace_id,
                    confidence=1.0,
                    last_seen=ts,
                ),
            )
        )
        self.upsert_edge(
            Edge(
                type="IN_TRACE",
                src=nk,
                dst=anchor,
                origin="agent",
                source_path=f"paperclip:trace/{trace_id}",
                namespace=ns,
            ),
            ts,
        )
        if actor_id:
            agent_nk = f"pc-agent:{actor_id}"
            self.upsert_node(
                Node(
                    type="pc_agent",
                    natural_key=agent_nk,
                    title=actor or actor_id,
                    properties={"actor_id": actor_id, "kind": actor_kind},
                    origin="agent",
                    namespace=ns,
                    provenance=Provenance(
                        source_path=f"paperclip:agent/{actor_id}",
                        source_id=actor_id,
                        confidence=1.0,
                        last_seen=ts,
                    ),
                )
            )
            self.upsert_edge(
                Edge(
                    type="PERFORMED_BY",
                    src=nk,
                    dst=agent_nk,
                    origin="agent",
                    source_path=f"paperclip:trace/{trace_id}",
                    namespace=ns,
                ),
                ts,
            )
        return {"natural_key": nk, "namespace": ns, "trace": anchor, "action": action, "ts": ts}

    def trace_subgraph(
        self,
        trace_id: str,
        *,
        limit: int = 200,
        namespace: str = "",
        allowed_scopes: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Вернуть подграф трейса задачи: события аудита (по времени) и привязанные факты.

        Источник для проверки сквозного сценария и для UI «Память и трейс по задаче».
        ``allowed_scopes`` (не None) — видимость вызывающего (MEM-ADR-019): узлы вне
        неё отбрасываются до ``limit``. Scope факта — его ``props.scopes``, события
        аудита — ещё и ``props.payload.scopes`` (scopes удалённого объекта).
        Фильтр — в Python (``trace_node_scopes`` + ``is_visible``), а не в SQL:
        payload события приходит от клиента, и предикат по произвольному JSON
        (объект, строка вместо списка) падал бы на весь трейс.
        """
        anchor = self.trace_anchor_key(trace_id)
        ns = self._ns(namespace)
        # С фильтром видимости лимит считается после него — в цикле ниже.
        sql_limit = f" LIMIT {int(limit)}" if allowed_scopes is None else ""
        query = self.sql(
            "SELECT " + node_json("n") + " FROM {E} e "
            "JOIN {N} a ON a.id = e.dst_id JOIN {N} n ON n.id = e.src_id "
            "WHERE e.type = 'IN_TRACE' AND a.namespace = %s AND a.natural_key = %s "
            "ORDER BY e.id" + sql_limit
        )
        events: list[dict[str, Any]] = []
        facts: list[dict[str, Any]] = []
        with self.conn.cursor() as cur:
            cur.execute(query, (ns, anchor))
            for (parsed,) in cur:
                if not isinstance(parsed, dict):
                    continue
                if not is_visible(trace_node_scopes(parsed), allowed_scopes):
                    continue
                if parsed.get("type") == "audit_event":
                    events.append(parsed)
                else:
                    facts.append(parsed)
                if len(events) + len(facts) >= limit:
                    break

        def _ts(node: dict[str, Any]) -> str:
            inner = node.get("props")
            return str(inner.get("ts", "")) if isinstance(inner, dict) else ""

        events.sort(key=_ts)
        return {
            "trace_id": trace_id,
            "anchor": anchor,
            "events": events,
            "facts": facts,
            "event_count": len(events),
            "fact_count": len(facts),
        }

    # --- чтение / обход ---

    def get_node(self, natural_key: str, namespaces: Sequence[str] = ()) -> dict[str, Any] | None:
        """Вернуть свойства узла по natural_key в области видимости или None, если не найден."""
        rows = self._all(
            "SELECT " + node_json("n") + " FROM {N} n "
            "WHERE n.namespace = ANY(%s::text[]) AND n.natural_key = %s ORDER BY n.id LIMIT 1",
            (self._nss(namespaces), natural_key),
        )
        return rows[0][0] if rows else None

    def delete_node(
        self,
        natural_key: str,
        namespace: str = "",
        *,
        allowed_scopes: Sequence[str] | None = None,
    ) -> dict[str, Any] | None:
        """Удалить узел вместе с рёбрами (``on delete cascade``); вернуть снапшот свойств.

        None — если узла нет или он вне видимости ``allowed_scopes`` (MEM-ADR-019;
        None — без ограничения): невидимый узел не удаляется и неотличим от
        отсутствующего. Защищённые типы (PROTECTED_TYPES) не удаляет — ValueError.
        Чанки индекса не трогает (слой VectorIndex); аудит пишет вызывающий — снапшот
        возвращается именно для payload аудит-события.
        """
        ns = self._ns(namespace)
        snapshot = self.get_node(natural_key, namespaces=[ns])
        if snapshot is None or not is_visible(node_scopes(snapshot), allowed_scopes):
            return None
        assert_deletable(str(snapshot.get("type") or ""))
        self._run("DELETE FROM {N} WHERE namespace = %s AND natural_key = %s", (ns, natural_key))
        return snapshot

    def _hop(self, ids: Iterable[int]) -> list[tuple[int, int]]:
        """Один хоп по рёбрам в обе стороны: пары (откуда, куда) для вершин ``ids``."""
        frontier = sorted(set(ids))
        if not frontier:
            return []
        rows = self._all(
            "SELECT src_id, dst_id FROM {E} WHERE src_id = ANY(%(ids)s::bigint[]) "
            "UNION ALL SELECT dst_id, src_id FROM {E} WHERE dst_id = ANY(%(ids)s::bigint[])",
            {"ids": frontier},
        )
        return [(int(a), int(b)) for a, b in rows]

    def _nodes_by_ids(
        self, ids: Iterable[int], namespaces: Sequence[str] | None = None
    ) -> dict[int, dict[str, Any]]:
        """Узлы по id одним запросом; ``namespaces`` — только узлы этих namespaces."""
        wanted = sorted(set(ids))
        if not wanted:
            return {}
        query = "SELECT n.id, " + node_json("n") + " FROM {N} n WHERE n.id = ANY(%s::bigint[])"
        params: list[Any] = [wanted]
        if namespaces is not None:
            query += " AND n.namespace = ANY(%s::text[])"
            params.append(list(namespaces))
        return {n["_gid"]: n for n in _gid_nodes(self._all(query, params))}

    def expand_neighbors(
        self,
        natural_keys: list[str],
        hops: int = 1,
        limit: int = 40,
        namespaces: Sequence[str] = (),
    ) -> list[dict[str, Any]]:
        """Соседи узлов в пределах ``hops`` (1–2) внутри области видимости namespaces.

        Обход — итеративно по одному хопу (фронт — массивом id), рёбра в обе стороны.
        Namespace проверяется на ОБОИХ концах пути — обход не выходит за пределы
        разрешённых namespaces. Сосед с тем же ключом, что у стартового узла, от
        которого он достигнут, не выдаётся. Порядок — по хопу, затем по id узла.
        """
        if not natural_keys:
            return []
        hops = max(1, min(hops, 2))
        nss = self._nss(namespaces)
        starts = self._all(
            "SELECT id, natural_key FROM {N} "
            "WHERE namespace = ANY(%s::text[]) AND natural_key = ANY(%s::text[])",
            (nss, list(dict.fromkeys(natural_keys))),
        )
        # Вершина -> ключи стартовых узлов, от которых она достигнута.
        frontier: dict[int, set[str]] = {int(i): {str(k)} for i, k in starts}
        reached: dict[int, set[str]] = {}
        for _ in range(hops):
            nxt: dict[int, set[str]] = {}
            for src, dst in self._hop(frontier):
                nxt.setdefault(dst, set()).update(frontier[src])
            for gid in sorted(nxt):
                reached.setdefault(gid, set()).update(nxt[gid])
            frontier = nxt
            if not frontier:
                break
        nodes = self._nodes_by_ids(reached, nss)
        out: list[dict[str, Any]] = []
        for gid, origins in reached.items():
            node = nodes.get(gid)
            if node is None or not origins - {str(node.get("natural_key"))}:
                continue
            node.pop("_gid", None)
            out.append(node)
            if len(out) >= limit:
                break
        return out

    # --- типизированный обход (MEM-ADR-020) ---

    def nodes_by_keys(
        self, natural_keys: Sequence[str], namespaces: Sequence[str] = ()
    ) -> list[dict[str, Any]]:
        """Узлы с точным natural_key из списка в области видимости namespaces.

        Любого вида (индекс ``(namespace, natural_key)``). У узла — ``_gid`` (id строки).
        """
        keys = [k for k in dict.fromkeys(natural_keys) if k]
        if not keys:
            return []
        rows = self._all(
            "SELECT n.id, " + node_json("n") + " FROM {N} n "
            "WHERE n.namespace = ANY(%s::text[]) AND n.natural_key = ANY(%s::text[]) ORDER BY n.id",
            (self._nss(namespaces), keys),
        )
        return _gid_nodes(rows)

    def graph_labels(self) -> dict[int, tuple[str, str]]:
        """Метки графа: порядковый номер -> (имя, ``v``|``e``): виды узлов и типы рёбер.

        Перебор различных ``(namespace, метка)`` по индексу (рекурсивный «skip scan»):
        шагов столько, сколько различных пар, таблицы целиком не читаются.
        """
        rows = self._all(
            "WITH RECURSIVE nl AS ("
            " (SELECT namespace, label FROM {N} ORDER BY namespace, label LIMIT 1)"
            " UNION ALL SELECT q.namespace, q.label FROM nl, LATERAL ("
            "  SELECT namespace, label FROM {N} WHERE (namespace, label) > (nl.namespace, nl.label)"
            "  ORDER BY namespace, label LIMIT 1) q"
            "), el AS ("
            " (SELECT namespace, type FROM {E} ORDER BY namespace, type LIMIT 1)"
            " UNION ALL SELECT q.namespace, q.type FROM el, LATERAL ("
            "  SELECT namespace, type FROM {E} WHERE (namespace, type) > (el.namespace, el.type)"
            "  ORDER BY namespace, type LIMIT 1) q"
            ") SELECT label, 'v' FROM nl UNION SELECT type, 'e' FROM el ORDER BY 2 DESC, 1"
        )
        return {i: (str(name), str(kind)) for i, (name, kind) in enumerate(rows, start=1)}

    def lookup_nodes(
        self,
        keys_by_label: dict[str, Iterable[str]],
        aliases_by_label: dict[str, Iterable[str]] | None = None,
        namespaces: Sequence[str] = (),
    ) -> list[dict[str, Any]]:
        """Узлы по точному ключу и по псевдониму — одним запросом, только в заданных метках.

        ``keys_by_label`` — метка -> ключи (уникальный индекс ``(namespace, label,
        natural_key)``), ``aliases_by_label`` — метка -> строки, которые должны быть
        среди ``props.aliases`` (вхождение ``props @> …`` — GIN по ``props``). Метка,
        которой нет в графе, ничего не находит. У узла — ``_gid``; узел, найденный
        несколькими ветвями, — один раз.
        """
        branches: list[str] = []
        params: dict[str, Any] = {"nss": self._nss(namespaces)}
        for kind, group in (("k", keys_by_label), ("a", aliases_by_label or {})):
            labels: list[str] = []
            values: list[str] = []
            for label in sorted(group):
                for v in dict.fromkeys(group[label]):
                    if v:
                        labels.append(sanitize_label(label))
                        values.append(str(v))
            if not values:
                continue
            params[f"{kind}l"], params[f"{kind}v"] = labels, values
            match = (
                "n.natural_key = q.v"
                if kind == "k"
                else "n.props @> jsonb_build_object('props', "
                "jsonb_build_object('aliases', jsonb_build_array(q.v)))"
            )
            branches.append(
                "SELECT n.id, " + node_json("n") + " FROM {N} n "
                f"JOIN unnest(%({kind}l)s::text[], %({kind}v)s::text[]) AS q(label, v) "
                f"ON n.label = q.label AND {match} WHERE n.namespace = ANY(%(nss)s::text[])"
            )
        if not branches:
            return []
        nodes = _gid_nodes(self._all(" UNION ALL ".join(branches), params))
        return list({n["_gid"]: n for n in nodes}.values())

    def typed_neighbors(
        self,
        start: dict[int, str],
        relation: str,
        directions: Sequence[str],
        *,
        labels: dict[int, tuple[str, str]],
        as_of: str = "",
        max_edges: int = 20000,
    ) -> tuple[list[tuple[int, str, dict[str, Any], dict[str, Any], int]], bool]:
        """Рёбра типа ``relation`` у стартовых вершин и узлы на других концах (MEM-ADR-020).

        ``start`` — id вершины -> её namespace; ``directions`` — из ``out``/``in``
        (``out`` — вершина начало ребра). Два запроса на все стартовые вершины: рёбра —
        по индексам концов ``(src_id, type)``/``(dst_id, type)``, концы — по id.
        ``labels`` — ``graph_labels()``: связи, которой нет среди типов рёбер, не
        ищем. Проходим только по рёбрам, валидным на ``as_of`` (``valid_from <= t <
        valid_to``; пусто — действующие сейчас, ``valid_to IS NULL``), без
        ``evidence_lost`` и с концом в том же namespace.

        Валидность, ``evidence_lost`` и namespace ребра проверяются в SQL до лимита:
        закрытые версии рёбер хаба не вытесняют действующие. Конец сверяется с
        namespace старта ещё раз по загруженному узлу. Действующих рёбер читается не
        больше ``max_edges`` (по возрастанию id). Вернуть ``([(id_откуда, направление,
        свойства_узла_куда, свойства_ребра, id_ребра)], усечено)``; у узла — ``_gid``;
        ``усечено`` — рёбер больше ``max_edges``, часть не прочитана.
        """
        if not start:
            return [], False
        dirs = [d for d in ("out", "in") if d in directions]
        if len(dirs) != len(set(directions)):
            raise ValueError(f"direction должен быть out|in: {list(directions)!r}")
        edge_type = sanitize_label(relation).upper()
        if (edge_type, "e") not in labels.values():
            return [], False
        ends = [
            f"(e.{'src_id' if d == 'out' else 'dst_id'} = ANY(%(ids)s::bigint[]) "
            "AND e.type = %(type)s)"
            for d in dirs
        ]
        cond = [
            "(" + " OR ".join(ends) + ")",
            "NOT e.evidence_lost",
            "(e.namespace = %(nsmap)s::jsonb ->> e.src_id::text "
            "OR e.namespace = %(nsmap)s::jsonb ->> e.dst_id::text)",
        ]
        params: dict[str, Any] = {
            "ids": sorted(start),
            "type": edge_type,
            "nsmap": Jsonb({str(g): ns for g, ns in start.items()}),
            # На строку больше лимита: так видно, что рёбер больше, чем прочитано.
            "lim": int(max_edges) + 1,
        }
        if as_of:
            cond.append("(coalesce(e.valid_from, '') = '' OR e.valid_from <= %(t)s)")
            cond.append("(e.valid_to IS NULL OR e.valid_to > %(t)s)")
            params["t"] = as_of
        else:
            cond.append("e.valid_to IS NULL")
        raw = self._all(
            "SELECT e.id, e.src_id, e.dst_id, " + edge_json("e") + " FROM {E} e "
            "WHERE " + " AND ".join(cond) + " ORDER BY e.id LIMIT %(lim)s",
            params,
        )
        truncated = len(raw) > max_edges
        edges: list[tuple[int, str, int, dict[str, Any], int]] = []
        for rid, sid, eid, rel in raw[:max_edges]:
            rel = rel if isinstance(rel, dict) else {}
            if not _edge_valid(rel, as_of):
                continue
            s_id, e_id = int(sid), int(eid)
            if "out" in dirs and s_id in start:
                edges.append((s_id, "out", e_id, rel, int(rid)))
            if "in" in dirs and e_id in start:
                edges.append((e_id, "in", s_id, rel, int(rid)))
        nodes = self._nodes_by_ids(dst for _, _, dst, _, _ in edges)
        out: list[tuple[int, str, dict[str, Any], dict[str, Any], int]] = []
        for src, direction, dst, rel, rid in edges:
            node = nodes.get(dst)
            if node is not None and node.get("namespace") == start[src]:
                out.append((src, direction, node, rel, rid))
        return out, truncated

    def concept_subgraph(
        self,
        seed_keys: list[str],
        namespaces: Sequence[str] = (),
        max_related: int = 8,
    ) -> dict[str, list[dict[str, Any]]]:
        """Подграф отбора для визуализации: seed-статьи → их концепты → связанные статьи.

        Возвращает ``{"nodes": [{natural_key,type,title,role}], "edges": [{src,dst}]}``,
        где role ∈ {seed, concept, related}. Показывает, что статьи связаны через общие
        концепты графа (COVERS), а не выбраны только вектором. Пусто, если рёбер нет.
        """
        empty: dict[str, list[dict[str, Any]]] = {"nodes": [], "edges": []}
        if not seed_keys:
            return empty
        nss = self._nss(namespaces)
        nodes: dict[str, dict[str, Any]] = {}
        edges: list[dict[str, str]] = []

        def _add(props: Any, role: str) -> None:
            if not isinstance(props, dict):
                return
            nk = props.get("natural_key")
            if not nk:
                return
            if nk not in nodes:
                nodes[nk] = {
                    "natural_key": nk,
                    "type": props.get("type", ""),
                    "title": props.get("title", ""),
                    "role": role,
                }
            elif role == "seed":
                nodes[nk]["role"] = "seed"  # seed важнее related при пересечении

        covers = (
            "SELECT " + node_json("a") + ", " + node_json("c") + " FROM {E} e "
            "JOIN {N} a ON a.id = e.src_id JOIN {N} c ON c.id = e.dst_id "
            "WHERE e.type = 'COVERS' AND "
        )
        # 1) seed-статьи → их концепты
        for a, c in self._all(
            covers + "a.natural_key = ANY(%(keys)s::text[]) AND a.namespace = ANY(%(nss)s::text[]) "
            "AND c.namespace = ANY(%(nss)s::text[]) ORDER BY e.id",
            {"keys": list(seed_keys), "nss": nss},
        ):
            _add(a, "seed")
            _add(c, "concept")
            if isinstance(a, dict) and isinstance(c, dict):
                edges.append({"src": a["natural_key"], "dst": c["natural_key"]})

        concept_keys = [k for k, v in nodes.items() if v["type"] == "concept"]
        if not concept_keys:
            return {"nodes": list(nodes.values()), "edges": edges}

        # 2) те же концепты → связанные статьи (не seed), ограничение по числу
        seeds = {k for k, v in nodes.items() if v["role"] == "seed"}
        related_count = 0
        per_concept: dict[str, int] = {}  # не даём одному концепту залить панель
        for b, c in self._all(
            covers
            + "c.natural_key = ANY(%(ckeys)s::text[]) AND a.namespace = ANY(%(nss)s::text[]) "
            "AND a.type = 'article' ORDER BY e.id LIMIT 200",
            {"ckeys": concept_keys, "nss": nss},
        ):
            if not (isinstance(b, dict) and isinstance(c, dict)):
                continue
            bk, ck = b.get("natural_key"), c.get("natural_key")
            if not bk or not ck or bk in seeds:
                continue
            if bk not in nodes:
                # новый связанный узел: лимит на концепт (≤2) и общий (max_related)
                if related_count >= max_related or per_concept.get(ck, 0) >= 2:
                    continue
                related_count += 1
                per_concept[ck] = per_concept.get(ck, 0) + 1
                _add(b, "related")
            edges.append({"src": bk, "dst": ck})
        return {"nodes": list(nodes.values()), "edges": edges}

    def neighborhood_subgraph(
        self,
        seed_keys: list[str],
        namespaces: Sequence[str] = (),
        hops: int = 1,
        limit: int = 55,
        per_seed: int = 10,  # оставлен для совместимости вызова; набор режется общим limit
    ) -> dict[str, list[dict[str, Any]]]:
        """Общая окрестность seed-узлов для визуализации графа компании (любые типы).

        Возвращает ``{"nodes": [{natural_key,type,title,role,hop}], "edges":
        [{src,dst,type}]}``. role: seed | neighbor; hop: 0 для seed, 1/2 для окрестности.
        Показывает типизированный подграф (люди, решения, проекты, концепты…), а не только
        концепты — retrieval вытаскивает связанную память по рёбрам. Изоляция по namespace.
        """
        empty: dict[str, list[dict[str, Any]]] = {"nodes": [], "edges": []}
        if not seed_keys:
            return empty
        nss = self._nss(namespaces)
        seeds = list(dict.fromkeys(seed_keys))
        seed_set = set(seeds)

        # 1) Набор узлов окрестности — постадийным обходом hop-за-hop (каждый уровень —
        #    hops=1 expand_neighbors от фронтира): на хаб-узлах (концепты связаны с
        #    десятками статей/людей) набор растёт быстро, а фронтир и набор режутся.
        node_keys = list(seeds)
        seen = set(seeds)
        frontier = list(seeds)
        # Фронтир hop2 ограничиваем: расширяться от десятков хабов не нужно, набор всё равно
        # режется общим limit, а меньший фронтир — меньше работы обходу.
        _FRONTIER_CAP = 16
        for _ in range(max(1, min(hops, 2))):
            if not frontier or len(node_keys) >= limit:
                break
            nbrs = self.expand_neighbors(
                frontier[:_FRONTIER_CAP], hops=1, limit=limit, namespaces=namespaces
            )
            frontier = []
            for n in nbrs:
                nk = str(n.get("natural_key", ""))
                if nk and nk not in seen:
                    seen.add(nk)
                    node_keys.append(nk)
                    frontier.append(nk)
                    if len(node_keys) >= limit:
                        break
        if not node_keys:
            return empty

        # 2) Свойства узлов (type/title) — один запрос по всему набору ключей.
        meta: dict[str, dict[str, str]] = {}
        params = {"keys": node_keys, "nss": nss}
        for k, t, ti in self._all(
            "SELECT natural_key, type, props ->> 'title' FROM {N} "
            "WHERE natural_key = ANY(%(keys)s::text[]) AND namespace = ANY(%(nss)s::text[]) "
            "ORDER BY id",
            params,
        ):
            if k:
                meta[str(k)] = {"type": str(t or "entity"), "title": str(ti or "")}

        # 3) Рёбра среди известного набора (оба конца в node_keys), каждое — один раз:
        #    рёбра хранятся направленными.
        edges: list[dict[str, str]] = []
        seen_edge: set[frozenset] = set()
        for ak, rel, bk in self._all(
            "SELECT a.natural_key, e.type, b.natural_key FROM {E} e "
            "JOIN {N} a ON a.id = e.src_id JOIN {N} b ON b.id = e.dst_id "
            "WHERE a.natural_key = ANY(%(keys)s::text[]) AND b.natural_key = ANY(%(keys)s::text[]) "
            "AND a.namespace = ANY(%(nss)s::text[]) AND a.natural_key <> b.natural_key "
            "ORDER BY e.id",
            params,
        ):
            ak, bk = str(ak or ""), str(bk or "")
            if not ak or not bk:
                continue
            etype = str(rel).upper() if rel else "REL"
            sig = frozenset((ak, bk, etype))  # схлопнуть неориентированный дубль
            if sig in seen_edge:
                continue
            seen_edge.add(sig)
            edges.append({"src": ak, "dst": bk, "type": etype})

        # 4) hop (0 для seed) — BFS от seed по неориентированным рёбрам.
        adj: dict[str, set[str]] = {}
        for e in edges:
            adj.setdefault(e["src"], set()).add(e["dst"])
            adj.setdefault(e["dst"], set()).add(e["src"])
        hop_of: dict[str, int] = dict.fromkeys(seeds, 0)
        frontier, dist = list(seeds), 0
        while frontier:
            dist += 1
            nxt: list[str] = []
            for u in frontier:
                for v in adj.get(u, ()):
                    if v not in hop_of:
                        hop_of[v] = dist
                        nxt.append(v)
            frontier = nxt

        nodes = [
            {
                "natural_key": k,
                "type": meta.get(k, {}).get("type", "entity"),
                "title": meta.get(k, {}).get("title", ""),
                "role": "seed" if k in seed_set else "neighbor",
                "hop": hop_of.get(k, 1),
            }
            for k in node_keys
        ]
        return {"nodes": nodes, "edges": edges}

    def list_nodes(
        self,
        *,
        type: str | None = None,
        status: str | None = None,
        limit: int = 50,
        namespaces: Sequence[str] = (),
    ) -> list[dict[str, Any]]:
        """Список узлов области видимости с фильтром по типу и (опционально) статусу."""
        query = "SELECT " + node_json("n") + " FROM {N} n WHERE n.namespace = ANY(%s::text[])"
        params: list[Any] = [self._nss(namespaces)]
        if type:
            query += " AND n.type = %s"
            params.append(type)
        out: list[dict[str, Any]] = []
        for (parsed,) in self._all(query + " ORDER BY n.id LIMIT 500", params):
            if not isinstance(parsed, dict):
                continue
            if status is not None:
                inner = parsed.get("props") or {}
                node_status = inner.get("status") if isinstance(inner, dict) else None
                if node_status != status:
                    continue
            out.append(parsed)
            if len(out) >= limit:
                break
        return out

    # --- экспорт для проекции в NetworkX ---

    def all_nodes(
        self, *, limit: int = 100_000, namespaces: Sequence[str] | None = None
    ) -> list[dict[str, Any]]:
        """Узлы графа (свойства) — для проекции в NetworkX (кластеризация/анализ).

        ``namespaces=None`` — весь граф (offline-анализ); список — только эти namespaces.
        """
        query = "SELECT " + node_json("n") + " FROM {N} n"
        params: list[Any] = []
        if namespaces is not None:
            query += " WHERE n.namespace = ANY(%s::text[])"
            params.append(self._nss(namespaces))
        rows = self._all(query + f" ORDER BY n.id LIMIT {int(limit)}", params)
        return [r[0] for r in rows if isinstance(r[0], dict)]

    def all_edges(
        self, *, limit: int = 500_000, namespaces: Sequence[str] | None = None
    ) -> list[dict[str, Any]]:
        """Рёбра графа как {src, dst, type, source_path} — для проекции в NetworkX.

        ``namespaces=None`` — весь граф; список — рёбра, оба конца которых в области.
        """
        query = (
            "SELECT a.natural_key, b.natural_key, e.type, e.props ->> 'source_path' FROM {E} e "
            "JOIN {N} a ON a.id = e.src_id JOIN {N} b ON b.id = e.dst_id"
        )
        params: dict[str, Any] = {}
        if namespaces is not None:
            query += (
                " WHERE a.namespace = ANY(%(nss)s::text[]) AND b.namespace = ANY(%(nss)s::text[])"
            )
            params["nss"] = self._nss(namespaces)
        rows = self._all(query + f" ORDER BY e.id LIMIT {int(limit)}", params)
        return [{"src": s, "dst": d, "type": t, "source_path": sp} for s, d, t, sp in rows]

    # --- разметка сообществ (Phase 3) ---

    def set_node_communities(
        self,
        assignments: dict[str, int],
        cohesions: dict[int, float] | None = None,
        names: dict[int, str] | None = None,
        namespace: str = "",
    ) -> int:
        """Проставить узлам ``community``/``cohesion``/``community_name`` одним запросом.

        ``assignments`` — natural_key → community_id. ``cohesions`` (community_id → скор)
        и ``names`` (community_id → метка) опциональны. Разметка скоуплена одним
        namespace (пусто -> default стора) — офлайн-анализ сообществ не трогает чужие
        базы знаний даже при коллизии natural_key (ADR-017). Возвращает число
        обновлённых узлов. Узлы, отсутствующие в графе, молча пропускаются.
        """
        if not assignments:
            return 0
        cohesions = cohesions or {}
        names = names or {}
        keys: list[str] = []
        rows: list[dict[str, Any]] = []
        for nk, cid in assignments.items():
            patch, keys = set_props(
                {"community": cid, "cohesion": cohesions.get(cid), "community_name": names.get(cid)}
            )
            rows.append({"nk": nk, "patch": patch.obj})
        return self._run(
            "UPDATE {N} n SET props = (n.props - %(keys)s::text[]) || r.patch "
            "FROM jsonb_to_recordset(%(rows)s) AS r(nk text, patch jsonb) "
            "WHERE n.namespace = %(ns)s AND n.natural_key = r.nk",
            {"keys": keys, "rows": Jsonb(rows), "ns": self._ns(namespace)},
        )

    def community_members(
        self, community_id: int, *, limit: int = 500, namespaces: Sequence[str] = ()
    ) -> list[dict[str, Any]]:
        """Узлы заданного сообщества в области видимости (пусто -> default namespace)."""
        rows = self._all(
            "SELECT " + node_json("n") + " FROM {N} n "
            "WHERE n.namespace = ANY(%s::text[]) AND n.props @> %s "
            f"ORDER BY n.id LIMIT {int(limit)}",
            (self._nss(namespaces), Jsonb({"community": community_id})),
        )
        return [r[0] for r in rows if isinstance(r[0], dict)]

    # --- слияние узлов (применение одобренного арбитром merge) ---

    def merge_nodes(
        self, survivor_key: str, duplicate_key: str, namespace: str = ""
    ) -> dict[str, Any]:
        """Слить ``duplicate_key`` в ``survivor_key``: рёбра перевешиваются, дубликат удаляется.

        Применяется при одобрении кандидата на слияние арбитром, в пределах одного
        namespace. Рёбра дубликата пересоздаются на выжившем (upsert связи — дубли и
        петли отсекаются), затем дубликат удаляется вместе со своими рёбрами (``on
        delete cascade``). Всё — одной транзакцией. Идемпотентно: повторный вызов на уже
        удалённом дубликате просто ничего не находит.
        """
        ns = self._ns(namespace)
        if survivor_key == duplicate_key:
            return {"ok": False, "reason": "same_node", "rewired": 0}
        if self.get_node(duplicate_key, namespaces=[ns]) is None:
            return {"ok": False, "reason": "duplicate_missing", "rewired": 0}
        if self.get_node(survivor_key, namespaces=[ns]) is None:
            return {"ok": False, "reason": "survivor_missing", "rewired": 0}

        rewired = 0
        with self.conn.transaction():
            # (исходящее?, тип, ключ другого конца, source_path, origin) рёбер дубликата.
            edges = self._all(
                "SELECT e.src_id = d.id, e.type, o.natural_key, e.props ->> 'source_path', "
                "e.props ->> 'origin' FROM {N} d "
                "JOIN {E} e ON e.src_id = d.id OR e.dst_id = d.id "
                "JOIN {N} o ON o.id = CASE WHEN e.src_id = d.id THEN e.dst_id ELSE e.src_id END "
                "WHERE d.namespace = %s AND d.natural_key = %s ORDER BY e.id",
                (ns, duplicate_key),
            )
            for outgoing, etype, other, sp, og in edges:
                if not other or other in (survivor_key, duplicate_key):
                    continue
                src, dst = (survivor_key, str(other)) if outgoing else (str(other), survivor_key)
                edge = Edge(
                    type=str(etype),
                    src=src,
                    dst=dst,
                    source_path=sp,
                    origin=og or "vault",
                    namespace=ns,
                )
                if self.upsert_edge(edge):
                    rewired += 1
            self._run(
                "DELETE FROM {N} WHERE namespace = %s AND natural_key = %s", (ns, duplicate_key)
            )
        return {
            "ok": True,
            "survivor": survivor_key,
            "duplicate": duplicate_key,
            "rewired": rewired,
        }

    # --- статистика ---

    def count_nodes_by_type(self, namespaces: Sequence[str] | None = None) -> dict[str, int]:
        """Вернуть число узлов в разрезе их типа (None — весь граф)."""
        query = "SELECT coalesce(type, 'entity'), count(*) FROM {N}"
        params: list[Any] = []
        if namespaces is not None:
            query += " WHERE namespace = ANY(%s::text[])"
            params.append(self._nss(namespaces))
        return {str(t): int(c) for t, c in self._all(query + " GROUP BY 1", params)}

    def count_edges_by_type(self, namespaces: Sequence[str] | None = None) -> dict[str, int]:
        """Вернуть число рёбер в разрезе их типа (None — весь граф)."""
        query = "SELECT type, count(*) FROM {E}"
        params: list[Any] = []
        if namespaces is not None:
            query += " WHERE namespace = ANY(%s::text[])"
            params.append(self._nss(namespaces))
        return {str(t): int(c) for t, c in self._all(query + " GROUP BY 1", params)}

    def total_nodes(self) -> int:
        """Вернуть суммарное число узлов в графе."""
        return sum(self.count_nodes_by_type().values())

    def total_edges(self) -> int:
        """Вернуть суммарное число рёбер в графе."""
        return sum(self.count_edges_by_type().values())

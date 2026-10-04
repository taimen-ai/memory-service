"""FactStore — temporal-факты с supersession поверх графа памяти (ADR-016 §4).

Факт — ребро ``graph_edges`` с ``fact_id``, уникальным в namespace (MEM-ADR-023): несколько рёбер одного типа между одной
парой узлов сосуществуют, различаясь validity-интервалами. Историю не удаляем:
supersession закрывает ``valid_to`` старого факта и связывает его с новым через
``superseded_by``. Времена — строки ISO-8601 UTC (лексикографически сравнимы).

Классы свидетельства (``evidence``): asserted > extracted > inferred > derived —
ranking/audit-сигнал, не математическая истина (ADR-016 §11).
"""

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Sequence
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from platform_memory.core import db as dbmod
from platform_memory.core.models import Node, Provenance
from platform_memory.core.namespaces import resolve_namespace, resolve_namespaces
from platform_memory.core.observations import EVIDENCE_ASSERTED, EVIDENCE_CLASSES
from platform_memory.core.ontology import sanitize_label
from platform_memory.core.scopes import (
    ForeignObjectError,
    check_write_scopes,
    is_visible,
    merge_scopes,
)
from platform_memory.graph.store import GraphStore, edge_json, set_props

# Ранг силы свидетельства для ranking/конфликтов (больше — сильнее).
EVIDENCE_RANK = {"asserted": 3, "extracted": 2, "inferred": 1, "derived": 1}


def _now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def normalize_ts(value: str | None) -> str | None:
    """Нормализовать метку времени факта в ISO-8601 UTC с точностью до секунды.

    Лексикографическое сравнение временных строк корректно ТОЛЬКО в одном
    формате: смещение ``+03:00`` или микросекунды ломают порядок (``.5Z`` < ``Z``).
    Невалидная строка -> ValueError (тихая порча temporal-фильтров хуже отказа).
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return ""
    parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def fact_id_for(namespace: str, subject: str, predicate: str, obj: str, valid_from: str) -> str:
    """Детерминированный id факта: повторная доставка того же утверждения не дублирует."""
    raw = f"{namespace}|{subject}|{predicate}|{obj}|{valid_from}"
    return f"fact-{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:24]}"


class FactStore:
    """Temporal-факты: assert/invalidate/supersede и выборки с ``as_of``."""

    def __init__(self, graph: GraphStore):
        """Работает поверх GraphStore (то же соединение, граф и default-namespace)."""
        self.store = graph
        self.conn = graph.conn
        self.graph = graph.graph

    def _ns(self, namespace: str) -> str:
        return resolve_namespace(namespace, self.store.default_namespace)

    # --- узлы-концы ---

    def ensure_entity(
        self,
        natural_key: str,
        *,
        entity_type: str = "entity",
        title: str = "",
        properties: dict[str, Any] | None = None,
        namespace: str = "",
        source_path: str = "",
        scopes: Sequence[str] = (),
        check_kind: bool = True,
        allowed_scopes: Sequence[str] | None = None,
    ) -> str:
        """Идемпотентно создать узел-сущность (origin='agent'); вернуть natural_key.

        Проекция assertions не должна зависеть от порядка: конец факта создаётся
        placeholder'ом, если entity-assertion ещё не приезжал. Вид проверяется
        kind-guard'ом стора (строгий режим namespace, MEM-ADR-020); ``check_kind=False``
        — для контентных узлов (текст наблюдения), которые не являются сущностями.
        Существующий узел вне видимости ``allowed_scopes`` не перезаписывается
        (:class:`ForeignObjectError`), scopes видимого сливаются (MEM-ADR-019).
        """
        ns = self._ns(namespace)
        if check_kind and self.store.kind_guard is not None:
            entity_type = self.store.kind_guard(
                ns, entity_type or "entity", natural_key, properties
            )
        props = dict(properties or {})
        if scopes:
            props["scopes"] = list(scopes)
        self.store.upsert_node(
            Node(
                type=entity_type or "entity",
                natural_key=natural_key,
                title=title or natural_key,
                properties=props,
                origin="agent",
                namespace=ns,
                provenance=Provenance(
                    source_path=source_path or "memory:observation",
                    confidence=1.0,
                    last_seen=_now_iso(),
                ),
            ),
            guard_scopes=True,
            allowed_scopes=allowed_scopes,
        )
        return natural_key

    # --- запись фактов ---

    # Ключи ``props`` факта, которые пишет assert_fact (``scopes`` — только когда заданы).
    _ASSERT_KEYS = ("observed_at", "evidence", "confidence", "supersedes", "source_path", "origin")

    def assert_fact(
        self,
        subject: str,
        predicate: str,
        obj: str,
        *,
        namespace: str = "",
        observed_at: str = "",
        valid_from: str = "",
        valid_to: str | None = None,
        evidence: str = EVIDENCE_ASSERTED,
        confidence: float = 1.0,
        observation_id: str = "",
        supersedes: str = "",
        scopes: Sequence[str] = (),
        source_path: str = "",
        allowed_scopes: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Записать temporal-факт; вернуть {fact_id, reinforced, superseded}.

        Идемпотентно по (namespace, subject, predicate, obj, valid_from): повтор
        того же утверждения усиливает факт (добавляет observation_id, поднимает
        confidence до максимума), а не дублирует ребро. ``supersedes`` в той же
        транзакции закрывает валидность старого факта. Историю не удаляет.

        ``allowed_scopes`` — видимость пишущего (None — без ограничения, MEM-ADR-019):
        конец факта или существующий факт вне неё — :class:`ForeignObjectError`, scopes
        видимости записи — только из неё; scopes повторённого факта сливаются
        (:func:`merge_scopes`), а не заменяются. Невидимый ``supersedes`` не
        закрывается — как отсутствующий.

        Новый факт вставляется ``INSERT … ON CONFLICT DO NOTHING``; повтор читается под
        блокировкой строки (``FOR UPDATE``) и сливается — параллельные подтверждения
        одного факта не теряют ``observation_ids`` друг друга.
        """
        if evidence not in EVIDENCE_CLASSES:
            raise ValueError(f"Неизвестный класс свидетельства {evidence!r}")
        ns = self._ns(namespace)
        # Temporal-поля нормализуются к единому формату ДО вычисления fact_id:
        # '13:00+03:00' и '10:00Z' — одно утверждение, id обязан совпасть.
        observed_at = normalize_ts(observed_at) or ""
        valid_from = normalize_ts(valid_from) or ""
        valid_to = normalize_ts(valid_to)
        etype = sanitize_label(predicate).upper()
        dbmod.check_graph_key(label=etype)
        fid = fact_id_for(ns, subject, predicate, obj, valid_from)
        confidence = max(0.0, min(1.0, float(confidence)))

        check_write_scopes(scopes, allowed_scopes)
        observed = observed_at or _now_iso()
        values: dict[str, Any] = {
            "observed_at": observed,
            "evidence": evidence,
            "confidence": confidence,
            "supersedes": supersedes or None,
            "source_path": source_path or "memory:observation",
            "origin": "agent",
        }
        obs_ids = [observation_id] if observation_id else []
        superseded = False
        existing: dict[str, Any] | None = None
        with self.conn.transaction():
            for key in (subject, obj):
                if not self.store.key_visible(key, ns, allowed_scopes):
                    raise ForeignObjectError(key)
            patch, _ = set_props({**values, "scopes": list(scopes) or None})
            created = self.store._all(
                "INSERT INTO {E} (namespace, type, src_id, dst_id, fact_id, observation_ids, "
                "valid_from, valid_to, props) "
                "SELECT %(ns)s, %(type)s, a.id, b.id, %(fid)s, %(obs)s::text[], %(vf)s, %(vt)s, "
                "%(patch)s FROM {N} a "
                "JOIN {N} b ON b.namespace = a.namespace AND b.natural_key = %(dst)s "
                "WHERE a.namespace = %(ns)s AND a.natural_key = %(src)s "
                "ORDER BY a.id, b.id LIMIT 1 "
                "ON CONFLICT (namespace, fact_id) WHERE fact_id IS NOT NULL DO NOTHING "
                "RETURNING id",
                {
                    "ns": ns,
                    "type": etype,
                    "fid": fid,
                    "obs": obs_ids,
                    "vf": valid_from,
                    "vt": valid_to,
                    "patch": patch,
                    "src": subject,
                    "dst": obj,
                },
            )
            if not created:
                found = self._select_facts(
                    "e.namespace = %(ns)s AND e.fact_id = %(fid)s",
                    {"ns": ns, "fid": fid},
                    lock=True,
                )
                if not found:
                    raise ValueError(
                        f"Концы факта не найдены в namespace {ns!r}: {subject!r} -> {obj!r}"
                    )
                existing = found[0]
                if not is_visible(_fact_scopes(existing), allowed_scopes):
                    raise ForeignObjectError(fid)
                # Reinforcement: тот же факт из нового наблюдения (ADR-016 §26).
                merged_obs = list(existing.get("observation_ids") or [])
                if observation_id and observation_id not in merged_obs:
                    merged_obs.append(observation_id)
                values["confidence"] = max(float(existing.get("confidence") or 0.0), confidence)
                merged_scopes = merge_scopes(_fact_scopes(existing), scopes)
                if merged_scopes:
                    values["scopes"] = list(merged_scopes)
                patch, keys = set_props(values)
                self.store._run(
                    "UPDATE {E} e SET valid_from = %(vf)s, valid_to = %(vt)s, "
                    "observation_ids = %(obs)s::text[], "
                    "props = (e.props - %(keys)s::text[]) || %(patch)s "
                    "WHERE e.namespace = %(ns)s AND e.fact_id = %(fid)s",
                    {
                        "vf": valid_from,
                        "vt": valid_to,
                        "obs": merged_obs,
                        "keys": keys,
                        "patch": patch,
                        "ns": ns,
                        "fid": fid,
                    },
                )
            if (
                supersedes
                and supersedes != fid
                and self._fact_visible(supersedes, ns, allowed_scopes)
            ):
                superseded = self._close_fact(
                    supersedes,
                    ns,
                    valid_to=valid_from or observed,
                    superseded_by=fid,
                )
        return {"fact_id": fid, "reinforced": bool(existing), "superseded": superseded}

    def _fact_visible(self, fact_id: str, ns: str, allowed_scopes: Sequence[str] | None) -> bool:
        """Виден ли факт ``allowed_scopes`` (нет факта — True: закрывать нечего)."""
        if allowed_scopes is None:
            return True
        fact = self.get_fact(fact_id, namespaces=[ns])
        return fact is None or is_visible(_fact_scopes(fact), allowed_scopes)

    # --- батчевые операции сверки снимков (MEM-ADR-020) ---

    def create_facts(
        self,
        relation: str,
        from_kind: str,
        to_kind: str,
        rows: Sequence[dict[str, Any]],
        *,
        namespace: str,
        observed_at: str,
    ) -> set[str]:
        """Создать рёбра-факты одной связи между узлами заданных видов одним запросом.

        ``rows`` — ``{fid, s, d, vf, sup, sp, a, src, scope, sid, scopes}``: концы,
        fact_id, ``valid_from``, supersedes, source_path, атрибуты связи, снимок-источник
        и scopes видимости (MEM-ADR-019).
        fact_id у сверки уникален для версии (включает источник и снимок), поэтому
        ребро создаётся, а не сливается: закрытое ребро не переоткрывается, уже
        существующий fact_id не перезаписывается. Вернуть множество созданных fact_id
        (конец не найден — ребра нет).
        """
        if not rows:
            return set()
        etype = sanitize_label(relation).upper()
        dbmod.check_graph_key(label=etype)
        ns = self._ns(namespace)
        obs = normalize_ts(observed_at) or _now_iso()
        items = []
        for r in rows:
            patch, _ = set_props(
                {
                    "observed_at": obs,
                    "evidence": "asserted",
                    "confidence": 1.0,
                    "supersedes": r.get("sup"),
                    "source_path": r.get("sp"),
                    "origin": "agent",
                    "attributes": r.get("a"),
                    "snapshot_source": r.get("src"),
                    "snapshot_scope": r.get("scope"),
                    "snapshot_id": r.get("sid"),
                    "scopes": r.get("scopes"),
                }
            )
            items.append(
                {"fid": r["fid"], "s": r["s"], "d": r["d"], "vf": r.get("vf"), "patch": patch.obj}
            )
        got = self.store._all(
            "INSERT INTO {E} (namespace, type, src_id, dst_id, fact_id, observation_ids, "
            "valid_from, props) "
            "SELECT %(ns)s, %(type)s, a.id, b.id, r.fid, ARRAY[]::text[], r.vf, r.patch "
            "FROM jsonb_to_recordset(%(rows)s) AS r(fid text, s text, d text, vf text, patch jsonb) "
            "JOIN {N} a ON a.namespace = %(ns)s AND a.label = %(la)s AND a.natural_key = r.s "
            "JOIN {N} b ON b.namespace = %(ns)s AND b.label = %(lb)s AND b.natural_key = r.d "
            "ON CONFLICT (namespace, fact_id) WHERE fact_id IS NOT NULL DO NOTHING "
            "RETURNING fact_id",
            {
                "ns": ns,
                "type": etype,
                "la": sanitize_label(from_kind),
                "lb": sanitize_label(to_kind),
                "rows": Jsonb(items),
            },
        )
        return {str(r[0]) for r in got}

    def close_facts(self, relation: str, rows: Sequence[dict[str, Any]], *, namespace: str) -> int:
        """Закрыть рёбра-факты одной связи одним запросом: ``rows`` — ``{fid, vt, nxt}``."""
        if not rows:
            return 0
        items = [{"fid": r.get("fid"), "vt": r.get("vt"), "nxt": r.get("nxt")} for r in rows]
        return self.store._run(
            "UPDATE {E} e SET valid_to = r.vt, props = (e.props - 'superseded_by'::text) "
            "|| jsonb_strip_nulls(jsonb_build_object('superseded_by', r.nxt)) "
            "FROM jsonb_to_recordset(%(rows)s) AS r(fid text, vt text, nxt text) "
            "WHERE e.namespace = %(ns)s AND e.type = %(type)s AND e.fact_id = r.fid",
            {
                "rows": Jsonb(items),
                "ns": self._ns(namespace),
                "type": sanitize_label(relation).upper(),
            },
        )

    def _close_fact(
        self, fact_id: str, ns: str, *, valid_to: str, superseded_by: str | None = None
    ) -> bool:
        """Закрыть validity-интервал факта (не удаляя его). True — если факт найден."""
        rows = self.store._all(
            "UPDATE {E} e SET valid_to = %(vt)s, props = (e.props - 'superseded_by'::text) "
            "|| jsonb_strip_nulls(jsonb_build_object('superseded_by', %(by)s::text)) "
            "WHERE e.namespace = %(ns)s AND e.fact_id = %(fid)s RETURNING e.id",
            {
                "vt": normalize_ts(valid_to) or _now_iso(),
                "by": superseded_by,
                "ns": ns,
                "fid": fact_id,
            },
        )
        return bool(rows)

    def invalidate_fact(
        self,
        fact_id: str,
        *,
        namespace: str = "",
        valid_to: str = "",
        observation_id: str = "",
    ) -> bool:
        """Закрыть факт без замены («Alice больше не работает над X»). История остаётся."""
        ns = self._ns(namespace)
        closed = self._close_fact(fact_id, ns, valid_to=valid_to or _now_iso())
        if closed and observation_id:
            self.store._run(
                "UPDATE {E} SET observation_ids = coalesce(observation_ids, ARRAY[]::text[]) "
                "|| %(oid)s::text WHERE namespace = %(ns)s AND fact_id = %(fid)s "
                "AND NOT (coalesce(observation_ids, ARRAY[]::text[]) @> ARRAY[%(oid)s::text])",
                {"oid": observation_id, "ns": ns, "fid": fact_id},
            )
        return closed

    def scrub_derived_nodes(
        self, observation_id: str, namespace: str = "", *, purge: bool = False
    ) -> int:
        """Зачистить узлы, порождённые наблюдением (props.observation_id == oid).

        Содержимое наблюдения не должно переживать его redact в производном
        узле (ADR-016 §8): props.content затирается, узел помечается
        ``redacted``; при ``purge=True`` узел удаляется целиком вместе с рёбрами
        (``on delete cascade``). Выборка — по колонке ``observation_id`` узла.
        Вернуть число затронутых узлов.
        """
        params = {"oid": observation_id, "ns": self._ns(namespace)}
        where = " WHERE namespace = %(ns)s AND observation_id = %(oid)s"
        if purge:
            return self.store._run("DELETE FROM {N}" + where, params)
        return self.store._run(
            "UPDATE {N} SET props = props || jsonb_build_object('props', "
            "jsonb_build_object('observation_id', %(oid)s::text, 'redacted', true))" + where,
            params,
        )

    def mark_evidence_lost(self, observation_id: str, namespace: str = "") -> int:
        """Отразить удаление наблюдения на derived-фактах (ADR-016 §8).

        Из ``observation_ids`` каждого факта вычёркивается id; факт, оставшийся
        без свидетельств, получает ``evidence_lost=true`` (retrieval по умолчанию
        его не отдаёт; из графа не удаляется — аудит). Одним запросом. Вернуть число
        фактов, потерявших последнее свидетельство.
        """
        rows = self.store._all(
            "UPDATE {E} SET observation_ids = array_remove(observation_ids, %(oid)s::text), "
            "evidence_lost = evidence_lost "
            "OR cardinality(array_remove(observation_ids, %(oid)s::text)) = 0 "
            "WHERE namespace = %(ns)s AND fact_id IS NOT NULL "
            "AND observation_ids @> ARRAY[%(oid)s::text] "
            "RETURNING cardinality(observation_ids) = 0",
            {"oid": observation_id, "ns": self._ns(namespace)},
        )
        return sum(1 for (lost,) in rows if lost)

    # --- чтение ---

    def _select_facts(
        self,
        where: str,
        params: dict[str, Any],
        *,
        limit: int | None = None,
        lock: bool = False,
    ) -> list[dict[str, Any]]:
        """Факты (рёбра с концами) по условию на ``e``/``a``/``b``, по возрастанию id."""
        query = (
            "SELECT a.natural_key, b.natural_key, e.type, "
            + edge_json("e")
            + ", a.props ->> 'title', b.props ->> 'title' FROM {E} e "
            "JOIN {N} a ON a.id = e.src_id JOIN {N} b ON b.id = e.dst_id "
            "WHERE " + where + " ORDER BY e.id"
        )
        if limit is not None:
            query += f" LIMIT {int(limit)}"
        if lock:
            query += " FOR UPDATE OF e"
        return self._rows_to_facts(self.store._all(query, params))

    @staticmethod
    def _rows_to_facts(rows: list[tuple]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for subject, obj, predicate, props, subject_title, object_title in rows:
            fact = {
                # Пустые свойства ребра не хранятся — нормализуем опциональные поля,
                # чтобы потребитель видел None.
                "valid_to": None,
                "supersedes": None,
                "superseded_by": None,
                "subject": subject,
                "object": obj,
                "predicate": predicate,
                "subject_title": subject_title,
                "object_title": object_title,
                **(props if isinstance(props, dict) else {}),
            }
            out.append(fact)
        return out

    def get_fact(self, fact_id: str, namespaces: Sequence[str] = ()) -> dict[str, Any] | None:
        """Вернуть факт по fact_id в области видимости, либо None."""
        nss = resolve_namespaces(namespaces, self.store.default_namespace)
        facts = self._select_facts(
            "e.namespace = ANY(%(nss)s::text[]) AND e.fact_id = %(fid)s",
            {"nss": nss, "fid": fact_id},
            limit=1,
        )
        return facts[0] if facts else None

    def facts(
        self,
        *,
        subject: str = "",
        predicate: str = "",
        obj: str = "",
        namespaces: Sequence[str] = (),
        as_of: str = "",
        include_closed: bool = False,
        include_evidence_lost: bool = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Выборка фактов вокруг узла с temporal-фильтром.

        ``as_of`` — история на момент времени (valid_from <= t < valid_to);
        пусто — действующие сейчас факты. ``include_closed=True`` отдаёт и
        закрытые интервалы (полная история). Факты с ``evidence_lost`` по
        умолчанию скрыты (ADR-016 §8).
        """
        nss = resolve_namespaces(namespaces, self.store.default_namespace)
        conds = ["e.fact_id IS NOT NULL", "e.namespace = ANY(%(nss)s::text[])"]
        params: dict[str, Any] = {"nss": nss}
        if predicate:
            conds.append("e.type = %(type)s")
            params["type"] = sanitize_label(predicate).upper()
        # Конец ищется в тех же namespaces, что и факт (концы факта — в его namespace):
        # так узел находится по индексу ``(namespace, natural_key)``, а не по ключу
        # во всех базах.
        if subject:
            conds.append("a.namespace = ANY(%(nss)s::text[]) AND a.natural_key = %(subj)s")
            params["subj"] = subject
        if obj:
            conds.append("b.namespace = ANY(%(nss)s::text[]) AND b.natural_key = %(obj)s")
            params["obj"] = obj
        if not include_evidence_lost:
            conds.append("NOT e.evidence_lost")
        if as_of:
            params["t"] = normalize_ts(as_of)
            conds.append("(e.valid_from IS NULL OR e.valid_from = '' OR e.valid_from <= %(t)s)")
            conds.append("(e.valid_to IS NULL OR e.valid_to > %(t)s)")
        elif not include_closed:
            conds.append("e.valid_to IS NULL")
        return self._select_facts(" AND ".join(conds), params, limit=limit)

    def facts_for_observation(
        self, observation_id: str, namespaces: Sequence[str] = ()
    ) -> list[dict[str, Any]]:
        """Все факты, среди свидетельств которых есть данное наблюдение."""
        nss = resolve_namespaces(namespaces, self.store.default_namespace)
        return self._select_facts(
            "e.fact_id IS NOT NULL AND e.namespace = ANY(%(nss)s::text[]) "
            "AND e.observation_ids @> ARRAY[%(oid)s::text]",
            {"nss": nss, "oid": observation_id},
        )

    @staticmethod
    def find_conflicts(facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Пометить конфликты в выборке фактов (чистая функция, БД не нужна).

        Конфликт: два факта одного (subject, predicate) с разными объектами и
        пересекающимися validity-интервалами, ни один не superseded другим.
        Помеченные факты получают ``conflict: true`` и ``conflicts_with`` —
        retrieval знает о конфликте, разрешение остаётся потребителю.
        """
        groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for fact in facts:
            key = (str(fact.get("subject")), str(fact.get("predicate")))
            groups.setdefault(key, []).append(fact)

        def _overlap(x: dict[str, Any], y: dict[str, Any]) -> bool:
            xf, xt = str(x.get("valid_from") or ""), x.get("valid_to")
            yf, yt = str(y.get("valid_from") or ""), y.get("valid_to")
            xt_s = str(xt) if xt else "￿"  # открытый интервал > любой даты
            yt_s = str(yt) if yt else "￿"
            return xf < yt_s and yf < xt_s

        for group in groups.values():
            if len(group) < 2:
                continue
            for i, x in enumerate(group):
                for y in group[i + 1 :]:
                    if str(x.get("object")) == str(y.get("object")):
                        continue
                    if x.get("superseded_by") == y.get("fact_id"):
                        continue
                    if y.get("superseded_by") == x.get("fact_id"):
                        continue
                    if _overlap(x, y):
                        for fact, other in ((x, y), (y, x)):
                            fact["conflict"] = True
                            lst = fact.setdefault("conflicts_with", [])
                            fid = other.get("fact_id")
                            if fid and fid not in lst:
                                lst.append(fid)
        return facts


def _fact_scopes(fact: dict[str, Any]) -> list[str]:
    """Scopes факта (свойство ребра ``scopes``; нет или не список — пусто)."""
    raw = fact.get("scopes")
    return [str(s) for s in raw] if isinstance(raw, list | tuple) else []


def _connect_stores(settings) -> tuple[psycopg.Connection, GraphStore, FactStore]:
    """Служебная фабрика для CLI/скриптов: соединение + графовые сторы."""
    conn = dbmod.connect(settings, autocommit=True)
    graph = GraphStore(conn, settings.graph_name, settings.default_namespace)
    return conn, graph, FactStore(graph)

"""GraphBenchOps — seam для сравнения графовых движков (ADR-016 §10, MEM-ADR-023 п.6).

Протокол фиксирует операции, по которым движки графа сравниваются БЕЗ изменения
Context API: реализуйте протокол для кандидата и прогоните тот же ``bench.py``. Это НЕ
runtime-абстракция хранилища (ADR-015 закрыл premature abstraction) — только интерфейс
бенчмарка.

Реализации:

- :class:`TableBenchOps` — текущий движок: таблицы ``graph_nodes``/``graph_edges``
  (MEM-ADR-023), методы ``GraphStore``/``FactStore`` как есть.
- :class:`AgeBenchOps` — прежний движок Apache AGE: те же операции запросами Cypher
  кода до TASK-001284 (``git show a4dc7fe:src/platform_memory/graph/store.py`` и
  ``facts.py``), перенесены сюда дословно — сервис AGE больше не читает. Нужна база с
  расширением AGE и граф AGE с тем же именем (до удаления AGE, MEM-ADR-023 п.5).

``hybrid_context`` — гибридный запрос так, как его выполняет Context Compiler
(``build_context``, стратегия ``hybrid``): каналы lexical (чанки и наблюдения), vector,
graph (обход на ``CB_CONTEXT_MAX_DEPTH`` хопов от якорей и топ-хитов вектора), facts
(temporal-факты якорей), recent, фильтр видимости ``allowed_scopes``. Граф компилятор
берёт из ``GraphStore``/``FactStore``; у AGE на время вызова подставляются их
AGE-варианты (:func:`age_compiler`) — остальные каналы общие и идут по тем же таблицам.
"""

from __future__ import annotations

import contextlib
import json
import re
from collections.abc import Iterator, Sequence
from typing import Any, Protocol

import psycopg
from psycopg.adapt import Dumper

from platform_memory.core.config import Settings
from platform_memory.core.namespaces import resolve_namespaces
from platform_memory.core.ontology import sanitize_label
from platform_memory.graph.facts import FactStore, normalize_ts
from platform_memory.graph.store import GraphStore


class GraphBenchOps(Protocol):
    """Операции, по которым сравниваются графовые движки."""

    def entity_lookup(self, natural_key: str) -> dict[str, Any] | None: ...

    def one_hop(self, natural_key: str) -> list[dict[str, Any]]: ...

    def two_hop(self, natural_key: str) -> list[dict[str, Any]]: ...

    def neighborhood(self, keys: list[str], limit: int) -> list[dict[str, Any]]: ...

    def temporal_filter(self, subject: str, as_of: str) -> list[dict[str, Any]]: ...

    def shortest_path(self, src: str, dst: str) -> list[str]: ...

    def hybrid_context(self, request: dict[str, Any], embedder: Any) -> Any: ...


class TableBenchOps:
    """GraphBenchOps поверх текущего GraphStore/FactStore (таблицы, MEM-ADR-023)."""

    engine = "tables"

    def __init__(self, settings: Settings, conn: psycopg.Connection):
        self.settings = settings
        self.conn = conn
        self.graph = GraphStore(conn, settings.graph_name, settings.default_namespace)
        self.facts = FactStore(self.graph)

    def entity_lookup(self, natural_key: str) -> dict[str, Any] | None:
        return self.graph.get_node(natural_key)

    def one_hop(self, natural_key: str, limit: int = 40) -> list[dict[str, Any]]:
        return self.graph.expand_neighbors([natural_key], hops=1, limit=limit)

    def two_hop(self, natural_key: str, limit: int = 40) -> list[dict[str, Any]]:
        return self.graph.expand_neighbors([natural_key], hops=2, limit=limit)

    def neighborhood(self, keys: list[str], limit: int) -> list[dict[str, Any]]:
        return self.graph.expand_neighbors(keys, hops=1, limit=limit)

    def temporal_filter(self, subject: str, as_of: str) -> list[dict[str, Any]]:
        return self.facts.facts(subject=subject, as_of=as_of, include_closed=True)

    def shortest_path(self, src: str, dst: str) -> list[str]:
        import networkx as nx

        from platform_memory.graph import to_networkx

        G = to_networkx(self.graph).to_undirected(as_view=True)
        try:
            return nx.shortest_path(G, src, dst)
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return []

    def hybrid_context(self, request: dict[str, Any], embedder: Any) -> Any:
        from platform_memory import build_context

        return build_context(self.settings, request, embedder=embedder, conn=self.conn)


# --- AGE: соединение и запросы кода до TASK-001284 ---


class _Agtype:
    """Параметр ``cypher()``: третий аргумент обязан быть bind-параметром типа agtype."""

    __slots__ = ("text",)

    def __init__(self, value: Any):
        self.text = json.dumps(value, ensure_ascii=False)


def age_connect(dsn: str, *, jit: bool) -> psycopg.Connection:
    """Соединение с загруженным AGE (``LOAD 'age'`` — только суперпользователю) и dumper agtype."""
    conn = psycopg.connect(dsn, autocommit=True)
    with conn.cursor() as cur:
        cur.execute("SELECT oid FROM pg_type WHERE typname = 'agtype'")
        row = cur.fetchone()
        if row is None:
            conn.close()
            raise RuntimeError("в базе нет расширения AGE — сторону AGE мерить нечем")
        cur.execute("LOAD 'age'")
        cur.execute('SET search_path = ag_catalog, "$user", public')
        cur.execute("SET jit = " + ("on" if jit else "off"))

    class _AgtypeDumper(Dumper):
        oid = int(row[0])

        def dump(self, obj: _Agtype) -> bytes:
            return obj.text.encode("utf-8")

    conn.adapters.register_dumper(_Agtype, _AgtypeDumper)
    return conn


# Хвост-аннотация типа в выводе agtype: 12345::vertex / {...}::edge и т.п.
_AGTYPE_SUFFIX = re.compile(r"::(vertex|edge|path|numeric|integer|float)\s*$")


def parse_agtype(value: Any) -> Any:
    """Значение agtype (строкой) -> Python, как ``core.db.parse_agtype`` кода a4dc7fe."""
    if value is None:
        return None
    if not isinstance(value, str):
        return value
    cleaned = _AGTYPE_SUFFIX.sub("", value.strip())
    try:
        return json.loads(cleaned)
    except (json.JSONDecodeError, ValueError):
        return value


class AgeGraphStore(GraphStore):
    """GraphStore, у которого пути чтения Context Compiler идут запросами AGE (код a4dc7fe).

    Переопределены ровно те методы, которые зовут ``build_context`` и операции бенчмарка:
    ``get_node``, ``expand_neighbors``, ``existing_keys``. Соединение — :func:`age_connect`.
    """

    # «Индексы метки есть» кэшировались на процесс (ensure_label_index): проверка
    # каталога AGE шла один раз на метку, дальше — без запроса.
    _labels: dict[tuple[str, str], bool] = {}

    def get_node(self, natural_key: str, namespaces: Sequence[str] = ()) -> dict[str, Any] | None:
        sql = (
            f"SELECT * FROM cypher('{self.graph}', $$ "
            f"MATCH (n {{natural_key: $nk}}) WHERE n.namespace IN $nss RETURN properties(n) "
            f"$$, %s) AS (props agtype)"
        )
        with self.conn.cursor() as cur:
            cur.execute(sql, (_Agtype({"nk": natural_key, "nss": self._nss(namespaces)}),))
            row = cur.fetchone()
        return parse_agtype(row[0]) if row else None

    def expand_neighbors(
        self,
        natural_keys: list[str],
        hops: int = 1,
        limit: int = 40,
        namespaces: Sequence[str] = (),
    ) -> list[dict[str, Any]]:
        if not natural_keys:
            return []
        hops = max(1, min(hops, 2))
        nss = self._nss(namespaces)
        sql = (
            f"SELECT * FROM cypher('{self.graph}', $$ "
            f"MATCH (a)-[*1..{hops}]-(b) "
            f"WHERE a.natural_key IN $keys AND a.namespace IN $nss AND b.namespace IN $nss "
            f"AND a.natural_key <> b.natural_key "
            f"RETURN DISTINCT properties(b) "
            f"$$, %s) AS (props agtype) LIMIT {int(limit)}"
        )
        out: list[dict[str, Any]] = []
        with self.conn.cursor() as cur:
            cur.execute(sql, (_Agtype({"keys": natural_keys, "nss": nss}),))
            for (props,) in cur.fetchall():
                parsed = parse_agtype(props)
                if isinstance(parsed, dict):
                    out.append(parsed)
        return out

    def _label_exists(self, label: str) -> bool:
        key = (self.graph, label)
        if key not in self._labels:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM ag_catalog.ag_label l JOIN ag_catalog.ag_graph g "
                    "ON g.graphid = l.graph WHERE g.name = %s AND l.name = %s",
                    (self.graph, label),
                )
                self._labels[key] = cur.fetchone() is not None
        return self._labels[key]

    def existing_keys(self, kind: str, natural_keys: Sequence[str], namespace: str) -> set[str]:
        keys = [k for k in dict.fromkeys(natural_keys) if k]
        if not keys:
            return set()
        label = sanitize_label(kind)
        if not self._label_exists(label):
            return set()
        sql = (
            f"SELECT * FROM cypher('{self.graph}', $$ "
            f"UNWIND $keys AS k MATCH (n:{label}) WHERE n.natural_key = k AND n.namespace = $ns "
            f"RETURN DISTINCT n.natural_key $$, %s) AS (k agtype)"
        )
        with self.conn.cursor() as cur:
            cur.execute(sql, (_Agtype({"keys": keys, "ns": self._ns(namespace)}),))
            rows = cur.fetchall()
        return {str(parse_agtype(r[0])) for r in rows}


class AgeFactStore(FactStore):
    """FactStore, у которого выборка фактов (``facts``) идёт запросом AGE (код a4dc7fe)."""

    _RETURN = "RETURN a.natural_key, b.natural_key, type(r), properties(r), a.title, b.title "

    @staticmethod
    def _age_rows(rows: list[tuple]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for row in rows:
            props = parse_agtype(row[3])
            if not isinstance(props, dict):
                props = {}
            out.append(
                {
                    "valid_to": None,
                    "supersedes": None,
                    "superseded_by": None,
                    "subject": parse_agtype(row[0]),
                    "object": parse_agtype(row[1]),
                    "predicate": parse_agtype(row[2]),
                    "subject_title": parse_agtype(row[4]),
                    "object_title": parse_agtype(row[5]),
                    **props,
                }
            )
        return out

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
        nss = resolve_namespaces(namespaces, self.store.default_namespace)
        match = (
            f"MATCH (a)-[r:{sanitize_label(predicate).upper()}]->(b)"
            if predicate
            else ("MATCH (a)-[r]->(b)")
        )
        conds = ["r.fact_id IS NOT NULL", "r.namespace IN $nss"]
        params: dict[str, Any] = {"nss": nss}
        if subject:
            conds.append("a.natural_key = $subj")
            params["subj"] = subject
        if obj:
            conds.append("b.natural_key = $obj")
            params["obj"] = obj
        if not include_evidence_lost:
            conds.append("(r.evidence_lost IS NULL OR r.evidence_lost = false)")
        if as_of:
            params["t"] = normalize_ts(as_of)
            conds.append("(r.valid_from IS NULL OR r.valid_from = '' OR r.valid_from <= $t)")
            conds.append("(r.valid_to IS NULL OR r.valid_to > $t)")
        elif not include_closed:
            conds.append("(r.valid_to IS NULL)")
        sql = (
            f"SELECT * FROM cypher('{self.graph}', $$ "
            f"{match} WHERE {' AND '.join(conds)} "
            f"{self._RETURN}$$, %s) AS (s agtype, o agtype, p agtype, r agtype, st agtype, "
            f"ot agtype) LIMIT {int(limit)}"
        )
        with self.conn.cursor() as cur:
            cur.execute(sql, (_Agtype(params),))
            rows = cur.fetchall()
        return self._age_rows(rows)


@contextlib.contextmanager
def age_compiler() -> Iterator[None]:
    """На время блока Context Compiler берёт граф и факты из AGE (Age*Store)."""
    from platform_memory.context import compiler

    saved = compiler.GraphStore, compiler.FactStore
    compiler.GraphStore, compiler.FactStore = AgeGraphStore, AgeFactStore
    try:
        yield
    finally:
        compiler.GraphStore, compiler.FactStore = saved


class AgeBenchOps(TableBenchOps):
    """GraphBenchOps поверх графа Apache AGE (запросы кода до TASK-001284)."""

    engine = "age"

    def __init__(self, settings: Settings, conn: psycopg.Connection):
        self.settings = settings
        self.conn = conn
        self.graph = AgeGraphStore(conn, settings.graph_name, settings.default_namespace)
        self.facts = AgeFactStore(self.graph)

    def shortest_path(self, src: str, dst: str) -> list[str]:
        raise NotImplementedError("путь у AGE считался в networkx по всему графу — не замеряем")

    def hybrid_context(self, request: dict[str, Any], embedder: Any) -> Any:
        with age_compiler():
            return super().hybrid_context(request, embedder)

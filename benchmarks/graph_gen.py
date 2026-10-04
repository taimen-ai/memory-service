"""Синтетический граф памяти в Apache AGE для замера MEM-ADR-023 п.6 (TASK-001286).

Копии рабочих данных у бенчмарка нет, поэтому граф генерируется: детерминированно по
``--seed``, в масштабе рабочей базы (по умолчанию 28 тыс. узлов) и в той раскладке,
в которой его писал код на AGE до TASK-001284 (``git show a4dc7fe``): метка — таблица
вида, свойства узла — ``natural_key``, ``namespace``, ``type``, ``title``,
``source_path``, ``confidence``, ``last_seen``, ``origin``, вложенная ``props``,
``valid_from``/``valid_to``; ребро-факт — ``fact_id``, ``namespace``, ``observed_at``,
``valid_from``/``valid_to``, ``evidence``, ``confidence``, ``observation_ids``,
``supersedes``/``superseded_by``, ``scopes``; структурная связь — ``source_path``,
``last_seen``, ``origin``, ``data``. У меток — индексы прежнего ``ensure_label_index``:
GIN по ``properties``, у узлов hash по ``natural_key``, у рёбер B-tree по концам.

Состав — по образцу того, что пишут сервис и его потребители:

- **ingest vault** — документы (решения, задачи, вопросы, встречи, контракты, заметки,
  люди, проекты, компании, продукты) со ссылками ``LINKS_TO``, связями frontmatter
  (``PART_OF_PROJECT``, ``OWNER``, ``ATTENDED``, ``PARTY``, ``ABOUT``) и
  ``MENTIONS`` извлечённых понятий; популярность целей — по Ципфу, поэтому есть хабы;
- **наблюдения** — текстовые узлы наблюдений (с чанками) и temporal-факты между
  сущностями (``WORKS_ON``, ``MANAGES``, ``SUPPLIES``, ``CONTRACTED``) с историей
  версий: закрытые интервалы, ``supersedes``/``superseded_by``, scopes видимости;
- **снимки software-delivery** (typed-контекст, MEM-ADR-020) — компоненты, файлы,
  эндпоинты, методы клиентов, UI-вызовы, события, таблицы, ADR и факты
  ``DEFINED_IN``/``PART_OF``/``CALLS``/``EMITS``/``GOVERNS`` со снимком-источником;
- **шум** — 10 % узлов в другом namespace с пересекающимися ключами (граф общий на
  все namespaces, фильтр namespace обязан отсекать их).

Рядом пишутся таблицы чанков (вектор ``--dim`` детерминированными псевдовекторами,
без моделей) и наблюдений — каналы lexical/vector/recent гибридного запроса.

Запуск (база с AGE и pgvector, суперпользователь — ``LOAD 'age'``)::

    uv run python benchmarks/graph_gen.py --dsn postgresql://… --graph bench_graph --seed 42

Дальше — перенос ``cb migrate-graph-from-age`` и замер ``bench.py engines``
(``benchmarks/graph-engines.md``).
"""

from __future__ import annotations

import argparse
import bisect
import datetime as dt
import hashlib
import itertools
import json
import math
import os
import random
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from benchmarks.graph_ops import _Agtype, age_connect  # noqa: E402

from platform_memory.core import db as dbmod  # noqa: E402
from platform_memory.core.config import Settings  # noqa: E402
from platform_memory.core.models import Chunk  # noqa: E402
from platform_memory.core.observations import Observation  # noqa: E402
from platform_memory.graph.facts import fact_id_for  # noqa: E402
from platform_memory.index import VectorIndex  # noqa: E402
from platform_memory.observations.store import ObservationStore  # noqa: E402

MAIN_NS = "tenant:bench:ws:main"
NOISE_NS = "tenant:bench:ws:other"

WORDS = (
    "деплой регрессия мигратор клиент возврат заказ сервер память граф контекст наблюдение "
    "факт документ поиск индекс проект встреча решение договор поставка счёт оплата "
    "согласование релиз задача ревью тест база отчёт склад закупка тендер цена срок "
    "подрядчик заказчик претензия гарантия лицензия доступ роль права аудит журнал"
).split()

# Доля узлов namespace по видам (vault, понятия, наблюдения, снимки software-delivery).
DOC_KINDS = {
    "person": 0.012,
    "project": 0.004,
    "company": 0.012,
    "contract": 0.02,
    "product": 0.006,
    "decision": 0.035,
    "task": 0.09,
    "question": 0.012,
    "meeting": 0.018,
    "note": 0.055,
}
CONCEPT_SHARE = 0.25
CUSTOMER_SHARE = 0.03
OBS_TEXT_SHARE = 0.20
SD_KINDS = {
    "source_file": 0.11,
    "endpoint": 0.024,
    "client_method": 0.046,
    "ui_call": 0.03,
    "event": 0.008,
    "table": 0.006,
    "adr": 0.009,
}
COMPONENTS = ("control-plane", "memory-service", "iam-service", "console", "fleet", "superproject")
CHUNKED_DOC_SHARE = 0.06  # доля от размера namespace: документы с чанком
CHUNKED_OBS_SHARE = 0.2  # доля текстовых узлов наблюдений с чанком
T0 = dt.datetime(2025, 1, 1, tzinfo=dt.UTC)
SPAN_DAYS = 640


def iso(t: dt.datetime) -> str:
    return t.astimezone(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


@dataclass
class Space:
    """Узлы, рёбра, чанки и наблюдения одного namespace."""

    ns: str
    nodes: dict[str, list[dict[str, Any]]] = field(default_factory=lambda: defaultdict(list))
    # (тип, метка начала, метка конца) -> [{s, d, p}]
    edges: dict[tuple[str, str, str], list[dict[str, Any]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    chunks: list[tuple[Chunk, int]] = field(default_factory=list)
    observations: list[Observation] = field(default_factory=list)


class Gen:
    """Детерминированный генератор одного namespace.

    Всё случайное — от ``random.Random`` с seed; множества перебираются отсортированными
    (порядок ``set`` строк зависит от ``PYTHONHASHSEED``).
    """

    def __init__(self, rng: random.Random, ns: str, size: int):
        self.rng = rng
        self.sp = Space(ns)
        self.size = size
        self.keys: dict[str, list[str]] = {}
        self.fact_ids: set[str] = set()

    # --- случайные величины ---

    def when(self, lo: float = 0.0, hi: float = 1.0) -> dt.datetime:
        frac = lo + (hi - lo) * self.rng.random()
        return T0 + dt.timedelta(seconds=int(frac * SPAN_DAYS * 86400))

    def text(self, n: int) -> str:
        return " ".join(self.rng.choice(WORDS) for _ in range(n))

    def count(self, share: float, minimum: int = 1) -> int:
        return max(minimum, round(self.size * share))

    def poisson(self, lam: float) -> int:
        # Кнут: малые lam, детерминированно от rng.
        limit, k, p = math.exp(-lam), 0, 1.0
        while True:
            p *= self.rng.random()
            if p <= limit:
                return k
            k += 1

    def zipf_picker(self, items: list[str], s: float = 1.1):
        """Выбор по Ципфу с перемешанным рангом: популярные цели — хабы."""
        ranked = list(items)
        self.rng.shuffle(ranked)
        weights = list(itertools.accumulate(1.0 / (i + 1) ** s for i in range(len(ranked))))
        total = weights[-1]

        def pick() -> str:
            return ranked[bisect.bisect_left(weights, self.rng.random() * total)]

        return pick

    def scopes(self, share: float) -> list[str]:
        return [f"project:p{self.rng.randrange(20)}"] if self.rng.random() < share else []

    # --- узлы и рёбра ---

    def node(self, label: str, key: str, **kw: Any) -> None:
        last_seen = iso(self.when(0.8, 1.0))
        row = {
            "natural_key": key,
            "namespace": self.sp.ns,
            "type": label,
            "title": kw.pop("title", key),
            "source_path": kw.pop("source_path", None),
            "source_id": kw.pop("source_id", None),
            "confidence": kw.pop("confidence", None),
            "last_seen": last_seen,
            "origin": kw.pop("origin", "vault"),
            "props": kw.pop("props", {}),
            "valid_from": kw.pop("valid_from", None),
            "valid_to": kw.pop("valid_to", None),
        }
        assert not kw, kw
        self.sp.nodes[label].append(row)
        self.keys.setdefault(label, []).append(key)

    def link(self, etype: str, la: str, s: str, lb: str, d: str, path: str) -> None:
        props = {
            "source_path": path,
            "last_seen": iso(self.when(0.8, 1.0)),
            "origin": "vault",
            "data": {},
        }
        self.sp.edges[(etype, la, lb)].append({"s": s, "d": d, "p": props})

    def fact(
        self,
        etype: str,
        la: str,
        s: str,
        lb: str,
        d: str,
        *,
        valid_from: str,
        valid_to: str | None = None,
        superseded_by: str | None = None,
        supersedes: str | None = None,
        extra: dict[str, Any] | None = None,
        observation_ids: list[str] | None = None,
        scopes: list[str] | None = None,
    ) -> str:
        fid = fact_id_for(self.sp.ns, s, etype.lower(), d, valid_from)
        if fid in self.fact_ids:  # то же утверждение — одно ребро (fact_id уникален)
            return fid
        self.fact_ids.add(fid)
        props: dict[str, Any] = {
            "fact_id": fid,
            "namespace": self.sp.ns,
            "observed_at": valid_from,
            "valid_from": valid_from,
            "valid_to": valid_to,
            "evidence": "asserted",
            "confidence": 1.0,
            "observation_ids": observation_ids or [],
            "supersedes": supersedes,
            "superseded_by": superseded_by,
            "source_path": "memory:observation",
            "origin": "agent",
            **(extra or {}),
        }
        if scopes:
            props["scopes"] = scopes
        self.sp.edges[(etype, la, lb)].append({"s": s, "d": d, "p": props})
        return fid

    # --- слои ---

    def vault(self) -> None:
        prefixes = {
            "decision": "BIZ-",
            "task": "T-",
            "question": "Q-",
            "meeting": "M-",
            "contract": "C-",
            "note": "N-",
        }
        for kind, share in DOC_KINDS.items():
            for i in range(self.count(share)):
                key = f"{prefixes[kind]}{i:05d}" if kind in prefixes else f"{kind}:{kind[:3]}{i}"
                props: dict[str, Any] = {
                    "status": self.rng.choice(["open", "done", "draft", "closed"]),
                    "date": iso(self.when())[:10],
                    "tags": [self.rng.choice(WORDS) for _ in range(self.rng.randrange(4))],
                }
                sc = self.scopes(0.2)
                if sc:
                    props["scopes"] = sc
                self.node(
                    kind,
                    key,
                    title=self.text(4).capitalize(),
                    source_path=f"vault/{kind}/{key}.md",
                    confidence=1.0,
                    props=props,
                )
        docs = [(k, key) for k in DOC_KINDS for key in self.keys[k]]
        doc_label = dict((key, k) for k, key in docs)
        for i in range(self.count(CONCEPT_SHARE)):
            word = self.rng.choice(WORDS)
            self.node(
                "concept",
                f"concept:{word}-{i}",
                title=word,
                source_path="vault/_entities.md",
                confidence=0.8,
                props={"aliases": [f"{word}{i}"]},
            )
        pick_concept = self.zipf_picker(self.keys["concept"], 1.05)
        pick_doc = self.zipf_picker([k for _, k in docs], 1.0)
        pick_project = self.zipf_picker(self.keys["project"], 0.9)
        pick_person = self.zipf_picker(self.keys["person"], 0.9)
        pick_company = self.zipf_picker(self.keys["company"], 0.9)
        products = self.keys["product"]
        for kind, key in docs:
            path = f"vault/{kind}/{key}.md"
            for c in sorted({pick_concept() for _ in range(1 + self.poisson(3.0))}):
                self.link("MENTIONS", kind, key, "concept", c, path)
            for d in sorted({pick_doc() for _ in range(self.poisson(1.5))} - {key}):
                self.link("LINKS_TO", kind, key, doc_label[d], d, path)
            if kind in ("task", "decision", "meeting", "contract"):
                self.link("PART_OF_PROJECT", kind, key, "project", pick_project(), path)
            if kind in ("task", "decision"):
                self.link("OWNER", kind, key, "person", pick_person(), path)
            if kind == "meeting":
                for p in sorted({pick_person() for _ in range(2 + self.rng.randrange(4))}):
                    self.link("ATTENDED", "person", p, kind, key, path)
            if kind == "contract":
                self.link("PARTY", kind, key, "company", pick_company(), path)
            if kind in ("decision", "question") and self.rng.random() < 0.5:
                self.link("ABOUT", kind, key, "product", self.rng.choice(products), path)
        # Чанки документов: текст с ключом документа (lexical) и вектором.
        chunked = self.rng.sample(docs, min(len(docs), self.count(CHUNKED_DOC_SHARE)))
        for kind, key in chunked:
            sc = self.scopes(0.2)
            self.sp.chunks.append(
                (
                    Chunk(
                        node_key=key,
                        text=f"{key} {self.text(60)}",
                        source_path=f"vault/{kind}/{key}.md",
                        title=self.text(4),
                        namespace=self.sp.ns,
                        meta={"scopes": sc} if sc else {},
                    ),
                    self.rng.getrandbits(32),
                )
            )

    def observations(self) -> None:
        for i in range(self.count(CUSTOMER_SHARE)):
            self.node(
                "customer",
                f"customer:cu{i}",
                title=f"Клиент {i}",
                source_path="memory:observation",
                confidence=1.0,
                origin="agent",
                props={"scopes": self.scopes(0.6)} if self.rng.random() < 0.6 else {},
            )
        pick_person = self.zipf_picker(self.keys["person"], 0.8)
        pick_project = self.zipf_picker(self.keys["project"], 0.8)
        pick_company = self.zipf_picker(self.keys["company"], 0.8)
        pick_customer = self.zipf_picker(self.keys["customer"], 0.8)
        rels = (
            ("WORKS_ON", "person", pick_person, "project", pick_project),
            ("MANAGES", "person", pick_person, "customer", pick_customer),
            ("SUPPLIES", "company", pick_company, "customer", pick_customer),
            ("CONTRACTED", "customer", pick_customer, "project", pick_project),
        )
        for i in range(self.count(OBS_TEXT_SHARE)):
            occurred = self.when()
            sc = self.scopes(0.6)
            obs = Observation(
                namespace=self.sp.ns,
                source_system="tracker",
                source_stream="events",
                external_id=f"ev-{i}",
                kind=self.rng.choice(["work.completed", "deal.updated", "meeting.held"]),
                occurred_at=iso(occurred),
                scopes=sc,
                content=f"{self.text(25)} T-{self.rng.randrange(9999):05d}",
            ).canonicalize()
            oid = obs.observation_id()
            self.sp.observations.append(obs)
            key = f"{oid}:text:0"
            self.node(
                "note",
                key,
                title=obs.kind,
                source_path="memory:observation",
                confidence=1.0,
                origin="agent",
                props={"content": obs.content, "observation_id": oid, "scopes": sc},
            )
            if self.rng.random() < CHUNKED_OBS_SHARE:
                meta: dict[str, Any] = {"observation_id": oid}
                if sc:
                    meta["scopes"] = sc
                self.sp.chunks.append(
                    (
                        Chunk(
                            node_key=key,
                            text=obs.content,
                            source_path="memory:observation",
                            title=obs.kind,
                            namespace=self.sp.ns,
                            meta=meta,
                        ),
                        self.rng.getrandbits(32),
                    )
                )
            # Факты наблюдения: 0–2, у трети — история (закрытая версия -> новая).
            for _ in range(self.rng.choice((0, 1, 1, 2))):
                etype, la, ps, lb, pd = self.rng.choice(rels)
                s = ps()
                vf = iso(occurred)
                if self.rng.random() < 0.33:
                    t_new = self.when(0.5, 1.0)
                    t_new = max(t_new, occurred + dt.timedelta(days=1))
                    vf_new = iso(t_new)
                    d_new = pd()
                    fid_new = fact_id_for(self.sp.ns, s, etype.lower(), d_new, vf_new)
                    fid_old = self.fact(
                        etype,
                        la,
                        s,
                        lb,
                        pd(),
                        valid_from=vf,
                        valid_to=vf_new,
                        superseded_by=fid_new,
                        observation_ids=[oid],
                        scopes=sc,
                    )
                    self.fact(
                        etype,
                        la,
                        s,
                        lb,
                        d_new,
                        valid_from=vf_new,
                        supersedes=fid_old,
                        observation_ids=[oid],
                        scopes=sc,
                    )
                else:
                    self.fact(
                        etype, la, s, lb, pd(), valid_from=vf, observation_ids=[oid], scopes=sc
                    )

    def software_delivery(self) -> None:
        n_comp = max(3, min(len(COMPONENTS), round(self.size / 4000)))
        comps = list(COMPONENTS[:n_comp])
        snap_from = iso(self.when(0.0, 0.3))
        for c in comps:
            self.node("component", c, origin="agent", confidence=1.0, valid_from=snap_from)
        for kind, share in SD_KINDS.items():
            for i in range(self.count(share)):
                comp = self.rng.choice(comps)
                if kind == "source_file":
                    key = f"{comp}:src/{self.rng.choice(WORDS)}/m{i}.py"
                elif kind == "endpoint":
                    key = f"{self.rng.choice(['GET', 'POST', 'PATCH'])} /api/v1/r{i}/{{}}"
                elif kind == "client_method":
                    key = f"{comp}:client.Client.m{i}"
                elif kind == "ui_call":
                    key = f"console:src/p{i % 97}.tsx:{i}"
                elif kind == "event":
                    key = f"{self.rng.choice(WORDS[:8])}.e{i}"
                elif kind == "table":
                    key = f"{comp}:t{i}"
                else:
                    key = f"{self.rng.choice(['TAI', 'CP', 'MEM'])}-{i:04d}"
                closed = self.rng.random() < 0.05
                self.node(
                    kind,
                    key,
                    source_path=f"snapshot:{comp}",
                    confidence=1.0,
                    origin="agent",
                    props={"attributes": {"repo": comp, "line": self.rng.randrange(2000)}},
                    valid_from=snap_from,
                    valid_to=iso(self.when(0.5, 1.0)) if closed else None,
                )
        files = self.keys["source_file"]
        pick_file = self.zipf_picker(files, 0.7)
        pick_endpoint = self.zipf_picker(self.keys["endpoint"], 1.0)
        pick_event = self.zipf_picker(self.keys["event"], 1.0)

        def snap_fact(etype: str, la: str, s: str, lb: str, d: str, alt: str | None) -> None:
            extra = {
                "attributes": {},
                "snapshot_source": "git",
                "snapshot_scope": "repo",
                "snapshot_id": f"snap-{self.rng.randrange(1000)}",
                "source_path": f"snapshot:{la}",
            }
            if alt is not None and self.rng.random() < 0.15:
                # Прежняя версия связи закрыта новым снимком (конец переехал).
                t_new = iso(self.when(0.4, 1.0))
                fid_new = fact_id_for(self.sp.ns, s, etype.lower(), d, t_new)
                fid_old = self.fact(
                    etype,
                    la,
                    s,
                    lb,
                    alt,
                    valid_from=snap_from,
                    valid_to=t_new,
                    superseded_by=fid_new,
                    extra=extra,
                )
                self.fact(etype, la, s, lb, d, valid_from=t_new, supersedes=fid_old, extra=extra)
            else:
                self.fact(etype, la, s, lb, d, valid_from=snap_from, extra=extra)

        for f in files:
            snap_fact("PART_OF", "source_file", f, "component", f.split(":", 1)[0], None)
        for kind in ("endpoint", "client_method", "ui_call", "event", "table"):
            for key in self.keys[kind]:
                snap_fact("DEFINED_IN", kind, key, "source_file", pick_file(), pick_file())
        for kind in ("client_method", "ui_call"):
            for key in self.keys[kind]:
                for e in sorted({pick_endpoint() for _ in range(1 + self.rng.randrange(2))}):
                    snap_fact("CALLS", kind, key, "endpoint", e, None)
        for f in self.rng.sample(files, len(files) // 10):
            snap_fact("EMITS", "source_file", f, "event", pick_event(), None)
        for adr in self.keys["adr"]:
            for f in sorted({pick_file() for _ in range(2 + self.rng.randrange(7))}):
                snap_fact("GOVERNS", "adr", adr, "source_file", f, None)


def generate(seed: int, nodes: int) -> list[Space]:
    """Сгенерировать основной namespace (90 % узлов) и шумовой (10 %)."""
    out = []
    for ns, share in ((MAIN_NS, 0.9), (NOISE_NS, 0.1)):
        g = Gen(random.Random(f"{seed}:{ns}"), ns, round(nodes * share))
        g.vault()
        g.observations()
        g.software_delivery()
        out.append(g.sp)
    return out


# --- запись в AGE так, как писал код до TASK-001284 ---

_NATURAL_KEY_EXPR = (
    "ag_catalog.agtype_access_operator(VARIADIC ARRAY[properties, "
    "'\"natural_key\"'::ag_catalog.agtype])"
)


def _ensure_label(cur, graph: str, label: str, *, edge: bool, known: set[str]) -> None:
    """Метка и её индексы — как ``GraphStore.ensure_label_index`` кода a4dc7fe."""
    if label in known:
        return
    cur.execute(
        "SELECT 1 FROM ag_catalog.ag_label l JOIN ag_catalog.ag_graph g ON g.graphid = l.graph "
        "WHERE g.name = %s AND l.name = %s",
        (graph, label),
    )
    if cur.fetchone() is None:
        cur.execute(
            f"SELECT ag_catalog.{'create_elabel' if edge else 'create_vlabel'}(%s, %s)",
            (graph, label),
        )
    tbl = f'"{graph}"."{label}"'
    cur.execute(
        f'CREATE INDEX IF NOT EXISTS "{label}_pm_props_gin" ON {tbl} USING gin (properties)'
    )
    if edge:
        cur.execute(f'CREATE INDEX IF NOT EXISTS "{label}_pm_start" ON {tbl} (start_id)')
        cur.execute(f'CREATE INDEX IF NOT EXISTS "{label}_pm_end" ON {tbl} (end_id)')
    else:
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS "{label}_pm_nk_hash" ON {tbl} USING hash '
            f"({_NATURAL_KEY_EXPR})"
        )
    known.add(label)


def _cypher(cur, graph: str, query: str, params: dict[str, Any]) -> None:
    cur.execute(
        f"SELECT * FROM cypher('{graph}', $$ {query} $$, %s) AS (v agtype)", (_Agtype(params),)
    )


def write_age(conn, graph: str, spaces: list[Space], batch: int = 500) -> dict[str, int]:
    """Создать граф AGE ``graph`` и записать узлы и рёбра пачками ``UNWIND``."""
    known: set[str] = set()
    stats = {"nodes": 0, "edges": 0}
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM ag_catalog.ag_graph WHERE name = %s", (graph,))
        if cur.fetchone() is not None:
            raise SystemExit(f"граф AGE {graph!r} уже есть — выберите другое имя или удалите его")
        cur.execute("SELECT ag_catalog.create_graph(%s)", (graph,))
        for sp in spaces:
            for label, rows in sp.nodes.items():
                _ensure_label(cur, graph, label, edge=False, known=known)
                for i in range(0, len(rows), batch):
                    part = rows[i : i + batch]
                    keys = sorted({k for r in part for k in r})
                    sets = ", ".join(f"n.{k} = row.{k}" for k in keys)
                    _cypher(
                        cur,
                        graph,
                        f"UNWIND $rows AS row CREATE (n:{label}) SET {sets}",
                        {"rows": part},
                    )
                    stats["nodes"] += len(part)
        for sp in spaces:
            for (etype, la, lb), rows in sp.edges.items():
                _ensure_label(cur, graph, etype, edge=True, known=known)
                for i in range(0, len(rows), batch):
                    part = rows[i : i + batch]
                    keys = sorted({k for r in part for k in r["p"]})
                    sets = ", ".join(f"r.{k} = row.p.{k}" for k in keys)
                    _cypher(
                        cur,
                        graph,
                        f"UNWIND $rows AS row "
                        f"MATCH (a:{la} {{natural_key: row.s, namespace: $ns}}), "
                        f"(b:{lb} {{natural_key: row.d, namespace: $ns}}) "
                        f"CREATE (a)-[r:{etype}]->(b) SET {sets}",
                        {"rows": part, "ns": sp.ns},
                    )
                    stats["edges"] += len(part)
    return stats


def pseudo_vector(seed: int, dim: int) -> list[float]:
    """Детерминированный нормированный псевдовектор (без модели эмбеддингов)."""
    rng = random.Random(seed)
    v = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    norm = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / norm for x in v]


def write_tables(settings: Settings, spaces: list[Space]) -> dict[str, int]:
    """Чанки (вектор + FTS) и наблюдения — общие таблицы обоих движков."""
    conn = dbmod.connect(settings)
    try:
        index = VectorIndex(conn, settings.chunks_table, settings.embedding_dim, MAIN_NS)
        index.ensure_schema()
        obs_store = ObservationStore(conn, settings.observations_table, MAIN_NS)
        obs_store.ensure_schema()
        n_chunks = n_obs = 0
        for sp in spaces:
            for i in range(0, len(sp.chunks), 200):
                part = sp.chunks[i : i + 200]
                index.add_chunks(
                    [c for c, _ in part],
                    [pseudo_vector(s, settings.embedding_dim) for _, s in part],
                )
                n_chunks += len(part)
            for obs in sp.observations:
                obs_store.insert(obs)
                n_obs += 1
        return {"chunks": n_chunks, "observations": n_obs}
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dsn", default=os.environ.get("CB_DATABASE_URL"), help="База с AGE")
    parser.add_argument("--graph", default="bench_graph", help="Имя графа AGE (схема)")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--nodes", type=int, default=28000, help="Узлов всего (≈)")
    parser.add_argument("--dim", type=int, default=1536, help="Размерность векторов чанков")
    parser.add_argument("--table-prefix", default="bench_", help="Префикс таблиц чанков/наблюдений")
    args = parser.parse_args()
    if not args.dsn:
        sys.exit("Задайте --dsn или CB_DATABASE_URL")

    t0 = time.monotonic()
    spaces = generate(args.seed, args.nodes)
    digest = hashlib.sha256()
    for sp in spaces:
        for label in sorted(sp.nodes):
            for r in sp.nodes[label]:
                digest.update(json.dumps(r, sort_keys=True, ensure_ascii=False).encode())
        for key in sorted(sp.edges):
            for r in sp.edges[key]:
                digest.update(json.dumps([key, r], sort_keys=True, ensure_ascii=False).encode())
    conn = age_connect(args.dsn, jit=False)
    try:
        stats = write_age(conn, args.graph, spaces)
        settings = Settings(
            _env_file=None,
            database_url=args.dsn,
            graph_name=args.graph,
            chunks_table=f"{args.table_prefix}chunks",
            observations_table=f"{args.table_prefix}observations",
            embedding_dim=args.dim,
            default_namespace=MAIN_NS,
        )
        stats.update(write_tables(settings, spaces))
        with conn.cursor() as cur:
            cur.execute("ANALYZE")
    finally:
        conn.close()
    by_ns = {
        sp.ns: {
            "nodes": sum(len(v) for v in sp.nodes.values()),
            "edges": sum(len(v) for v in sp.edges.values()),
        }
        for sp in spaces
    }
    report = {
        "graph": args.graph,
        "seed": args.seed,
        "digest": digest.hexdigest()[:16],
        "seconds": round(time.monotonic() - t0, 1),
        **stats,
        "namespaces": by_ns,
    }
    print(json.dumps(report, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()

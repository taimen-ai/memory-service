"""Разметка сообществ графа и их персист на узлы графа (Phase 3, активация Фазы C).

Связывает перенесённые [[cluster]]/[[analyze]] в живой пайплайн: проекция графа → NetworkX
→ детекция сообществ → стабилизация ID относительно прошлого прогона (remap) → запись
``community``/``cohesion``/``community_name`` обратно на узлы. Опционально — короткая
LLM-метка сообщества по заголовкам его узлов (community-summaries).

Стабильность ID важна: без remap целочисленные id сообществ перетасовываются между
прогонами (та же группировка читается как «дрожание»), что ломает дифы и UI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from platform_memory.graph.cluster import cluster, remap_communities_to_previous, score_all
from platform_memory.graph.projection import to_networkx

if TYPE_CHECKING:
    from platform_memory.graph.store import GraphStore


class SummaryLLM(Protocol):
    """Минимальный контракт LLM для меток сообществ (совпадает с retrieval.LLM)."""

    def answer(self, system: str, user: str) -> str:
        """Сгенерировать ответ по system/user-сообщениям."""
        ...


_SUMMARY_SYSTEM = (
    "Дай очень короткую (2–4 слова) тему для группы связанных сущностей по их заголовкам. "
    "Ответь ТОЛЬКО темой, без кавычек и пояснений."
)


@dataclass(slots=True)
class CommunityResult:
    """Итог разметки сообществ: сколько групп, сколько узлов размечено, метки."""

    community_count: int = 0
    nodes_assigned: int = 0
    labeled: int = 0
    cohesions: dict[int, float] = field(default_factory=dict)
    names: dict[int, str] = field(default_factory=dict)


def _previous_assignment(store: GraphStore) -> dict[str, int]:
    """Прочитать прошлую разметку (node → community) из свойств узлов графа."""
    prev: dict[str, int] = {}
    for props in store.all_nodes():
        nk = props.get("natural_key")
        cid = props.get("community")
        if nk and isinstance(cid, int):
            prev[str(nk)] = cid
    return prev


def assign_communities(
    store: GraphStore,
    *,
    resolution: float = 1.0,
    stable: bool = True,
    llm: SummaryLLM | None = None,
    min_label_size: int = 3,
) -> CommunityResult:
    """Разметить сообщества и записать их на узлы графа.

    ``stable`` (по умолч.) переиндексирует ID сообществ под прошлый прогон. ``llm`` (опц.)
    проставляет короткую метку ``community_name`` сообществам размером ≥ ``min_label_size``.
    """
    # Офлайн-анализ сообществ скоуплен default-namespace стора: чужие базы знаний
    # не участвуют ни в кластеризации, ни в разметке (ADR-017).
    G = to_networkx(store, namespaces=[store.default_namespace])
    if G.number_of_nodes() == 0:
        return CommunityResult()

    communities = cluster(G, resolution=resolution)
    if stable:
        communities = remap_communities_to_previous(communities, _previous_assignment(store))

    cohesions = score_all(G, communities)
    node_to_comm = {n: cid for cid, nodes in communities.items() for n in nodes}

    names: dict[int, str] = {}
    if llm is not None:
        for cid, members in communities.items():
            if len(members) < min_label_size:
                continue
            label = _label_community(G, members, llm=llm)
            if label:
                names[cid] = label

    updated = store.set_node_communities(node_to_comm, cohesions, names)
    return CommunityResult(
        community_count=len(communities),
        nodes_assigned=updated,
        labeled=len(names),
        cohesions=cohesions,
        names=names,
    )


def _label_community(G, members: list[str], *, llm: SummaryLLM, max_titles: int = 15) -> str:
    """Короткая LLM-метка сообщества по заголовкам его узлов (топ по степени)."""
    ranked = sorted(members, key=lambda n: G.degree(n), reverse=True)[:max_titles]
    titles = [str(G.nodes[n].get("label", n)) for n in ranked]
    user = "Заголовки сущностей:\n" + "\n".join(f"- {t}" for t in titles)
    try:
        label = llm.answer(_SUMMARY_SYSTEM, user).strip().strip('"').strip()
    except Exception:  # noqa: BLE001 — метка не критична; пропускаем при сбое LLM
        return ""
    # Защита от болтливого ответа: берём первую строку, ограничиваем длину.
    label = label.splitlines()[0] if label else ""
    return label[:80]

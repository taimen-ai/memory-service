# Адаптировано из graphify/cluster.py (https://github.com/safishamsi/graphify), MIT License.
# Copyright (c) 2026 Safi Shamsi. See THIRD_PARTY.md for the full license text.
# Алгоритм перенесён практически дословно — он работает над произвольным networkx-графом
# и не зависит от модели данных graphify; нужен лишь для проекции нашего графа памяти.
"""Детекция сообществ на NetworkX-графе: Leiden (graspologic) с fallback на Louvain.

Дробит слишком крупные сообщества, считает cohesion-скоры, умеет переиндексировать
сообщества под предыдущую разметку для стабильности ID. Для Phase 3 (community-summaries)
и обзорного retrieval.
"""

from __future__ import annotations

import contextlib
import inspect
import io
import json
import sys

import networkx as nx


def _suppress_output():
    """Заглушить stdout библиотек (graspologic печатает ANSI-прогрессбары)."""
    return contextlib.redirect_stdout(io.StringIO())


def _partition(G: nx.Graph, resolution: float = 1.0) -> dict[str, int]:
    """Детекция сообществ: {node_id: community_id}. Leiden → Louvain-fallback.

    resolution > 1.0 → больше мелких сообществ; < 1.0 → меньше крупных.
    Граф пересобирается в детерминированном порядке узлов/рёбер для воспроизводимости.
    """
    stable = nx.Graph()
    stable.add_nodes_from(sorted(G.nodes(), key=str))
    edge_rows = sorted(
        G.edges(data=True),
        key=lambda row: (
            str(row[0]),
            str(row[1]),
            json.dumps(row[2], sort_keys=True, ensure_ascii=False, default=str),
        ),
    )
    for src, tgt, attrs in edge_rows:
        stable.add_edge(src, tgt, **attrs)

    try:
        from graspologic.partition import leiden

        lsig = inspect.signature(leiden).parameters
        kwargs: dict = {}
        if "random_seed" in lsig:
            kwargs["random_seed"] = 42
        if "trials" in lsig:
            kwargs["trials"] = 1
        if "resolution" in lsig:
            kwargs["resolution"] = resolution
        old_stderr = sys.stderr
        try:
            sys.stderr = io.StringIO()
            with _suppress_output():
                result = leiden(stable, **kwargs)
        finally:
            sys.stderr = old_stderr
        return result
    except ImportError:
        pass

    # Fallback: networkx louvain (есть начиная с networkx 2.7).
    kwargs = {"seed": 42, "threshold": 1e-4, "resolution": resolution}
    if "max_level" in inspect.signature(nx.community.louvain_communities).parameters:
        kwargs["max_level"] = 10
    communities = nx.community.louvain_communities(stable, **kwargs)
    return {node: cid for cid, nodes in enumerate(communities) for node in nodes}


_MAX_COMMUNITY_FRACTION = 0.25  # сообщества крупнее 25% графа дробятся
_MIN_SPLIT_SIZE = 10  # дробим только при ≥ стольких узлов
_COHESION_SPLIT_THRESHOLD = 0.05  # пере-дробить сообщества с cohesion ниже этого
_COHESION_SPLIT_MIN_SIZE = 50  # cohesion-дробление только при ≥ стольких узлов


def cluster(
    G: nx.Graph,
    resolution: float = 1.0,
    exclude_hubs_percentile: float | None = None,
) -> dict[int, list[str]]:
    """Детекция сообществ. Возвращает {community_id: [node_ids]}, 0 = крупнейшее.

    Крупные сообщества (> 25% узлов, мин 10) дробятся вторым проходом. Принимает
    directed/undirected (DiGraph внутри приводится к undirected).
    ``exclude_hubs_percentile`` (0–100): узлы со степенью выше перцентиля исключаются
    из разбиения и переподключаются к сообществу-большинству соседей (super-hubs).
    """
    if G.number_of_nodes() == 0:
        return {}
    if G.is_directed():
        G = G.to_undirected()
    if G.number_of_edges() == 0:
        return {i: [n] for i, n in enumerate(sorted(G.nodes))}

    hub_nodes: set[str] = set()
    if exclude_hubs_percentile is not None:
        degrees = sorted(d for _, d in G.degree())
        if degrees:
            idx = max(0, int(len(degrees) * exclude_hubs_percentile / 100) - 1)
            threshold = degrees[idx]
            hub_nodes = {n for n, d in G.degree() if d > threshold}

    excluded = hub_nodes
    isolates = [n for n in G.nodes() if G.degree(n) == 0 and n not in excluded]
    connected_nodes = [n for n in G.nodes() if G.degree(n) > 0 and n not in excluded]
    connected = G.subgraph(connected_nodes)

    raw: dict[int, list[str]] = {}
    if connected.number_of_nodes() > 0:
        partition = _partition(connected, resolution=resolution)
        for node, cid in partition.items():
            raw.setdefault(cid, []).append(node)

    next_cid = max(raw.keys(), default=-1) + 1
    for node in isolates:
        raw[next_cid] = [node]
        next_cid += 1

    if hub_nodes:
        node_community: dict[str, int] = {n: cid for cid, nodes in raw.items() for n in nodes}
        for hub in sorted(hub_nodes):
            votes: dict[int, int] = {}
            for nb in G.neighbors(hub):
                cid = node_community.get(nb)
                if cid is not None:
                    votes[cid] = votes.get(cid, 0) + 1
            if votes:
                best = min(votes, key=lambda c: (-votes[c], c))
                raw.setdefault(best, []).append(hub)
                node_community[hub] = best
            else:
                raw[next_cid] = [hub]
                node_community[hub] = next_cid
                next_cid += 1

    max_size = max(_MIN_SPLIT_SIZE, int(G.number_of_nodes() * _MAX_COMMUNITY_FRACTION))
    final_communities: list[list[str]] = []
    for nodes in raw.values():
        if len(nodes) > max_size:
            final_communities.extend(_split_community(G, nodes))
        else:
            final_communities.append(nodes)

    second_pass: list[list[str]] = []
    for nodes in final_communities:
        if (
            len(nodes) >= _COHESION_SPLIT_MIN_SIZE
            and cohesion_score(G, nodes) < _COHESION_SPLIT_THRESHOLD
        ):
            splits = _split_community(G, nodes)
            second_pass.extend(splits if len(splits) > 1 else [nodes])
        else:
            second_pass.append(nodes)
    final_communities = second_pass

    # Тотальный порядок (размер ↓, затем лексикографически) → воспроизводимые ID.
    final_communities.sort(key=lambda nodes: (-len(nodes), tuple(sorted(map(str, nodes)))))
    return {i: sorted(nodes) for i, nodes in enumerate(final_communities)}


def _split_community(G: nx.Graph, nodes: list[str]) -> list[list[str]]:
    """Второй проход разбиения на подграфе сообщества."""
    subgraph = G.subgraph(nodes)
    if subgraph.number_of_edges() == 0:
        return [[n] for n in sorted(nodes)]
    try:
        sub_partition = _partition(subgraph)
        sub_communities: dict[int, list[str]] = {}
        for node, cid in sub_partition.items():
            sub_communities.setdefault(cid, []).append(node)
        if len(sub_communities) <= 1:
            return [sorted(nodes)]
        return [sorted(v) for v in sub_communities.values()]
    except Exception:
        return [sorted(nodes)]


def cohesion_score(G: nx.Graph, community_nodes: list[str]) -> float:
    """Доля реальных внутрисообщественных рёбер от максимально возможных."""
    n = len(community_nodes)
    if n <= 1:
        return 1.0
    subgraph = G.subgraph(community_nodes)
    actual = subgraph.number_of_edges()
    possible = n * (n - 1) / 2
    return actual / possible if possible > 0 else 0.0


def score_all(G: nx.Graph, communities: dict[int, list[str]]) -> dict[int, float]:
    """Cohesion-скор для каждого сообщества."""
    return {cid: cohesion_score(G, nodes) for cid, nodes in communities.items()}


def remap_communities_to_previous(
    communities: dict[int, list[str]],
    previous_node_community: dict[str, int],
) -> dict[int, list[str]]:
    """Переиндексировать ID сообществ под предыдущую разметку (макс. перекрытие).

    Жадное one-to-one сопоставление по размеру пересечения; несопоставленным —
    свежие ID в детерминированном порядке. Гасит «дрожание» community ID между прогонами.
    """
    if not communities:
        return {}

    new_sets = {cid: set(nodes) for cid, nodes in communities.items()}
    old_sets: dict[int, set[str]] = {}
    for node, old_cid in previous_node_community.items():
        old_sets.setdefault(old_cid, set()).add(node)

    overlaps: list[tuple[int, int, int]] = []
    for old_cid, old_nodes in old_sets.items():
        for new_cid, new_nodes in new_sets.items():
            overlap = len(old_nodes & new_nodes)
            if overlap > 0:
                overlaps.append((overlap, old_cid, new_cid))
    overlaps.sort(key=lambda x: (-x[0], x[1], x[2]))

    new_to_final: dict[int, int] = {}
    used_old_ids: set[int] = set()
    matched_new_ids: set[int] = set()
    for _overlap, old_cid, new_cid in overlaps:
        if old_cid in used_old_ids or new_cid in matched_new_ids:
            continue
        new_to_final[new_cid] = old_cid
        used_old_ids.add(old_cid)
        matched_new_ids.add(new_cid)

    unmatched = [cid for cid in communities if cid not in matched_new_ids]
    unmatched.sort(key=lambda cid: (-len(communities[cid]), tuple(sorted(communities[cid]))))
    next_id = 0
    for new_cid in unmatched:
        while next_id in used_old_ids:
            next_id += 1
        new_to_final[new_cid] = next_id
        used_old_ids.add(next_id)
        next_id += 1

    remapped: dict[int, list[str]] = {}
    for new_cid, nodes in communities.items():
        remapped[new_to_final[new_cid]] = sorted(nodes)
    return dict(sorted(remapped.items(), key=lambda kv: kv[0]))

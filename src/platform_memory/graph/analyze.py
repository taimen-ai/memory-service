# Адаптировано из graphify/analyze.py (https://github.com/safishamsi/graphify), MIT License.
# Copyright (c) 2026 Safi Shamsi. See THIRD_PARTY.md for the full license text.
#
# Перенесены метрики обзора графа: god-узлы (самые связанные сущности), cross-community
# связи (мосты между сообществами) и авто-вопросы. НЕ перенесён код-специфичный слой
# graphify (языковые семейства, AST-method-stubs, import-циклы, JSON-шум, confidence-теги
# рёбер) — он бессмыслен для бизнес-графа Company Brain. Сигналы и тексты вопросов
# адаптированы под наши узлы (label/type/source_file) и русскоязычный домен.
"""Обзорный анализ графа знаний: god-узлы, мосты между сообществами, авто-вопросы.

Работает над проекцией графа памяти в NetworkX ([[projection]]). Питает обзорный retrieval
и Phase 3 (community-summaries): что в графе центрально, что неожиданно связано и какие
вопросы граф уникально способен прояснить.
"""

from __future__ import annotations

import networkx as nx

from platform_memory.graph.cluster import cohesion_score

# Синтетические узлы-хабы: связаны со всем по построению (узлы-проекты, якоря трейсов).
# Исключаются из god-узлов/мостов/вопросов, чтобы не вытеснять реальные сущности.
_DEFAULT_HUB_TYPES = frozenset({"project", "pc_trace"})


def _node_community_map(communities: dict[int, list[str]]) -> dict[str, int]:
    """Инвертировать {cid: [nodes]} → {node: cid}."""
    return {n: cid for cid, nodes in communities.items() for n in nodes}


def _label(G: nx.Graph, node_id: str) -> str:
    return G.nodes[node_id].get("label", node_id)


def _is_hub(G: nx.Graph, node_id: str, hub_types: frozenset[str]) -> bool:
    """Синтетический узел-хаб (по типу) — исключается из обзорных метрик."""
    return G.nodes[node_id].get("type", "") in hub_types


def god_nodes(
    G: nx.Graph,
    top_n: int = 10,
    *,
    hub_types: frozenset[str] = _DEFAULT_HUB_TYPES,
) -> list[dict]:
    """Топ-N самых связанных реальных сущностей — ядро графа знаний.

    Синтетические хабы (узлы-проекты, якоря трейсов) исключаются: они копят рёбра
    механически и не являются содержательными центрами.
    """
    degree = dict(G.degree())
    result: list[dict] = []
    for node_id, deg in sorted(degree.items(), key=lambda x: (-x[1], str(x[0]))):
        if _is_hub(G, node_id, hub_types):
            continue
        result.append(
            {
                "id": node_id,
                "label": _label(G, node_id),
                "type": G.nodes[node_id].get("type", "entity"),
                "degree": deg,
            }
        )
        if len(result) >= top_n:
            break
    return result


def cross_community_connections(
    G: nx.Graph,
    communities: dict[int, list[str]] | None = None,
    top_n: int = 5,
    *,
    hub_types: frozenset[str] = _DEFAULT_HUB_TYPES,
) -> list[dict]:
    """Связи-мосты между разными сообществами — структурно неожиданные.

    Если разметка сообществ задана — берём рёбра, чьи концы в разных сообществах
    (по одному представителю на пару сообществ). Иначе — рёбра с высокой реберной
    betweenness (мосты структуры).
    """
    communities = communities or {}
    if not communities:
        if G.number_of_edges() == 0 or G.number_of_nodes() > 5000:
            return []
        betweenness = nx.edge_betweenness_centrality(G)
        top_edges = sorted(betweenness.items(), key=lambda x: (-x[1], str(x[0])))[:top_n]
        out: list[dict] = []
        for (u, v), score in top_edges:
            data = G.get_edge_data(u, v) or {}
            out.append(
                {
                    "source": _label(G, u),
                    "target": _label(G, v),
                    "relation": data.get("relation", ""),
                    "note": f"мост структуры (betweenness={score:.3f})",
                }
            )
        return out

    node_community = _node_community_map(communities)
    surprises: list[dict] = []
    for u, v, data in G.edges(data=True):
        cid_u, cid_v = node_community.get(u), node_community.get(v)
        if cid_u is None or cid_v is None or cid_u == cid_v:
            continue
        if _is_hub(G, u, hub_types) or _is_hub(G, v, hub_types):
            continue
        surprises.append(
            {
                "source": _label(G, u),
                "target": _label(G, v),
                "relation": data.get("relation", ""),
                "note": f"мост между сообществами {cid_u} → {cid_v}",
                "_pair": tuple(sorted((cid_u, cid_v))),
            }
        )

    # Один представитель на пару сообществ — иначе один хаб задавит выдачу.
    seen_pairs: set[tuple] = set()
    deduped: list[dict] = []
    for s in sorted(surprises, key=lambda x: (x["_pair"], x["source"], x["target"])):
        pair = s.pop("_pair")
        if pair not in seen_pairs:
            seen_pairs.add(pair)
            deduped.append(s)
    return deduped[:top_n]


def suggest_questions(
    G: nx.Graph,
    communities: dict[int, list[str]],
    community_labels: dict[int, str] | None = None,
    top_n: int = 7,
    *,
    hub_types: frozenset[str] = _DEFAULT_HUB_TYPES,
) -> list[dict]:
    """Сгенерировать вопросы, которые граф уникально способен прояснить.

    Сигналы: узлы-мосты (betweenness), god-узлы, изолированные узлы, низко-cohesion
    сообщества. Каждый вопрос — {type, question, why}.
    """
    community_labels = community_labels or {}
    if community_labels:
        community_labels = {
            int(k) if isinstance(k, str) else k: v for k, v in community_labels.items()
        }

    questions: list[dict] = []
    node_community = _node_community_map(communities)

    # 1. Узлы-мосты (высокая betweenness) → вопросы о сквозных связях.
    if G.number_of_edges() > 0:
        k = min(100, G.number_of_nodes()) if G.number_of_nodes() > 1000 else None
        betweenness = nx.betweenness_centrality(G, k=k, seed=42)
        bridges = sorted(
            [(n, s) for n, s in betweenness.items() if not _is_hub(G, n, hub_types) and s > 0],
            key=lambda x: (-x[1], str(x[0])),
        )[:3]
        for node_id, score in bridges:
            label = _label(G, node_id)
            cid = node_community.get(node_id)
            comm_label = community_labels.get(cid, f"Сообщество {cid}") if cid is not None else "?"
            neighbor_comms = {
                node_community.get(n) for n in G.neighbors(node_id) if node_community.get(n) != cid
            }
            neighbor_comms.discard(None)
            if neighbor_comms:
                others = [community_labels.get(c, f"Сообщество {c}") for c in neighbor_comms]
                questions.append(
                    {
                        "type": "bridge_node",
                        "question": f"Почему «{label}» связывает «{comm_label}» с "
                        f"{', '.join(f'«{o}»' for o in others)}?",
                        "why": f"Высокая betweenness ({score:.3f}) — узел-мост между сообществами.",
                    }
                )

    # 2. God-узлы → вопросы о центральных сущностях.
    for gn in god_nodes(G, top_n=3, hub_types=hub_types):
        if gn["degree"] >= 3:
            questions.append(
                {
                    "type": "god_node",
                    "question": f"Какова роль «{gn['label']}» в общей картине компании?",
                    "why": f"Одна из самых связанных сущностей (степень {gn['degree']}).",
                }
            )

    # 3. Изолированные/слабосвязанные узлы → вопросы об интеграции.
    isolated = [n for n in G.nodes() if G.degree(n) <= 1 and not _is_hub(G, n, hub_types)]
    if isolated:
        labels = [_label(G, n) for n in sorted(isolated)[:3]]
        questions.append(
            {
                "type": "isolated_nodes",
                "question": f"Что связывает {', '.join(f'«{lbl}»' for lbl in labels)} "
                f"с остальной базой знаний?",
                "why": f"{len(isolated)} слабосвязанных узлов — возможные пробелы в связях.",
            }
        )

    # 4. Низко-cohesion сообщества → структурные вопросы.
    for cid, nodes in communities.items():
        score = cohesion_score(G, nodes)
        if score < 0.15 and len(nodes) >= 5:
            label = community_labels.get(cid, f"Сообщество {cid}")
            questions.append(
                {
                    "type": "low_cohesion",
                    "question": f"Стоит ли разбить «{label}» на более узкие темы?",
                    "why": f"Cohesion {score:.3f} — узлы слабо связаны между собой.",
                }
            )

    if not questions:
        return [
            {
                "type": "no_signal",
                "question": None,
                "why": "Недостаточно сигнала: нет узлов-мостов, изолятов и низко-cohesion "
                "сообществ. Добавьте больше заметок/связей.",
            }
        ]
    return questions[:top_n]

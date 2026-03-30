from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import networkx as nx

from .complexity import ComplexityWeights, compute_complexity_score


def build_demand_undirected_graph(demand_counts: Dict[Tuple[str, str], int]) -> nx.Graph:
    UG = nx.Graph()
    for (src, dst), w in demand_counts.items():
        if not src or not dst or src == dst:
            continue
        if UG.has_edge(src, dst):
            UG[src][dst]["weight"] += w
        else:
            UG.add_edge(src, dst, weight=float(w))
    return UG


def _component_nodes(demand_counts: Dict[Tuple[str, str], int]) -> List[Set[str]]:
    UG = build_demand_undirected_graph(demand_counts)
    return [set(c) for c in nx.connected_components(UG)] if UG.number_of_nodes() else []


def _undirected_demand_weight(demand_counts: Dict[Tuple[str, str], int], a: str, b: str) -> float:
    return float(demand_counts.get((a, b), 0) + demand_counts.get((b, a), 0))


def _score_tree(
    *,
    demand_counts: Dict[Tuple[str, str], int],
    links: Iterable[Tuple[str, str]],
    weights: ComplexityWeights,
    object_count: int,
) -> float:
    return compute_complexity_score(
        demand_counts=demand_counts,
        channel_links=links,
        weights=weights,
        object_count=object_count,
    )["score"]


def select_channel_topology(
    demand_counts: Dict[Tuple[str, str], int],
    *,
    qms_in_component: Optional[Set[str]] = None,
    max_hub_candidates: int = 3,
    weights: ComplexityWeights = ComplexityWeights(),
    object_count: int = 0,
    neighbourhood_map: Optional[Dict[str, str]] = None,
    return_component_decisions: bool = False,
) -> Union[List[Tuple[str, str]], Tuple[List[Tuple[str, str]], List[Dict[str, Any]]]]:
    """
    Select a deterministic, acyclic channel link topology (forest of trees).

    Heuristic:
    - For each demand-connected component, evaluate:
      1) Star trees around top-k hub candidates (min hop; high fan-out)
      2) A maximum spanning tree over weighted candidate edges (balances fan)
    - Pick the topology with the lowest complexity score.
    """
    if not demand_counts:
        return []

    comps = [qms_in_component] if qms_in_component else _component_nodes(demand_counts)

    all_links: List[Tuple[str, str]] = []
    component_decisions: List[Dict[str, Any]] = []
    for comp_nodes in comps:
        comp_nodes = set(comp_nodes)
        if len(comp_nodes) <= 1:
            continue

        # Important: evaluate candidate topologies only against demands inside
        # this component. Otherwise, demand pairs in other components become
        # “unroutable” for every candidate and distort the scoring.
        demand_counts_comp: Dict[Tuple[str, str], int] = {
            (s, d): w
            for (s, d), w in demand_counts.items()
            if s in comp_nodes and d in comp_nodes and s != d
        }

        # Candidate hubs by total demand incident weight.
        hub_scores = {}
        for n in comp_nodes:
            total = 0
            for (src, dst), w in demand_counts.items():
                if src == n or dst == n:
                    total += w
            hub_scores[n] = total
        hub_sorted = sorted(hub_scores.items(), key=lambda kv: (-kv[1], kv[0]))
        hubs = [n for n, _ in hub_sorted[:max_hub_candidates]]

        # Star candidate(s).
        tree_candidates: List[Tuple[List[Tuple[str, str]], Dict[str, Any]]] = []
        for hub in hubs:
            links: List[Tuple[str, str]] = []
            for other in sorted(comp_nodes):
                if other == hub:
                    continue
                a, b = sorted([hub, other])
                links.append((a, b))
            tree_candidates.append((links, {"type": "star", "hub": hub}))

        # MST candidate based on weighted complete graph (restricted to comp_nodes).
        # Use a deterministic Kruskal by setting weights, and then pick max spanning tree.
        def edge_weight(a: str, b: str) -> float:
            d = _undirected_demand_weight(demand_counts, a, b)
            if neighbourhood_map and neighbourhood_map.get(a) and neighbourhood_map.get(b):
                if neighbourhood_map[a] == neighbourhood_map[b]:
                    d += 0.25 * d + 1.0  # within-neighbourhood bonus
                else:
                    d += 0.1  # across-neighbourhood small bias
            else:
                if d == 0:
                    d = 0.05  # tie-break to keep MST connected deterministically
            return float(d)

        # Build weighted graph for Kruskal.
        WG = nx.Graph()
        WG.add_nodes_from(comp_nodes)
        for i, a in enumerate(sorted(comp_nodes)):
            for b in sorted(comp_nodes)[i + 1 :]:
                WG.add_edge(a, b, weight=edge_weight(a, b))

        mst_edges: List[Tuple[str, str]] = []
        if WG.number_of_edges() > 0:
            # Maximum spanning tree: negate weights.
            mst = nx.maximum_spanning_tree(WG, weight="weight")
            mst_edges = [tuple(sorted((u, v))) for u, v in mst.edges()]

        if mst_edges:
            tree_candidates.append((mst_edges, {"type": "mst"}))

        # Evaluate candidates; pick best score.
        best_links = None
        best_score = None
        best_meta: Dict[str, Any] = {}
        for links, meta in tree_candidates:
            # Ensure it’s a tree-like link set for the component size.
            uniq = set(tuple(sorted(e)) for e in links)
            links_dedup = sorted(list(uniq))
            if len(links_dedup) != len(comp_nodes) - 1:
                continue
            score = _score_tree(
                demand_counts=demand_counts_comp,
                links=links_dedup,
                weights=weights,
                object_count=object_count,
            )
            if best_score is None or score < best_score:
                best_score = score
                best_links = links_dedup
                best_meta = meta

        if best_links is None:
            # Fallback: just connect to smallest node as hub.
            hub = sorted(comp_nodes)[0]
            best_links = []
            for other in sorted(comp_nodes):
                if other == hub:
                    continue
                best_links.append(tuple(sorted((hub, other))))

        all_links.extend(best_links)
        if return_component_decisions:
            component_decisions.append(
                {
                    "component_nodes": sorted(list(comp_nodes)),
                    "selected_links_undirected": [list(x) for x in best_links],
                    "selected_meta": best_meta,
                    "selected_component_score": float(best_score) if best_score is not None else None,
                    "candidate_count": len(tree_candidates),
                }
            )

    # Return deterministic ordering.
    all_links_unique = sorted(set(tuple(sorted(e)) for e in all_links))
    if return_component_decisions:
        return all_links_unique, component_decisions
    return all_links_unique


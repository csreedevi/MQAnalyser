from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple

import networkx as nx


@dataclass(frozen=True)
class ComplexityWeights:
    # Primary knobs for your “quantitative complexity” metric.
    # Weights are intentionally simple and interpretable.
    w_channels: float = 1.0
    w_hops: float = 1.5
    w_fan: float = 0.75
    w_cycles: float = 2.0
    w_objects: float = 0.25


def _safe_div(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return a / b


def build_channel_digraph(channel_links: Iterable[Tuple[str, str]]) -> nx.DiGraph:
    """
    Convert undirected links into a deterministic directed channel graph.
    For each undirected link (A,B), we add A->B and B->A.
    """
    G = nx.DiGraph()
    for a, b in channel_links:
        if a == b:
            continue
        G.add_edge(a, b)
        G.add_edge(b, a)
    return G


def compute_weighted_hops(
    demand_counts: Dict[Tuple[str, str], int], channel_links: Iterable[Tuple[str, str]]
) -> Tuple[float, List[Dict]]:
    """
    Weighted sum of shortest-path hop counts for each demand (src->dst).
    Returns (total_weighted_hops, per_demand_details).
    """
    links = list(channel_links)
    UG = nx.Graph()
    UG.add_edges_from(links)

    total = 0.0
    details: List[Dict] = []
    for (src, dst), w in sorted(demand_counts.items()):
        if src not in UG or dst not in UG:
            # Unroutable demand; treat as large penalty.
            hops = 999.0
            total += w * hops
            details.append({"src": src, "dst": dst, "weight": w, "hops": hops, "routable": False})
            continue
        try:
            hops = float(nx.shortest_path_length(UG, src, dst))
            total += w * hops
            details.append({"src": src, "dst": dst, "weight": w, "hops": hops, "routable": True})
        except nx.NetworkXNoPath:
            hops = 999.0
            total += w * hops
            details.append({"src": src, "dst": dst, "weight": w, "hops": hops, "routable": False})
    return total, details


def count_cycles_in_undirected_graph(UG: nx.Graph) -> int:
    """
    Use an undirected notion of cycles (cycle basis).

    For a tree-like undirected topology, this returns 0, which better matches
    "avoid cycles / routing loops" at the QM connectivity layer.
    """
    try:
        return len(nx.cycle_basis(UG))
    except Exception:
        return 0


def compute_complexity_score(
    *,
    demand_counts: Dict[Tuple[str, str], int],
    channel_links: Iterable[Tuple[str, str]],
    weights: ComplexityWeights,
    object_count: int = 0,
) -> Dict:
    """
    Complexity metric tailored to MQ topology:
    - Channels: number of directed channels required by introduced links.
    - Routing hops: shortest path length in introduced channel topology.
    - Fan: max node degree in directed channel graph.
    - Cycles: number of simple cycles in directed graph.
    - Objects: number of MQ objects in the dataset (approx).
    """
    links = list(channel_links)
    channel_digraph = build_channel_digraph(links)

    # Channels term.
    directed_channel_count = 2 * len(links)

    # Hops term.
    weighted_hops_total, _details = compute_weighted_hops(demand_counts, links)

    # Fan term.
    out_degrees = dict(channel_digraph.out_degree())
    max_fan_out = max(out_degrees.values()) if out_degrees else 0

    # Cycles term (undirected cycle basis).
    UG = nx.Graph()
    UG.add_edges_from(links)
    cycles = count_cycles_in_undirected_graph(UG) if UG.number_of_nodes() <= 60 else 0

    score = (
        weights.w_channels * float(directed_channel_count)
        + weights.w_hops * float(weighted_hops_total)
        + weights.w_fan * float(max_fan_out)
        + weights.w_cycles * float(cycles)
        + weights.w_objects * float(object_count)
    )

    return {
        "directed_channel_count": directed_channel_count,
        "weighted_hops_total": float(weighted_hops_total),
        "max_fan_out": int(max_fan_out),
        "cycles": int(cycles),
        "object_count": int(object_count),
        "score": float(score),
        "weights": {
            "w_channels": weights.w_channels,
            "w_hops": weights.w_hops,
            "w_fan": weights.w_fan,
            "w_cycles": weights.w_cycles,
            "w_objects": weights.w_objects,
        },
    }


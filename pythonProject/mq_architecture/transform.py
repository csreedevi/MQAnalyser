from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from .complexity import ComplexityWeights, compute_complexity_score
from .discovery import dedupe_rows, find_app_violations, find_orphan_candidates, build_demand_pairs, build_qm_app_ownership
from .planner import select_channel_topology
from .schema import ColumnMap


def _clean_str(x: object) -> str:
    if x is None:
        return ""
    return str(x).strip()


def auto_fix_ownership_conflicts(
    df_in: pd.DataFrame,
    cm: ColumnMap,
    *,
    tie_breaker: str = "sorted",
    return_mapping: bool = False,
) -> "pd.DataFrame | tuple[pd.DataFrame, Dict[str, str]]":
    """
    Optional constraint auto-fix:
    - app_id must map to exactly one queue manager

    Strategy: for each app_id, pick the most frequently observed queue manager
    and reassign all rows of that app_id to it.
    """
    if not cm.app_id:
        return df_in

    df = df_in.copy()
    app_to_counts: Dict[str, Dict[str, int]] = {}
    for _, row in df.iterrows():
        app = _clean_str(row.get(cm.app_id, ""))
        qm = _clean_str(row.get(cm.queue_manager_name, ""))
        if not app or not qm:
            continue
        app_to_counts.setdefault(app, {})
        app_to_counts[app][qm] = app_to_counts[app].get(qm, 0) + 1

    app_to_majority_qm: Dict[str, str] = {}
    for app, counts in app_to_counts.items():
        if not counts:
            continue
        max_count = max(counts.values())
        best_qms = [q for q, c in counts.items() if c == max_count]
        app_to_majority_qm[app] = sorted(best_qms)[0] if tie_breaker == "sorted" else best_qms[0]

    # Reassign queue managers for the conflicting app_ids.
    for idx, row in df.iterrows():
        app = _clean_str(row.get(cm.app_id, ""))
        if app in app_to_majority_qm:
            df.at[idx, cm.queue_manager_name] = app_to_majority_qm[app]

    if return_mapping:
        return df, app_to_majority_qm
    return df


def _build_neighbourhood_map(df: pd.DataFrame, cm: ColumnMap) -> Dict[str, str]:
    if not cm.primary_neighbourhood:
        return {}
    # Use the first non-empty neighbourhood per QM to keep deterministic.
    mapping: Dict[str, str] = {}
    for _, row in df.iterrows():
        qm = _clean_str(row.get(cm.queue_manager_name, ""))
        nh = _clean_str(row.get(cm.primary_neighbourhood, ""))
        if qm and nh and qm not in mapping:
            mapping[qm] = nh
    return mapping


def _as_is_channel_links_from_demand(demand_counts: Dict[Tuple[str, str], int]) -> List[Tuple[str, str]]:
    # As-is assumption: if there is a demand src->dst, assume a direct channel path exists.
    # We model that as an undirected link between src and dst.
    links = set()
    for (src, dst), _w in demand_counts.items():
        if not src or not dst or src == dst:
            continue
        a, b = sorted([src, dst])
        links.add((a, b))
    return sorted(links)


def _channel_row_template(columns: Sequence[str]) -> Dict[str, str]:
    return {c: "" for c in columns}


def _make_channel_rows(
    *,
    channel_links: Iterable[Tuple[str, str]],
    cm: ColumnMap,
    columns: Sequence[str],
) -> List[Dict[str, str]]:
    """
    Represent channel objects in the same schema by mapping:
    - queue manager name = channel owner (from QM)
    - queue name = deterministic channel name
    - remote queue manager name = other end QM
    - q type / usage = channel marker (best-effort)
    """
    rows: List[Dict[str, str]] = []

    for a, b in sorted(set(tuple(sorted(e)) for e in channel_links)):
        from_qm = a
        to_qm = b

        # Sender channel: fromQM.toQM
        sender_name = f"{from_qm}.to.{to_qm}"
        r = _channel_row_template(columns)
        r[cm.queue_manager_name] = from_qm
        r[cm.queue_name] = sender_name
        if cm.remote_queue_manager_name:
            r[cm.remote_queue_manager_name] = to_qm
        if cm.q_type:
            r[cm.q_type] = "CHANNEL"
        if cm.usage:
            r[cm.usage] = "CHANNEL_SENDER"
        rows.append(r)

        # Receiver channel: toQM.fromQM
        receiver_name = f"{to_qm}.from.{from_qm}"
        r2 = _channel_row_template(columns)
        r2[cm.queue_manager_name] = to_qm
        r2[cm.queue_name] = receiver_name
        if cm.remote_queue_manager_name:
            r2[cm.remote_queue_manager_name] = from_qm
        if cm.q_type:
            r2[cm.q_type] = "CHANNEL"
        if cm.usage:
            r2[cm.usage] = "CHANNEL_RECEIVER"
        rows.append(r2)

    return rows


def plan_target_topology(
    df_in: pd.DataFrame,
    columns: List[str],
    cm: ColumnMap,
    *,
    weights: ComplexityWeights = ComplexityWeights(),
    max_hub_candidates: int = 3,
    object_count: Optional[int] = None,
) -> Dict:
    """
    Create a human-in-loop plan:
    - proposed channel links
    - dedupe candidates
    - orphan removal candidates (conservative)
    - as-is vs target complexity scores
    """
    # Dedupe first so the “before fix” snapshot is stable/deterministic.
    df_deduped, dupes = dedupe_rows(df_in, cm)

    # Constraint snapshot before enforcement.
    ownership_before = build_qm_app_ownership(df_deduped, cm)
    ownership_violations_before = find_app_violations(ownership_before)

    # Enforce constraint at code-level (no external flag required):
    # app_id is mapped to exactly one queue manager.
    df_enforced, app_to_majority_qm = auto_fix_ownership_conflicts(
        df_in, cm, return_mapping=True
    )
    df_enforced_deduped, _dupes2 = dedupe_rows(df_enforced, cm)

    demand_counts, _qms_involved = build_demand_pairs(df_enforced_deduped, cm)

    ownership_after = build_qm_app_ownership(df_enforced_deduped, cm)
    ownership_violations_after = find_app_violations(ownership_after)

    orphan_candidates = find_orphan_candidates(df_enforced_deduped, cm)

    # Neighbourhood map (optional) used as a small deterministic bias.
    neighbourhood_map = _build_neighbourhood_map(df_enforced_deduped, cm)

    as_is_links = _as_is_channel_links_from_demand(demand_counts)
    obj_cnt = int(object_count if object_count is not None else len(df_enforced_deduped))

    as_is_complexity = compute_complexity_score(
        demand_counts=demand_counts,
        channel_links=as_is_links,
        weights=weights,
        object_count=obj_cnt,
    )

    target_links, component_decisions = select_channel_topology(
        demand_counts,
        weights=weights,
        object_count=obj_cnt,
        max_hub_candidates=max_hub_candidates,
        neighbourhood_map=neighbourhood_map,
        return_component_decisions=True,
    )

    target_complexity = compute_complexity_score(
        demand_counts=demand_counts,
        channel_links=target_links,
        weights=weights,
        object_count=obj_cnt,
    )

    plan = {
        "as_is": {
            "assumed_channel_links_undirected": [[a, b] for (a, b) in as_is_links],
            "complexity": as_is_complexity,
        },
        "target": {
            "proposed_channel_links_undirected": [[a, b] for (a, b) in target_links],
            "complexity": target_complexity,
        },
        "constraints": {
            "ownership_violations_before_fix": ownership_violations_before,
            "ownership_violations_after_fix": ownership_violations_after,
            "auto_fix_applied": {
                "tie_breaker": "sorted",
                "app_to_majority_qm": app_to_majority_qm,
            },
            "enforced": [
                "one queue manager per application (enforced at planning time; auto-fix via majority rule when violated)",
                "application connects only to its own queue manager (indirectly enforced by the app_id -> queue manager ownership mapping)",
                "deterministic channel naming (sender/receiver pair) for introduced links",
            ],
        },
        "dedupe": {
            "duplicate_rows_count": int(len(dupes)),
        },
        "anomalies": {
            "orphan_candidates": orphan_candidates,
        },
        "decision_explanation": [
            "Target channels are introduced only between queue managers that participate in observed producer->consumer demands (secure-by-default).",
            "For each demand-connected component, we pick an acyclic (tree) topology to avoid cycles and keep channel count minimal.",
            "We evaluate star vs maximum-spanning-tree candidates using the same quantitative complexity metric, selecting the lowest-score topology.",
        ],
        "component_decisions": component_decisions,
    }
    return plan


def apply_plan_to_build_target_df(
    df_in: pd.DataFrame,
    columns: List[str],
    cm: ColumnMap,
    plan: Dict,
    *,
    apply_orphan_removals: bool = False,
) -> pd.DataFrame:
    # Keep output dataset aligned with planning-time constraint enforcement.
    df_enforced = auto_fix_ownership_conflicts(df_in, cm)
    df_deduped, _dupes = dedupe_rows(df_enforced, cm)

    # Optionally remove orphan candidates (conservative, human-in-loop friendly).
    if apply_orphan_removals:
        orphan_idxs = {int(o["row_index"]) for o in plan.get("anomalies", {}).get("orphan_candidates", [])}
        if orphan_idxs:
            # df_deduped indices follow the original df indices (from iterrows); remove by index set.
            df_deduped = df_deduped.drop(index=list(orphan_idxs), errors="ignore")

    # Add channels according to plan.
    target_links = [tuple(x) for x in plan["target"]["proposed_channel_links_undirected"]]
    channel_rows = _make_channel_rows(channel_links=target_links, cm=cm, columns=columns)

    df_channels = pd.DataFrame(channel_rows, columns=columns)

    df_out = pd.concat([df_deduped, df_channels], ignore_index=True)

    # Ensure deterministic column ordering.
    df_out = df_out[columns]
    return df_out


def write_plan(plan: Dict, out_path: str) -> None:
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text(json.dumps(plan, indent=2), encoding="utf-8")


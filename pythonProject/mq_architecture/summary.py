"""Summaries and tables for dashboard / reporting."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd

from mq_architecture.discovery import build_demand_pairs
from mq_architecture.schema import ColumnMap


def demand_pairs_table(df, cm: ColumnMap) -> pd.DataFrame:
    """From QM → To QM demand counts (rows in CSV supporting each path)."""
    demand_counts, _ = build_demand_pairs(df, cm)
    if not demand_counts:
        return pd.DataFrame(columns=["From queue manager", "To queue manager", "Rows (weight)"])
    rows = []
    for (src, dst), w in sorted(demand_counts.items()):
        rows.append(
            {
                "From queue manager": src,
                "To queue manager": dst,
                "Rows (weight)": int(w),
            }
        )
    return pd.DataFrame(rows)


def _normalize_edges(link_list: List[List[str]]) -> List[Tuple[str, str]]:
    out = []
    for pair in link_list:
        if len(pair) >= 2:
            a, b = sorted([pair[0], pair[1]])
            out.append((a, b))
    return sorted(set(out))


def plan_verdict(plan: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare as-is vs target undirected QM links and produce plain-English fields.
    """
    as_is = _normalize_edges(plan.get("as_is", {}).get("assumed_channel_links_undirected", []))
    target = _normalize_edges(plan.get("target", {}).get("proposed_channel_links_undirected", []))
    as_set = set(as_is)
    tg_set = set(target)
    removed = sorted(as_set - tg_set)
    added = sorted(tg_set - as_set)
    same = as_set == tg_set

    as_c = plan.get("as_is", {}).get("complexity", {})
    tg_c = plan.get("target", {}).get("complexity", {})
    score_as = float(as_c.get("score", 0))
    score_tg = float(tg_c.get("score", 0))
    delta = score_tg - score_as

    if same and abs(delta) < 1e-9:
        verdict = (
            "The planned target uses the **same** queue-manager links as the as-is baseline. "
            "Scores match; the graphs will look the same. This happens when your demands already "
            "form a minimal tree (no extra link to remove)."
        )
    elif same:
        verdict = (
            "Link sets are the same, but the **complexity score** can still differ slightly if "
            "weights or internal metrics differ. Usually scores match."
        )
    elif removed and not added:
        verdict = (
            f"The target **removes {len(removed)} link(s)** compared to the as-is baseline, "
            "reducing interconnect while keeping a connected plan for your demands."
        )
    elif added and not removed:
        verdict = (
            f"The target **adds {len(added)} link(s)** vs the as-is baseline (unusual; review "
            "demand data and weights)."
        )
    else:
        verdict = (
            f"The target **removes {len(removed)} link(s)** and **adds {len(added)} link(s)** "
            "compared to the as-is baseline."
        )

    return {
        "as_is_edges": [f"{a} — {b}" for a, b in as_is],
        "target_edges": [f"{a} — {b}" for a, b in target],
        "edges_removed": [f"{a} — {b}" for a, b in removed],
        "edges_added": [f"{a} — {b}" for a, b in added],
        "same_topology": same,
        "score_as_is": score_as,
        "score_target": score_tg,
        "score_delta": delta,
        "verdict_markdown": verdict,
    }


def complexity_interpretation_sentence(plan: Dict[str, Any]) -> str:
    """One sentence for the bar chart."""
    v = plan_verdict(plan)
    d = v["score_delta"]
    if abs(d) < 1e-6:
        return "Overall complexity score is **unchanged** between as-is and target for this dataset."
    if d < 0:
        return f"Target complexity score is **lower** by {abs(d):.3f} than as-is (lower is generally simpler)."
    return f"Target complexity score is **higher** by {d:.3f} than as-is (review weights or demand data)."


def violations_tables(plan: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Before / after ownership violation tables."""
    before = plan.get("constraints", {}).get("ownership_violations_before_fix", [])
    after = plan.get("constraints", {}).get("ownership_violations_after_fix", [])

    def _to_df(items: List[Dict]) -> pd.DataFrame:
        if not items:
            return pd.DataFrame(columns=["Application (app id)", "Queue managers seen", "Issue"])
        rows = []
        for it in items:
            rows.append(
                {
                    "Application (app id)": it.get("app_id", ""),
                    "Queue managers seen": ", ".join(it.get("queue_managers_seen", []) or []),
                    "Issue": it.get("violation", ""),
                }
            )
        return pd.DataFrame(rows)

    return _to_df(before), _to_df(after)


def component_decisions_table(plan: Dict[str, Any]) -> pd.DataFrame:
    """Per-component planner decisions in one table."""
    decs = plan.get("component_decisions", [])
    if not decs:
        return pd.DataFrame(
            columns=["Queue managers in group", "Chosen pattern", "Hub (if star)", "Links (undirected)"]
        )
    rows = []
    for d in decs:
        meta = d.get("selected_meta") or {}
        typ = meta.get("type", "")
        hub = meta.get("hub", "")
        nodes = ", ".join(d.get("component_nodes", []))
        links = d.get("selected_links_undirected", [])
        links_s = "; ".join(f"{x[0]}—{x[1]}" for x in links if len(x) >= 2)
        rows.append(
            {
                "Queue managers in group": nodes,
                "Chosen pattern": typ or "—",
                "Hub (if star)": hub or "—",
                "Links (undirected)": links_s or "—",
            }
        )
    return pd.DataFrame(rows)


def executive_summary_paragraph(
    *,
    filename: str,
    row_count: int,
    col_count: int,
    plan: Dict[str, Any],
    df,
    cm: ColumnMap,
) -> str:
    """Short auto-generated paragraph for the dashboard."""
    demand_counts, _ = build_demand_pairs(df, cm)
    n_demands = len(demand_counts)
    v = plan_verdict(plan)
    link_list = plan.get("as_is", {}).get("assumed_channel_links_undirected", [])
    qms = set()
    for pair in link_list:
        if len(pair) >= 2:
            qms.add(pair[0])
            qms.add(pair[1])
    n_qm_as_is = len(qms)

    parts = [
        f"You uploaded **{filename}** ({row_count} rows, {col_count} columns). ",
        f"The tool found **{n_demands}** distinct queue-manager-to-queue-manager message path(s) (from your local QM to remote QM columns). ",
    ]
    if n_qm_as_is:
        parts.append(f"Those paths involve **{n_qm_as_is}** queue manager name(s) in the baseline graph. ")
    parts.append(
        "**Target state** is a planned interconnect (usually a tree) between those queue managers, "
        "plus new channel rows for automation.\n\n"
    )
    parts.append(v["verdict_markdown"])
    return "".join(parts)

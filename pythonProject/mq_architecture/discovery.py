from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd

from .schema import ColumnMap


def _clean_str(x: object) -> str:
    if x is None:
        return ""
    s = str(x)
    return s.strip()


def build_qm_app_ownership(df: pd.DataFrame, cm: ColumnMap) -> Dict[str, Set[str]]:
    """
    Map app_id -> set(queue managers seen).
    """
    if not cm.app_id:
        return {}

    ownership: Dict[str, Set[str]] = defaultdict(set)
    for _, row in df.iterrows():
        app = _clean_str(row.get(cm.app_id, ""))
        qm = _clean_str(row.get(cm.queue_manager_name, ""))
        if app and qm:
            ownership[app].add(qm)
    return ownership


def find_app_violations(ownership: Dict[str, Set[str]]) -> List[Dict]:
    """
    Constraint: exactly one queue manager per application.
    """
    violations: List[Dict] = []
    for app_id, qms in sorted(ownership.items()):
        if len(qms) != 1:
            violations.append(
                {
                    "app_id": app_id,
                    "queue_managers_seen": sorted(list(qms)),
                    "violation": "app spans multiple queue managers",
                }
            )
    return violations


def build_demand_pairs(
    df: pd.DataFrame, cm: ColumnMap
) -> Tuple[Dict[Tuple[str, str], int], Set[str]]:
    """
    Extract demand pairs (producer_qm -> consumer_qm).

    We assume the dataset uses:
    - queue manager name = source/local QM of the producer-side queue
    - remote queue manager name = destination QM
    """
    if not cm.remote_queue_manager_name:
        return {}, set()

    demand_counts: Dict[Tuple[str, str], int] = Counter()
    qms_involved: Set[str] = set()

    for _, row in df.iterrows():
        src = _clean_str(row.get(cm.queue_manager_name, ""))
        dst = _clean_str(row.get(cm.remote_queue_manager_name, ""))
        if not src or not dst or src == dst:
            continue
        demand_counts[(src, dst)] += 1
        qms_involved.add(src)
        qms_involved.add(dst)

    return demand_counts, qms_involved


def build_objects_dedup_key(row: pd.Series, cm: ColumnMap) -> str:
    parts = [
        _clean_str(row.get(cm.queue_manager_name, "")),
        _clean_str(row.get(cm.queue_name, "")),
        _clean_str(row.get(cm.q_type, "")) if cm.q_type else "",
        _clean_str(row.get(cm.remote_queue_manager_name, "")) if cm.remote_queue_manager_name else "",
        _clean_str(row.get(cm.remote_queue_name, "")) if cm.remote_queue_name else "",
        _clean_str(row.get(cm.usage, "")) if cm.usage else "",
    ]
    return "|".join(parts)


def dedupe_rows(df: pd.DataFrame, cm: ColumnMap) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Dedupe exact queue definitions by a best-effort key.
    Returns (df_deduped, duplicates_df).
    """
    keys = df.apply(lambda r: build_objects_dedup_key(r, cm), axis=1)
    df2 = df.copy()
    df2["_dedupe_key"] = keys
    dup_mask = df2.duplicated(subset=["_dedupe_key"], keep="first")
    dupes = df2[dup_mask].drop(columns=["_dedupe_key"])
    deduped = df2[~dup_mask].drop(columns=["_dedupe_key"])
    return deduped, dupes


def find_orphan_candidates(df: pd.DataFrame, cm: ColumnMap) -> List[Dict]:
    """
    Identify likely unused queues based on best-effort signals:
    - Neither producer_name nor consumer_name is set
    - usage is empty

    This is intentionally conservative; human-in-the-loop should validate.
    """
    if not cm.producer_name and not cm.consumer_name and not cm.usage:
        return []

    orphan_rows: List[Dict] = []
    for idx, row in df.iterrows():
        producer = _clean_str(row.get(cm.producer_name, "")) if cm.producer_name else ""
        consumer = _clean_str(row.get(cm.consumer_name, "")) if cm.consumer_name else ""
        usage = _clean_str(row.get(cm.usage, "")) if cm.usage else ""
        if not producer and not consumer and not usage:
            orphan_rows.append(
                {
                    "row_index": int(idx),
                    "queue_manager_name": _clean_str(row.get(cm.queue_manager_name, "")),
                    "queue_name": _clean_str(row.get(cm.queue_name, "")),
                }
            )
    return orphan_rows



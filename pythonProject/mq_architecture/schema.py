from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Optional


def _normalize(col: str) -> str:
    # Normalize for resilient lookup across CSV header variants.
    return " ".join(col.strip().lower().split())


def resolve_column(df, candidates: Iterable[str]) -> Optional[str]:
    """
    Resolve a column in `df` by trying candidate header names with
    case/whitespace-insensitive matching.
    """
    by_norm = {_normalize(c): c for c in df.columns}
    for cand in candidates:
        key = _normalize(cand)
        if key in by_norm:
            return by_norm[key]
    return None


@dataclass(frozen=True)
class ColumnMap:
    # Canonical keys used throughout the transformer.
    queue_name: str
    producer_name: Optional[str]
    consumer_name: Optional[str]
    primary_app_full_name: Optional[str]
    primary_app_disp: Optional[str]
    primary_app_role: Optional[str]
    primary_neighbourhood: Optional[str]
    primary_hosting_type: Optional[str]
    primary_data_classification: Optional[str]
    primary_psi: Optional[str]
    primary_publicly_accessible: Optional[str]
    primary_tract: Optional[str]
    q_type: Optional[str]
    queue_manager_name: str
    app_id: Optional[str]
    lob: Optional[str]
    cluster_name: Optional[str]
    cluster_name_list: Optional[str]
    remote_queue_manager_name: Optional[str]
    remote_queue_name: Optional[str]
    usage: Optional[str]
    emit_q_name: Optional[str]

    @staticmethod
    def from_df(df) -> "ColumnMap":
        # Required columns (best-effort; if missing, we fail early).
        queue_manager_name = resolve_column(
            df,
            [
                "queue manager name",
                "queue_manager_name",
                "queue manager",
                "qmgr",
                "q manager name",
            ],
        )
        if not queue_manager_name:
            raise ValueError(
                "Missing required column: queue manager name (or equivalent)."
            )

        queue_name = resolve_column(
            df,
            [
                "queue name",
                "queue_name",
                "q name",
                "qname",
            ],
        )
        if not queue_name:
            raise ValueError("Missing required column: queue name (or equivalent).")

        # Optional columns.
        def opt(cands: list[str]) -> Optional[str]:
            return resolve_column(df, cands)

        return ColumnMap(
            queue_name=queue_name,
            producer_name=opt(["producer name", "producer_name", "producer"]),
            consumer_name=opt(["consumer name", "consumer_name", "consumer"]),
            primary_app_full_name=opt(
                ["primary app full name", "primary app name", "primary app full"]
            ),
            primary_app_disp=opt(["primary app disp", "primary app display name"]),
            primary_app_role=opt(["primary app role", "app role", "primary role"]),
            primary_neighbourhood=opt(["primary neighbourhood", "primary neighborhood", "neighbourhood"]),
            primary_hosting_type=opt(["primary hosting type", "hosting type"]),
            primary_data_classification=opt(
                ["primary data classification", "data classification", "primary classification"]
            ),
            primary_psi=opt(["primary psi", "psi"]),
            primary_publicly_accessible=opt(
                ["primary publicly accessible", "publicly accessible", "public access"]
            ),
            primary_tract=opt(["primary tract", "tract"]),
            q_type=opt(["q type", "queue type", "q_type"]),
            queue_manager_name=queue_manager_name,
            app_id=opt(["app id", "application id", "primary app id", "app_id"]),
            lob=opt(["lob", "line of business", "primary lob"]),
            cluster_name=opt(["cluster name", "primary cluster name"]),
            cluster_name_list=opt(["cluster name list", "cluster_name_list"]),
            remote_queue_manager_name=opt(
                ["remote queue manager name", "remote queue manager", "remote_qm"]
            ),
            remote_queue_name=opt(["remote queue name", "remote q name", "remote_queue_name"]),
            usage=opt(["usage"]),
            emit_q_name=opt(["emit q name", "emit q", "emit queue name", "emit_q_name"]),
        )


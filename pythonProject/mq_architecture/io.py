from __future__ import annotations

import csv
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd


def sniff_delimiter(path: str) -> Optional[str]:
    # Simple delimiter sniffing from the first non-empty line.
    # Supports: comma, semicolon, tab, pipe.
    p = Path(path)
    raw = p.read_text(encoding="utf-8", errors="ignore").splitlines()
    for line in raw:
        if not line.strip():
            continue
        candidates = [",", ";", "\t", "|"]
        counts = {d: line.count(d) for d in candidates}
        best = max(counts.items(), key=lambda kv: kv[1])
        if best[1] == 0:
            return None
        return best[0]
    return None


def load_csv(path: str) -> Tuple[pd.DataFrame, List[str], Optional[str]]:
    """Load an input CSV while preserving header order.

    Returns (df, column_order, delimiter).
    """
    delim = sniff_delimiter(path)
    if delim:
        df = pd.read_csv(path, sep=delim, dtype=str, keep_default_na=False)
    else:
        # Fall back to pandas auto-detect.
        df = pd.read_csv(path, dtype=str, keep_default_na=False, engine="python")
    # Preserve exact column order.
    cols = list(df.columns)
    return df, cols, delim


def write_csv(df: pd.DataFrame, path: str, delimiter: Optional[str] = ",") -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    # Always write with the same columns order.
    cols = list(df.columns)
    df_out = df[cols]
    df_out.to_csv(path, index=False, sep=delimiter if delimiter else ",", quoting=csv.QUOTE_MINIMAL)


# Backward-compatible aliases (old naming).
def load_cdv_csv(path: str) -> Tuple[pd.DataFrame, List[str], Optional[str]]:
    return load_csv(path)


def write_cdv_csv(df: pd.DataFrame, path: str, delimiter: Optional[str] = ",") -> None:
    write_csv(df, path, delimiter=delimiter)


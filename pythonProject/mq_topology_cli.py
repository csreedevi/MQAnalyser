from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

from mq_architecture.complexity import ComplexityWeights
from mq_architecture.discovery import build_demand_pairs
from mq_architecture.io import load_csv, write_csv
from mq_architecture.schema import ColumnMap
from mq_architecture.transform import (
    apply_plan_to_build_target_df,
    plan_target_topology,
    write_plan,
)
from mq_architecture.viz import plot_channel_graph


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MQ CSV -> target topology transformer")
    p.add_argument("--input", required=True, help="Input CSV file (with headers).")
    p.add_argument("--outdir", required=True, help="Output directory for plan and viz.")
    p.add_argument("--output", default="", help="Write target CSV to this path (optional).")
    p.add_argument("--apply-orphans", action="store_true", help="Remove conservative orphan candidates.")
    p.add_argument("--auto-fix-ownership", action="store_true", help="Deprecated: constraint is enforced automatically in code.")
    p.add_argument("--fail-on-ownership-violations", action="store_true", help="Fail if app spans multiple QMs.")
    p.add_argument("--max-hub-candidates", type=int, default=3, help="Candidate hub QMs per demand component.")
    p.add_argument("--w-channels", type=float, default=1.0)
    p.add_argument("--w-hops", type=float, default=1.5)
    p.add_argument("--w-fan", type=float, default=0.75)
    p.add_argument("--w-cycles", type=float, default=2.0)
    p.add_argument("--w-objects", type=float, default=0.25)
    return p.parse_args()


def _cleanup_known_outputs(outdir: Path, output_path: str) -> None:
    """
    Prevent stale artifacts from previous runs.

    Only removes the known files this CLI writes.
    """
    candidates = [
        outdir / "plan.json",
        outdir / "as_is_channels.png",
        outdir / "target_channels.png",
    ]

    # If output is in the same outdir, also clean it.
    if output_path:
        p = Path(output_path)
        # If user passed a relative path, it is relative to cwd.
        if not p.is_absolute():
            p = Path.cwd() / p
        candidates.append(p)

    for c in candidates:
        try:
            c.unlink(missing_ok=True)
        except Exception:
            # Best-effort cleanup only; never fail the run.
            pass


def main() -> int:
    args = parse_args()

    outdir = Path(args.outdir)
    _cleanup_known_outputs(outdir, args.output)
    outdir.mkdir(parents=True, exist_ok=True)

    df_in, columns, delimiter = load_csv(args.input)
    cm = ColumnMap.from_df(df_in)

    # Ownership constraint is enforced inside plan_target_topology at code-level.
    df = df_in

    weights = ComplexityWeights(
        w_channels=args.w_channels,
        w_hops=args.w_hops,
        w_fan=args.w_fan,
        w_cycles=args.w_cycles,
        w_objects=args.w_objects,
    )

    # Build plan (human-in-loop artefact).
    plan = plan_target_topology(
        df_in=df,
        columns=columns,
        cm=cm,
        weights=weights,
        max_hub_candidates=args.max_hub_candidates,
    )

    ownership_violations_after = plan.get("constraints", {}).get("ownership_violations_after_fix", [])
    if args.fail_on_ownership_violations and ownership_violations_after:
        plan_path = outdir / "plan.json"
        write_plan(plan, str(plan_path))
        print(f"Ownership violations found after enforcement: {len(ownership_violations_after)}. Plan written to {plan_path}")
        return 2

    write_plan(plan, str(outdir / "plan.json"))

    # Demand pairs for visualization overlay.
    demand_counts, _ = build_demand_pairs(df, cm)
    demand_pairs: List[Tuple[str, str, int]] = [(s, d, int(w)) for (s, d), w in demand_counts.items()]

    # Visualize as-is vs target.
    as_is_links = [tuple(x) for x in plan["as_is"]["assumed_channel_links_undirected"]]
    target_links = [tuple(x) for x in plan["target"]["proposed_channel_links_undirected"]]

    plot_channel_graph(
        channel_links=as_is_links,
        out_png=str(outdir / "as_is_channels.png"),
        title="As-Is (assumed) channel topology",
        demand_pairs=demand_pairs,
    )
    plot_channel_graph(
        channel_links=target_links,
        out_png=str(outdir / "target_channels.png"),
        title="Target proposed channel topology",
        demand_pairs=demand_pairs,
    )

    # Optionally build and write the target CSV.
    if args.output:
        df_target = apply_plan_to_build_target_df(
            df_in=df,
            columns=columns,
            cm=cm,
            plan=plan,
            apply_orphan_removals=args.apply_orphans,
        )
        write_csv(df_target, args.output, delimiter=delimiter or ",")

    # Print a concise summary for automation logs.
    as_is_score = plan["as_is"]["complexity"]["score"]
    target_score = plan["target"]["complexity"]["score"]
    print(f"As-Is complexity score: {as_is_score:.3f}")
    print(f"Target complexity score: {target_score:.3f}")
    print(f"Proposed directed channels: {plan['target']['complexity']['directed_channel_count']}")
    if ownership_violations_after:
        print(f"Ownership violations after enforcement: {len(ownership_violations_after)} (see plan.json)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


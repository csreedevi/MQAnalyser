from __future__ import annotations

import json
import shutil
from pathlib import Path

from mq_architecture.complexity import ComplexityWeights
from mq_architecture.io import load_csv, write_csv
from mq_architecture.planner import select_channel_topology
from mq_architecture.schema import ColumnMap
from mq_architecture.transform import apply_plan_to_build_target_df, plan_target_topology
from mq_architecture.discovery import build_demand_pairs
from mq_architecture.viz import plot_channel_graph


def main() -> int:
    repo_root = Path(__file__).resolve().parent
    input_csv = repo_root / "sample_input.csv"
    outdir = repo_root / "sample_out"
    output_csv = repo_root / "sample_output.csv"

    if not input_csv.exists():
        raise FileNotFoundError(f"Missing {input_csv}")

    # Clear stale artifacts so there's only one sample output.
    if outdir.exists():
        shutil.rmtree(outdir)
    if output_csv.exists():
        output_csv.unlink()

    outdir.mkdir(parents=True, exist_ok=True)

    df, columns, delimiter = load_csv(str(input_csv))
    cm = ColumnMap.from_df(df)

    weights = ComplexityWeights()
    plan = plan_target_topology(
        df_in=df,
        columns=columns,
        cm=cm,
        weights=weights,
        max_hub_candidates=3,
    )

    (outdir / "plan.json").write_text(json.dumps(plan, indent=2), encoding="utf-8")

    demand_counts, _ = build_demand_pairs(df, cm)
    demand_pairs = [(s, d, int(w)) for (s, d), w in demand_counts.items()]

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

    df_target = apply_plan_to_build_target_df(
        df_in=df,
        columns=columns,
        cm=cm,
        plan=plan,
        apply_orphan_removals=False,
    )
    write_csv(df_target, str(output_csv), delimiter=delimiter or ",")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


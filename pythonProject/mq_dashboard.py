from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from mq_architecture.complexity import ComplexityWeights
from mq_architecture.discovery import build_demand_pairs
from mq_architecture.summary import (
    complexity_interpretation_sentence,
    component_decisions_table,
    demand_pairs_table,
    executive_summary_paragraph,
    plan_verdict,
    violations_tables,
)
from mq_architecture.io import load_csv, write_csv
from mq_architecture.schema import ColumnMap
from mq_architecture.transform import (
    apply_plan_to_build_target_df,
    plan_target_topology,
)
from mq_architecture.viz import figure_to_png_bytes, plot_channel_graph_figure


def _inject_page_style() -> None:
    st.markdown(
        """
        <style>
            .block-container { padding-top: 1.5rem; max-width: 1200px; }
            div[data-testid="stMetric"] {
                background: linear-gradient(145deg, #ffffff 0%, #f8fafc 100%);
                border: 1px solid #e2e8f0;
                border-radius: 12px;
                padding: 0.75rem 1rem;
                box-shadow: 0 1px 2px rgba(15, 23, 42, 0.06);
            }
            /* Avoid matplotlib + use_container_width creating huge vertical “blank” areas */
            [data-testid="stImage"] img {
                max-width: 720px;
                height: auto;
            }
            .stTabs [data-baseweb="tab-list"] { gap: 8px; }
            .stTabs [data-baseweb="tab"] {
                border-radius: 8px 8px 0 0;
                padding: 0.5rem 1rem;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _dataframe_height_px(num_rows: int, *, cap: int = 320, row_px: int = 36, header_px: int = 44) -> int:
    """Keep small tables compact so Streamlit doesn’t reserve a tall empty grid."""
    if num_rows <= 0:
        return 80
    return min(cap, header_px + max(1, num_rows) * row_px)


def _show_topology_image(
    *,
    channel_links: list,
    title: str,
    demand_pairs: list,
    figsize: tuple[float, float] = (7.0, 4.25),
    image_width: int | None = 720,
    use_column_width: bool = False,
) -> None:
    """PNG + st.image avoids st.pyplot(use_container_width=True) stretching plots to extreme height."""
    fig = plot_channel_graph_figure(
        channel_links=channel_links,
        title=title,
        demand_pairs=demand_pairs,
        figsize=figsize,
    )
    png = figure_to_png_bytes(fig, dpi=110)
    if use_column_width:
        st.image(png, use_container_width=True)
    else:
        st.image(png, width=image_width)


def _edges_comparison_df(plan: dict) -> pd.DataFrame:
    v = plan_verdict(plan)
    all_edges = sorted(set(v["as_is_edges"] + v["target_edges"]))
    rows = []
    for e in all_edges:
        rows.append(
            {
                "QM interconnect (undirected)": e,
                "In as-is baseline": "Yes" if e in v["as_is_edges"] else "No",
                "In target plan": "Yes" if e in v["target_edges"] else "No",
            }
        )
    if not rows:
        return pd.DataFrame(columns=["QM interconnect (undirected)", "In as-is baseline", "In target plan"])
    return pd.DataFrame(rows)


st.set_page_config(
    page_title="MQ Topology Transformer",
    layout="wide",
    initial_sidebar_state="expanded",
)
_inject_page_style()

st.markdown("### MQ Topology Transformer")
st.markdown(
    """
**What this page does**
- Upload a **CSV** that describes queues, queue managers, and where messages go (`remote queue manager name`).
- The tool **enforces** one queue manager per application (`app id`), then proposes a **target** way to connect queue managers with **fewer unnecessary links** where possible.
- You get **plain-language results**, tables, graphs, and an optional **target CSV** for automation.

**Privacy:** Everything runs in your browser session and local Python process—no external APIs or API keys.
"""
)

with st.expander("How to read as-is vs target", expanded=False):
    st.markdown(
        """
- **As-is (baseline):** The tool assumes each distinct *local QM → remote QM* path in your CSV would have a **direct** link between those two queue managers. Your file usually has **no channel rows**; this is a *model* of complexity, not a dump of real channels.
- **Target:** A **planned** interconnect—often a **tree** so there are no cycles—plus **new rows** for sender/receiver channel names you can provision.
- **Graphs:** If as-is and target use the **same** links, the two pictures will look the same.
"""
    )

uploaded = st.file_uploader(
    "Upload your CSV (with headers). It should not already contain channel rows.",
    type=["csv"],
)
if uploaded:
    with tempfile.TemporaryDirectory() as td:
        tmp_in = Path(td) / uploaded.name
        tmp_in.write_bytes(uploaded.getbuffer())

        df, columns, delimiter = load_csv(str(tmp_in))
        cm = ColumnMap.from_df(df)

        with st.sidebar:
            st.markdown("#### Tuning")
            st.markdown(
                "**Weights** change how the **complexity score** is calculated (numbers in the main area). "
                "**Hub candidates** can change which topology wins when several stars are possible."
            )
            st.markdown(
                "The **bar chart** shows raw components (channels, hops, …). They only change if the "
                "planner picks a **different** graph. On small datasets the best topology often stays the same."
            )
            weights = ComplexityWeights(
                w_channels=st.slider(
                    "Weight: channels", 0.0, 5.0, 1.0, step=0.1, key="mq_w_channels"
                ),
                w_hops=st.slider(
                    "Weight: routing hops", 0.0, 5.0, 1.5, step=0.1, key="mq_w_hops"
                ),
                w_fan=st.slider("Weight: fan-out", 0.0, 5.0, 0.75, step=0.1, key="mq_w_fan"),
                w_cycles=st.slider("Weight: cycles", 0.0, 10.0, 2.0, step=0.1, key="mq_w_cyc"),
                w_objects=st.slider(
                    "Weight: objects", 0.0, 5.0, 0.25, step=0.1, key="mq_w_obj"
                ),
            )
            max_hub = st.slider(
                "Hub candidates (per component)",
                1,
                5,
                3,
                step=1,
                key="mq_max_hub",
                help="How many hub queue managers to try when evaluating star topologies.",
            )

        plan = plan_target_topology(
            df_in=df,
            columns=columns,
            cm=cm,
            weights=weights,
            max_hub_candidates=max_hub,
        )

        as_c = plan["as_is"]["complexity"]
        tg_c = plan["target"]["complexity"]
        pv = plan_verdict(plan)

        st.subheader("Summary")
        st.markdown(
            executive_summary_paragraph(
                filename=uploaded.name,
                row_count=len(df),
                col_count=len(columns),
                plan=plan,
                df=df,
                cm=cm,
            )
        )

        st.subheader("Message paths found in your CSV")
        st.caption(
            "Each row is one distinct path: messages leave the *From* queue manager toward the *To* "
            "queue manager (from `queue manager name` and `remote queue manager name`)."
        )
        _dmd = demand_pairs_table(df, cm)
        st.dataframe(
            _dmd,
            use_container_width=True,
            hide_index=True,
            height=_dataframe_height_px(len(_dmd)),
        )

        st.subheader("As-is vs target interconnect")
        # Use markdown, not caption: st.caption poorly renders **bold** / line breaks and can look like blank space.
        st.markdown(pv["verdict_markdown"])
        edges_df = _edges_comparison_df(plan)
        if edges_df.empty:
            st.caption("No QM-to-QM links to compare (no remote queue manager paths in the file).")
        else:
            st.dataframe(
                edges_df,
                use_container_width=True,
                hide_index=True,
                height=_dataframe_height_px(len(edges_df)),
            )

        demand_counts, _ = build_demand_pairs(df, cm)
        demand_pairs = [(s, d, int(w)) for (s, d), w in demand_counts.items()]
        as_is_links = [tuple(x) for x in plan["as_is"]["assumed_channel_links_undirected"]]
        target_links = [tuple(x) for x in plan["target"]["proposed_channel_links_undirected"]]

        st.markdown("**Topology (as-is vs target)**")
        col_as, col_tg = st.columns(2, gap="small")
        with col_as:
            _show_topology_image(
                channel_links=as_is_links,
                title="As-is: direct link per demand pair",
                demand_pairs=demand_pairs,
                figsize=(5.0, 3.35),
                use_column_width=True,
            )
        with col_tg:
            _show_topology_image(
                channel_links=target_links,
                title="Target: planned interconnect",
                demand_pairs=demand_pairs,
                figsize=(5.0, 3.35),
                use_column_width=True,
            )
        st.caption(
            "Red edges highlight demand pairs with a direct link on that graph. "
            "If both use the same links, the two pictures match."
        )

        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("As-is complexity score", f"{as_c['score']:.3f}")
        with m2:
            delta = tg_c["score"] - as_c["score"]
            st.metric("Target complexity score", f"{tg_c['score']:.3f}", delta=f"{delta:+.3f}")
        with m3:
            st.metric(
                "Directed channel objects (target)",
                int(tg_c["directed_channel_count"]),
                help="Each undirected QM pair implies a sender + receiver channel row in the target CSV.",
            )

        w_used = as_c.get("weights", {})
        st.markdown(
            f"**Scores use sidebar weights:** channels={w_used.get('w_channels')}, "
            f"hops={w_used.get('w_hops')}, fan={w_used.get('w_fan')}, "
            f"cycles={w_used.get('w_cycles')}, objects={w_used.get('w_objects')} · "
            f"hub candidates={max_hub}"
        )

        st.subheader("Constraints and checks")
        v_before, v_after = violations_tables(plan)
        st.markdown("**Application ↔ queue manager (before enforcement)**")
        if v_before.empty:
            st.success("No issue: each application id was already tied to a single queue manager.")
        else:
            st.warning("These application ids appeared on more than one queue manager in the file.")
            st.dataframe(
                v_before,
                use_container_width=True,
                hide_index=True,
                height=_dataframe_height_px(len(v_before)),
            )
        st.markdown("**After enforcement**")
        if v_after.empty:
            st.success("All rows were aligned to one queue manager per application id.")
        else:
            st.error("Some violations remain—see table.")
            st.dataframe(
                v_after,
                use_container_width=True,
                hide_index=True,
                height=_dataframe_height_px(len(v_after)),
            )

        orphan_candidates = plan["anomalies"]["orphan_candidates"]
        if orphan_candidates:
            st.info(
                f"**Orphan queue candidates:** {len(orphan_candidates)} row(s) flagged as possibly "
                "unused (conservative heuristic). Review before deleting in production."
            )
        else:
            st.caption("No orphan queue candidates flagged.")

        st.subheader("Complexity comparison")
        st.markdown(complexity_interpretation_sentence(plan))
        st.info(
            "**Why tuning might not change the bar chart:** That chart shows **raw** ingredients "
            "(e.g. channel count, hops). **Weights** only change how those ingredients combine into "
            "the **scores** above. **Graphs and link table** change only when the planner selects a "
            "different set of QM-to-QM links."
        )
        chart_df = pd.DataFrame(
            {
                "As-is": [
                    as_c["directed_channel_count"],
                    as_c["weighted_hops_total"],
                    as_c["max_fan_out"],
                    as_c["cycles"],
                    as_c["object_count"],
                ],
                "Target": [
                    tg_c["directed_channel_count"],
                    tg_c["weighted_hops_total"],
                    tg_c["max_fan_out"],
                    tg_c["cycles"],
                    tg_c["object_count"],
                ],
            },
            index=[
                "Directed channels (count)",
                "Weighted routing hops",
                "Max fan-out",
                "Cycles (undirected basis)",
                "Object count (rows)",
            ],
        )
        st.bar_chart(chart_df, height=280)
        with st.expander("Raw complexity numbers (JSON)", expanded=False):
            st.json({"as_is": as_c, "target": tg_c})

        st.subheader("Planner decisions (per group of queue managers)")
        _cd = component_decisions_table(plan)
        st.dataframe(
            _cd,
            use_container_width=True,
            hide_index=True,
            height=_dataframe_height_px(len(_cd), cap=280),
        )

        with st.expander("Full plan JSON (for automation / debugging)", expanded=False):
            st.json(plan)

        st.subheader("Download target CSV")
        apply_orphans = st.checkbox("Apply orphan removals when building file (conservative)", value=False)
        if st.button("Generate target.csv"):
            df_target = apply_plan_to_build_target_df(
                df_in=df,
                columns=columns,
                cm=cm,
                plan=plan,
                apply_orphan_removals=apply_orphans,
            )
            out_csv = Path(td) / "target.csv"
            write_csv(df_target, str(out_csv), delimiter=delimiter or ",")
            st.download_button(
                label="Download target.csv",
                data=out_csv.read_bytes(),
                file_name="target.csv",
                mime="text/csv",
            )

else:
    st.info("Upload a CSV above to see the summary, tables, complexity chart, and topology graphs.")

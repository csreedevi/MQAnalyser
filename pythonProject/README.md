# MQ Topology Transformer (CSV -> Target State)

This repo turns an input CSV describing your **current MQ object topology + producer/consumer relationships** into a **planned target topology** and an automation-ready **target CSV**.

It is “agent-style” in the sense that the work is split into clear, explainable components (sometimes implemented as functions/modules rather than separate OS processes). The key design idea is:

1. Discover what exists (and what demands exist) from the CSV.
2. Enforce your constraints.
3. Propose a deterministic, lower-complexity channel topology.
4. Produce a transparent `plan.json` (human-in-the-loop).
5. Generate the target dataset (automation-ready) and local visualizations.

## Architecture: the “agents” and what each does

### 1) Ingestion agent (CSV loader + schema mapping)
Module: `mq_architecture/io.py`, `mq_architecture/schema.py`

Responsibilities:
- Load the input CSV with delimiter sniffing (`load_csv`)
- Preserve the exact input header order so the output can keep the same CSV column structure (`columns` list)
- Resolve header variations case/whitespace-insensitively into a canonical `ColumnMap` (required fields like `queue manager name` and `queue name`, optional fields like `remote queue manager name`, `usage`, etc.)

Key I/O:
- Input: `--input <file>`
- Output to pipeline: `(df, columns, delimiter)`

### 2) Discovery agent (extract QMs + producer/consumer demand graph)
Module: `mq_architecture/discovery.py`

Responsibilities:
- Build “demand pairs” from the dataset using:
  - `queue manager name` as the producer/local QM
  - `remote queue manager name` as the destination/remote QM
- Build QM ownership for the constraint:
  - maps `app id -> set(queue managers seen)`
- Detect conservative anomaly candidates (currently “likely unused queues” based on lack of producer/consumer signals and usage)
- Provide deduplication keys so exact duplicates are removed before planning

Key I/O:
- Input: `df` and `ColumnMap`
- Output:
  - `demand_counts: (src_qm, dst_qm) -> weight`
  - `ownership violations` candidates (before enforcement)

### 3) Constraint enforcement agent (code-level enforcement)
Module: `mq_architecture/transform.py` (function `auto_fix_ownership_conflicts`)

Your constraint:
- Exactly one queue manager per application (based on `app id`)

What this agent does:
- Takes any dataset where an `app id` appears under multiple QMs
- Reassigns all rows of that `app id` to exactly one queue manager using a deterministic majority rule:
  - pick the QM with the most occurrences for that `app id`
  - deterministic tie-break: lexicographic

Important behavior:
- This enforcement runs inside the planning pipeline (not a runtime flag).
- The resulting `plan.json` includes both:
  - `constraints.ownership_violations_before_fix`
  - `constraints.ownership_violations_after_fix` (expected to be empty)

### 4) Planning agent (choose deterministic channel link topology)
Module: `mq_architecture/planner.py` (function `select_channel_topology`)

Responsibilities:
- Convert demand into “component-wise” topology decisions:
  - build connected components over observed QM demand pairs
  - for each component, consider candidate link topologies
- Ensure the proposed links are acyclic at the QM connectivity layer (tree/forest)
- Choose between candidates using the same quantitative complexity metric that the reporting uses

Candidate styles:
- Star tree around one of the best hub candidates
- Maximum spanning tree over weighted candidate edges

Explainability:
- `plan.json` includes `component_decisions` such as:
  - selected topology type (`star` or `mst`)
  - selected hub (for star)
  - selected undirected links for that component

### 5) Complexity evaluation agent (quantitative as-is vs target metric)
Module: `mq_architecture/complexity.py`

Metric inputs:
- `demand_counts` derived from your dataset
- `channel_links` proposed by the planner (target) or inferred (as-is)

What “as-is” means in this repo:
- Since your input CSV is “no channels present”, the tool currently models as-is complexity by assuming a direct link exists between every observed demand pair (QM->QM). This provides a baseline to compare against the reduced target topology.

What the score includes:
- Directed channel count: `2 * (#undirected links)` because each introduced link implies sender/receiver channel objects
- Weighted routing hops: shortest path lengths in the proposed channel connectivity graph
- Fan-out penalty: max outgoing degree from any QM in the channel graph
- Cycles penalty: computed using an undirected cycle basis so tree-like topologies score 0 cycles
- Object count approximation: uses row count as a proxy

Outputs:
- A `complexity` object placed under:
  - `plan.json.as_is.complexity`
  - `plan.json.target.complexity`

### 6) Transformation agent (build the target CSV)
Module: `mq_architecture/transform.py` (functions `apply_plan_to_build_target_df`, `apply_plan_*`)

Responsibilities:
- Enforce the same ownership constraint again before writing output (so target dataset matches `plan.json`)
- Deduplicate the final set of input rows
- Introduce deterministic channel “objects” as new rows

Channel row mapping (best-effort, deterministic):
- For each introduced undirected link (A,B), create:
  - sender row: `A.to.B`
  - receiver row: `B.from.A`
- Deterministic naming pairs:
  - `fromQM.to.toQM` and `toQM.from.fromQM`
- The tool sets:
  - `q type = CHANNEL` (best-effort placeholder; adjust if your admin expects a different literal)
  - `usage = CHANNEL_SENDER` / `CHANNEL_RECEIVER` (if the `usage` column exists in the CSV)

Key I/O:
- Input: your `df` + `plan`
- Output: `target.csv` (same columns as input, plus added channel rows)

### 7) Visualization agent (local topology graphs, no external API keys)
Module: `mq_architecture/viz.py`

What it renders:
- As-is vs target QM channel connectivity graphs to PNG
- Uses `networkx` + `matplotlib` with a writable `MPLCONFIGDIR` and `Agg` backend for stable headless rendering in this environment

Outputs in `--outdir`:
- `as_is_channels.png`
- `target_channels.png`

### 8) Human-in-the-loop UI agent (optional interactive dashboard)
Module: `mq_dashboard.py`, with helpers in `mq_architecture/summary.py`

What the dashboard does:
- Upload a CSV
- Runs the same planning pipeline locally (no external APIs)
- **Readable first:** intent text, auto-generated **summary paragraph**, **demand path table**, **as-is vs target link comparison** table, constraint tables (not JSON-first), then metrics and charts
- **UI / graphics (all local Python):**
  - `streamlit` layout, metrics, and light CSS styling
  - **Bar chart** comparing as-is vs target complexity components (`st.bar_chart`)
  - **Tabs** with styled **matplotlib + networkx** topology graphs (`st.pyplot`)
- **Sidebar tuning:** complexity weight sliders and hub candidates (scores update with weights; bar chart reflects raw components and may stay unchanged if topology does not change)
- **Advanced** expander: full `plan` JSON for debugging
- Download of `target.csv`

## Run instructions

### CLI (automation-friendly)
Run from the repo root (`/`):

```bash
.venv/bin/python mq_topology_cli.py \
  --input /path/to/input.csv \
  --outdir /path/to/results \
  --output /path/to/results/target.csv
```

Note:
- `--auto-fix-ownership` is deprecated/ignored because enforcement happens at code level. The enforcement results are captured in `plan.json` regardless of whether you pass the flag.

Stale output handling:
- The CLI deletes known outputs in `--outdir` (`plan.json` and the two `.png` graphs, and the exact `--output` file if it is inside the same path) before generating fresh results, so you won’t accidentally read old artifacts after a partial run.

### Streamlit UI (optional, richer graphics)
```bash
.venv/bin/streamlit run mq_dashboard.py
```

Open the local URL Streamlit prints in your browser. Graphs use matplotlib only (no Plotly/API keys required).

### Understanding the dashboard

- **Purpose:** The page explains what you uploaded, what message paths were inferred, and how the **target** interconnect differs from the **as-is baseline** (if at all).
- **As-is:** A *model*: one direct QM–QM link per distinct path in your CSV. Your file usually has no channel rows; this is not a copy of production channels.
- **Target:** A *plan*: a tree-like interconnect where possible, plus channel rows for provisioning. If baseline and target use the **same** links, the graphs match; **complexity scores** still reflect your sidebar weights.
- **Why graphs can match:** When demands already form a minimal tree (e.g. only a star), the planner has no extra edge to remove.

## Test with dummy “constraint-violating” data

This repo includes a small `sample_input.csv`.

1. `sample_input.csv` is intentionally edited to create an ownership violation (same `app id` appearing under multiple QMs).
2. Run the fixed sample workflow (one input and one output):
   ```bash
   .venv/bin/python run_sample.py
   ```
3. Validate:
   - `sample_out/plan.json` should show:
     - `constraints.ownership_violations_before_fix` not empty
     - `constraints.ownership_violations_after_fix` empty
   - `sample_output.csv` should reflect the enforced ownership (so the target dataset aligns with `plan.json`).

## Output contract

In every `--outdir`, the tool writes:
- `plan.json` (human validation artifact)
- `as_is_channels.png` and `target_channels.png`
- `target.csv` (only if you pass `--output`)

The fixed sample runner writes:
- `sample_out/plan.json`
- `sample_out/as_is_channels.png` and `sample_out/target_channels.png`
- `sample_output.csv`

If you share your real CSV header + a few rows, I can adjust the channel-row mapping literals (`q type`, `usage`) to perfectly match your column conventions so automation applies the objects without manual tweaking.


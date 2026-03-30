from __future__ import annotations

import io
import os
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import networkx as nx

# Ensure Matplotlib uses a writable cache/config directory in this environment.
_mpl_dir = Path.cwd() / ".mplconfig"
_mpl_dir.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(_mpl_dir))
os.environ.setdefault("MPLBACKEND", "Agg")

import matplotlib.pyplot as plt
from matplotlib.figure import Figure


def _apply_topology_style() -> None:
    """Polished defaults (no external APIs)."""
    try:
        plt.style.use("seaborn-v0_8-whitegrid")
    except OSError:
        try:
            plt.style.use("ggplot")
        except OSError:
            pass
    plt.rcParams.update(
        {
            "figure.facecolor": "#f8fafc",
            "axes.facecolor": "#ffffff",
            "axes.edgecolor": "#e2e8f0",
            "axes.labelcolor": "#1e293b",
            "text.color": "#0f172a",
            "font.size": 10,
            "axes.titlesize": 12,
            "axes.titleweight": "600",
        }
    )


def _layout_positions(G: nx.Graph) -> dict:
    if G.number_of_nodes() == 0:
        return {}
    if G.number_of_nodes() <= 20:
        try:
            return nx.kamada_kawai_layout(G)
        except Exception:
            return nx.spring_layout(G, seed=7, k=2.0)
    return nx.spring_layout(G, seed=7, k=2.0)


def plot_channel_graph_figure(
    *,
    channel_links: Iterable[Tuple[str, str]],
    title: str,
    demand_pairs: Optional[List[Tuple[str, str, int]]] = None,
    figsize: Tuple[float, float] = (11, 7),
):
    """
    Build a styled topology figure for Streamlit / in-memory display (no file I/O).
    """
    _apply_topology_style()
    links = sorted(set(tuple(sorted(e)) for e in channel_links))
    G = nx.Graph()
    G.add_edges_from(links)

    fig, ax = plt.subplots(figsize=figsize, facecolor="#f8fafc")
    ax.set_facecolor("#ffffff")

    if G.number_of_nodes() == 0:
        ax.text(0.5, 0.5, "No channel links to display", ha="center", va="center", fontsize=12)
        ax.set_axis_off()
        fig.suptitle(title, fontsize=13, fontweight="600", color="#0f172a")
        return fig

    pos = _layout_positions(G)
    node_color = "#38bdf8"
    edge_color = "#64748b"
    demand_edge = "#f43f5e"

    nx.draw_networkx_nodes(
        G,
        pos,
        ax=ax,
        node_size=2200,
        node_color=node_color,
        edgecolors="#0f172a",
        linewidths=1.2,
        alpha=0.95,
    )
    nx.draw_networkx_labels(G, pos, ax=ax, font_size=9, font_weight="500", font_color="#0f172a")

    nx.draw_networkx_edges(G, pos, ax=ax, width=2.0, edge_color=edge_color, alpha=0.85)

    if demand_pairs:
        for src, dst, _w in demand_pairs:
            if G.has_edge(*sorted([src, dst])):
                nx.draw_networkx_edges(
                    G,
                    pos,
                    ax=ax,
                    edgelist=[tuple(sorted([src, dst]))],
                    width=3.5,
                    edge_color=demand_edge,
                    alpha=0.75,
                )

    ax.set_axis_off()
    ax.set_title(title, pad=12, color="#0f172a")
    fig.tight_layout()
    return fig


def figure_to_png_bytes(fig: Figure, *, dpi: int = 120) -> bytes:
    """Encode figure to PNG and close it (avoids huge stretched pyplot in Streamlit)."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def plot_channel_graph(
    *,
    channel_links: Iterable[Tuple[str, str]],
    out_png: str,
    title: str,
    demand_pairs: Optional[List[Tuple[str, str, int]]] = None,
    figsize: Tuple[int, int] = (12, 8),
) -> None:
    fig = plot_channel_graph_figure(
        channel_links=channel_links,
        title=title,
        demand_pairs=demand_pairs,
        figsize=(float(figsize[0]), float(figsize[1])),
    )
    Path(out_png).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=200, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)

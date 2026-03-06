from __future__ import annotations

from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd

from pipeline.aggregation import aggregate_flows
from pipeline.metrics import (
    compute_import_shares,
    compute_export_shares,
    compute_import_concentration,
    compute_export_concentration,
    compute_bilateral_asymmetry,
    compute_centrality,
    compute_replaceability,
    compute_composite_indices,
)
from pipeline.coercion import compute_coercion_scores


# --- CONFIG ---
PROJECT = Path(__file__).resolve().parent

YEAR = 2024
SECTOR = "machinery_hightech"  # e.g. "energy", "machinery_hightech", ...
FOCUS_COUNTRY_ISO3: Optional[str] = "IND"  # if set, only show edges touching this country
MIN_COERCION_SCORE = 0.2
MAX_EDGES = 50


def build_coercion_graph_for_sector(
    year: int,
    sector_code: str,
    focus_iso3: Optional[str] = None,
    min_score: float = 0.2,
    max_edges: int = 50,
) -> pd.DataFrame:
    """
    Run the full pipeline for a single (year, sector) and compute directed
    coercion scores. Returns a DataFrame of edges and also writes a simple
    SVG network visualization to disk.
    """
    # 1) Aggregate flows and compute all metrics
    flows_hs2, flows_sector = aggregate_flows()

    import_shares = compute_import_shares(flows_sector)
    export_shares = compute_export_shares(flows_sector)

    imports_conc = compute_import_concentration(import_shares)
    exports_conc = compute_export_concentration(export_shares)
    bilateral = compute_bilateral_asymmetry(import_shares)
    centrality = compute_centrality(import_shares)
    replaceability = compute_replaceability(flows_sector)

    country_metrics, pairwise_metrics = compute_composite_indices(
        imports_conc,
        exports_conc,
        centrality,
        replaceability,
        bilateral,
    )

    # 2) Compute coercion scores
    coercion_edges = compute_coercion_scores(
        country_metrics,
        pairwise_metrics,
        year=year,
        sector_code=sector_code,
    )

    if coercion_edges.empty:
        raise RuntimeError("No coercion edges found for the given year/sector.")

    # 3) Filter by focus country (if any) and score threshold
    df = coercion_edges.copy()
    if focus_iso3 is not None:
        df = df[(df["source_iso3"] == focus_iso3) | (df["target_iso3"] == focus_iso3)]

    df = df[df["coercion_score"] >= min_score]
    df = df.sort_values("coercion_score", ascending=False).head(max_edges).reset_index(drop=True)

    # 4) Write edges to CSV
    out_csv = PROJECT / f"coercion_edges_{sector_code}_{year}.csv"
    df.to_csv(out_csv, index=False)
    print(f"Wrote coercion edges to {out_csv}")

    # 5) Build and draw directed graph
    G = nx.DiGraph()
    for _, row in df.iterrows():
        s = row["source_iso3"]
        t = row["target_iso3"]
        score = float(row["coercion_score"])
        G.add_edge(s, t, weight=score)

    if not G:
        raise RuntimeError("Graph is empty after filtering; relax filters or min_score.")

    # Layout: Kamada-Kawai tends to produce a more evenly spaced, readable graph.
    pos = nx.kamada_kawai_layout(G, weight="weight")

    # Compute node strengths to scale node size and color.
    out_strength = dict(G.out_degree(weight="weight"))
    in_strength = dict(G.in_degree(weight="weight"))
    total_strength = {n: out_strength.get(n, 0.0) + in_strength.get(n, 0.0) for n in G.nodes()}

    max_total = max(total_strength.values()) if total_strength else 1.0
    max_out = max(out_strength.values()) if out_strength else 1.0

    node_sizes = [
        300.0 + 1400.0 * (total_strength[n] / max_total if max_total > 0 else 0.0)
        for n in G.nodes()
    ]
    node_colors = [
        (out_strength.get(n, 0.0) / max_out if max_out > 0 else 0.0) for n in G.nodes()
    ]

    # Edge visual encoding: width and color scale with coercion score.
    weights = [G[u][v]["weight"] for u, v in G.edges()]
    max_w = max(weights) if weights else 1.0
    widths = [0.8 + 3.5 * ((w / max_w) ** 0.5) for w in weights]

    plt.figure(figsize=(12, 9))

    # Draw nodes: size ~ total strength, color ~ outgoing coercion
    nodes = nx.draw_networkx_nodes(
        G,
        pos,
        node_size=node_sizes,
        node_color=node_colors,
        cmap=plt.cm.OrRd,
    )
    plt.colorbar(nodes, label="Relative outgoing coercion")

    # Draw directed edges with widths and colors based on coercion score
    nx.draw_networkx_edges(
        G,
        pos,
        arrowstyle="->",
        arrowsize=10,
        width=widths,
        edge_color=weights,
        edge_cmap=plt.cm.Blues,
        edge_vmin=0.0,
        edge_vmax=max_w,
        alpha=0.8,
    )

    # Draw labels slightly larger for clarity
    nx.draw_networkx_labels(G, pos, font_size=9, font_color="black")

    plt.axis("off")
    title_focus = f" (focus {focus_iso3})" if focus_iso3 else ""
    plt.title(
        f"Coercion exposure network – sector={sector_code}, year={year}{title_focus}\n"
        "Node size = total coercion activity, node color = outgoing coercion, edge width/color = S→T coercion score"
    )

    out_svg = PROJECT / f"coercion_network_{sector_code}_{year}.svg"
    plt.tight_layout()
    plt.savefig(out_svg, format="svg")
    print(f"Wrote coercion network SVG to {out_svg}")

    return df


if __name__ == "__main__":
    edges = build_coercion_graph_for_sector(
        year=YEAR,
        sector_code=SECTOR,
        focus_iso3=FOCUS_COUNTRY_ISO3,
        min_score=MIN_COERCION_SCORE,
        max_edges=MAX_EDGES,
    )
    print(edges.head())

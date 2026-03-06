"""
Metric computation for trade dependency and leverage.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

import networkx as nx
import numpy as np
import pandas as pd


@dataclass
class ConcentrationMetrics:
    hhi: float
    top1_share: float
    top3_share: float


def _concentration_from_shares(shares: np.ndarray) -> ConcentrationMetrics:
    """
    Compute HHI and top-k shares from a 1D array of shares.
    """
    if shares.size == 0:
        return ConcentrationMetrics(hhi=0.0, top1_share=0.0, top3_share=0.0)

    shares_sorted = np.sort(shares)[::-1]
    hhi = float(np.sum(shares_sorted**2))
    top1 = float(shares_sorted[0])
    top3 = float(np.sum(shares_sorted[:3])) if shares_sorted.size >= 3 else float(np.sum(shares_sorted))
    return ConcentrationMetrics(hhi=hhi, top1_share=top1, top3_share=top3)


def compute_import_shares(flows_sector: pd.DataFrame) -> pd.DataFrame:
    """
    From sector-level flows, compute import shares for each importer-sector-year.
    """
    df = flows_sector.copy()

    totals = (
        df.groupby(["year", "importer_iso3", "sector_code"], as_index=False)["value_kusd"]
        .sum()
        .rename(columns={"value_kusd": "total_imports_kusd"})
    )

    merged = df.merge(
        totals, on=["year", "importer_iso3", "sector_code"], how="left", validate="many_to_one"
    )
    merged["import_share"] = merged["value_kusd"] / merged["total_imports_kusd"].replace(0, np.nan)
    return merged


def compute_export_shares(flows_sector: pd.DataFrame) -> pd.DataFrame:
    """
    From sector-level flows, compute export shares for each exporter-sector-year.
    """
    df = flows_sector.copy()

    totals = (
        df.groupby(["year", "exporter_iso3", "sector_code"], as_index=False)["value_kusd"]
        .sum()
        .rename(columns={"value_kusd": "total_exports_kusd"})
    )

    merged = df.merge(
        totals, on=["year", "exporter_iso3", "sector_code"], how="left", validate="many_to_one"
    )
    merged["export_share"] = merged["value_kusd"] / merged["total_exports_kusd"].replace(0, np.nan)
    return merged


def compute_import_concentration(import_shares: pd.DataFrame) -> pd.DataFrame:
    """
    Compute HHI and top shares for import dependency, per importer-year-sector.
    """
    records = []
    group_cols = ["year", "importer_iso3", "sector_code"]
    for key, group in import_shares.groupby(group_cols):
        shares = group["import_share"].to_numpy(dtype=float)
        metrics = _concentration_from_shares(shares)
        records.append(
            {
                "year": key[0],
                "country_iso3": key[1],
                "sector_code": key[2],
                "import_hhi": metrics.hhi,
                "top1_import_share": metrics.top1_share,
                "top3_import_share": metrics.top3_share,
            }
        )
    return pd.DataFrame.from_records(records)


def compute_export_concentration(export_shares: pd.DataFrame) -> pd.DataFrame:
    """
    Compute HHI and top shares for export markets, per exporter-year-sector.
    """
    records = []
    group_cols = ["year", "exporter_iso3", "sector_code"]
    for key, group in export_shares.groupby(group_cols):
        shares = group["export_share"].to_numpy(dtype=float)
        metrics = _concentration_from_shares(shares)
        records.append(
            {
                "year": key[0],
                "country_iso3": key[1],
                "sector_code": key[2],
                "export_hhi": metrics.hhi,
                "top1_export_share": metrics.top1_share,
                "top3_export_share": metrics.top3_share,
            }
        )
    return pd.DataFrame.from_records(records)


def compute_bilateral_asymmetry(import_shares: pd.DataFrame) -> pd.DataFrame:
    """
    Compute bilateral dependency and asymmetry metrics from import shares.

    dep_i_on_j: importer i's dependence on exporter j in a sector-year.
    """
    df = import_shares.copy()
    df = df.rename(columns={"importer_iso3": "importer", "exporter_iso3": "exporter"})

    # For each (year, sector, importer, exporter) we already have import_share.
    left = df[["year", "sector_code", "importer", "exporter", "import_share"]].rename(
        columns={"importer": "country_a", "exporter": "country_b", "import_share": "dep_a_on_b"}
    )

    right = df[["year", "sector_code", "importer", "exporter", "import_share"]].rename(
        columns={"importer": "country_b", "exporter": "country_a", "import_share": "dep_b_on_a"}
    )

    merged = left.merge(
        right,
        on=["year", "sector_code", "country_a", "country_b"],
        how="outer",
    ).fillna(0.0)

    # Asymmetry as signed difference and log-ratio (with small epsilon)
    eps = 1e-9
    merged["asymmetry_diff"] = merged["dep_a_on_b"] - merged["dep_b_on_a"]
    merged["asymmetry_log_ratio"] = np.log(
        (merged["dep_a_on_b"] + eps) / (merged["dep_b_on_a"] + eps)
    )

    return merged


def compute_centrality(import_shares: pd.DataFrame) -> pd.DataFrame:
    """
    Compute a simple eigenvector-based centrality measure per year, sector, and country.

    We build a directed graph where edges go exporter -> importer,
    weighted by import_share.
    """
    records = []

    grouped = import_shares.groupby(["year", "sector_code"])
    for (year, sector), group in grouped:
        G = nx.DiGraph()
        for _, row in group.iterrows():
            exporter = row["exporter_iso3"]
            importer = row["importer_iso3"]
            weight = float(row["import_share"])
            if np.isnan(weight) or weight <= 0:
                continue
            if exporter not in G:
                G.add_node(exporter)
            if importer not in G:
                G.add_node(importer)
            G.add_edge(exporter, importer, weight=weight)

        if len(G) == 0:
            continue

        try:
            centrality = nx.eigenvector_centrality_numpy(G, weight="weight")
        except Exception:
            # Fallback to degree centrality if eigenvector fails
            centrality = nx.degree_centrality(G)

        for country, score in centrality.items():
            records.append(
                {
                    "year": year,
                    "country_iso3": country,
                    "sector_code": sector,
                    "centrality_score": float(score),
                }
            )

    return pd.DataFrame.from_records(records)


def compute_replaceability(
    flows_sector: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute a simple replaceability proxy for each importer-year-sector.

    For a given importer and sector, we:
      - identify the top supplier by value,
      - compute the volume imported from that supplier,
      - compare with total exports from all *other* exporters globally in that sector,
      - map the ratio to [0, 1] where higher means easier to replace.
    """
    df = flows_sector.copy()

    # Total exports per exporter-sector-year (global)
    global_exports = (
        df.groupby(["year", "exporter_iso3", "sector_code"], as_index=False)["value_kusd"]
        .sum()
        .rename(columns={"value_kusd": "global_exports_kusd"})
    )

    # Identify top supplier per importer-year-sector
    grouped = df.groupby(["year", "importer_iso3", "sector_code"])

    records = []
    for (year, importer, sector), group in grouped:
        if group.empty:
            continue

        top_row = group.loc[group["value_kusd"].idxmax()]
        top_supplier = top_row["exporter_iso3"]
        top_volume = float(top_row["value_kusd"])

        # Global exports from other suppliers in this sector-year
        sector_exports = global_exports[
            (global_exports["year"] == year) & (global_exports["sector_code"] == sector)
        ]
        others = sector_exports[sector_exports["exporter_iso3"] != top_supplier]
        total_exports_others = float(others["global_exports_kusd"].sum())

        if top_volume <= 0.0:
            ratio = 0.0
        else:
            ratio = total_exports_others / top_volume

        # Simple mapping: ratio >= 2 → ~1, ratio == 0 → 0
        replaceability_score = max(0.0, min(1.0, ratio / 2.0))

        records.append(
            {
                "year": year,
                "country_iso3": importer,
                "sector_code": sector,
                "replaceability_score": replaceability_score,
                "top_supplier_iso3": top_supplier,
            }
        )

    return pd.DataFrame.from_records(records)


def compute_composite_indices(
    imports_conc: pd.DataFrame,
    exports_conc: pd.DataFrame,
    centrality: pd.DataFrame,
    replaceability: pd.DataFrame,
    bilateral: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute composite exposure and leverage indices.

    Returns:
      country_sector_metrics, pairwise_metrics
    """
    # Merge country-level components
    metrics = imports_conc.merge(
        exports_conc,
        on=["year", "country_iso3", "sector_code"],
        how="outer",
    ).merge(
        centrality,
        on=["year", "country_iso3", "sector_code"],
        how="left",
    ).merge(
        replaceability,
        on=["year", "country_iso3", "sector_code"],
        how="left",
    )

    # Normalize basic components into [0, 1] with simple clipping based on plausible ranges.
    # These choices are heuristic and can be refined later.
    def _norm(x: pd.Series, max_val: float) -> pd.Series:
        return (x.fillna(0.0) / max_val).clip(0.0, 1.0)

    # HHI naturally lies in (0,1]; rescale directly.
    metrics["import_hhi_norm"] = metrics["import_hhi"].fillna(0.0).clip(0.0, 1.0)
    metrics["export_hhi_norm"] = metrics["export_hhi"].fillna(0.0).clip(0.0, 1.0)

    metrics["top1_import_norm"] = metrics["top1_import_share"].fillna(0.0).clip(0.0, 1.0)
    metrics["top1_export_norm"] = metrics["top1_export_share"].fillna(0.0).clip(0.0, 1.0)

    # Centrality is positive; scale by max within the dataset for each sector-year later.
    # For now, approximate global scaling.
    if "centrality_score" in metrics.columns and metrics["centrality_score"].notna().any():
        max_cent = float(metrics["centrality_score"].max())
        metrics["centrality_norm"] = _norm(metrics["centrality_score"], max_cent if max_cent > 0 else 1.0)
    else:
        metrics["centrality_norm"] = 0.0

    metrics["replaceability_score"] = metrics["replaceability_score"].fillna(0.0).clip(0.0, 1.0)

    # Exposure index: high when imports are concentrated and hard to replace.
    metrics["exposure_index"] = (
        0.4 * metrics["import_hhi_norm"]
        + 0.4 * metrics["top1_import_norm"]
        + 0.2 * (1.0 - metrics["replaceability_score"])
    ).clip(0.0, 1.0)

    # Compute average dependence of others on this country from bilateral table.
    avg_dep = (
        bilateral.groupby(["year", "sector_code", "country_b"], as_index=False)["dep_a_on_b"]
        .mean()
        .rename(columns={"country_b": "country_iso3", "dep_a_on_b": "avg_dep_others_on_country"})
    )

    metrics = metrics.merge(
        avg_dep,
        on=["year", "sector_code", "country_iso3"],
        how="left",
    )
    metrics["avg_dep_others_on_country"] = metrics["avg_dep_others_on_country"].fillna(0.0)

    # Normalize average dependence with a simple cap at 1.
    metrics["avg_dep_norm"] = metrics["avg_dep_others_on_country"].clip(0.0, 1.0)

    # Leverage index: high when others depend on you, your export markets are concentrated, and you are central.
    metrics["leverage_index"] = (
        0.4 * metrics["avg_dep_norm"]
        + 0.3 * metrics["export_hhi_norm"]
        + 0.3 * metrics["centrality_norm"]
    ).clip(0.0, 1.0)

    # Pairwise metrics: thin down bilateral table for visualization-ready use.
    pairwise_metrics = bilateral[
        [
            "year",
            "sector_code",
            "country_a",
            "country_b",
            "dep_a_on_b",
            "dep_b_on_a",
            "asymmetry_diff",
            "asymmetry_log_ratio",
        ]
    ].copy()

    return metrics, pairwise_metrics


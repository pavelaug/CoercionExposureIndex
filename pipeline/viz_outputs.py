"""
Helpers to construct visualization-ready outputs from computed metrics.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


def build_country_year_sector_metrics(metrics: pd.DataFrame) -> pd.DataFrame:
    """
    Thin down the full metrics DataFrame into a compact table suitable
    for maps and time series charts.

    Columns:
      - year
      - country_iso3
      - sector_code
      - exposure_index
      - leverage_index
      - import_hhi
      - export_hhi
      - top1_import_share
      - top1_export_share
      - centrality_score
    """
    cols = [
        "year",
        "country_iso3",
        "sector_code",
        "exposure_index",
        "leverage_index",
        "import_hhi",
        "export_hhi",
        "top1_import_share",
        "top1_export_share",
        "centrality_score",
    ]
    # Some columns may not exist if upstream calculations were not run;
    # select intersection to avoid KeyErrors.
    existing_cols = [c for c in cols if c in metrics.columns]
    subset = metrics[existing_cols].copy()
    return subset


def build_pairwise_dependency_edges(pairwise_metrics: pd.DataFrame) -> pd.DataFrame:
    """
    Thin pairwise metrics into a table consumable by network visualizations.

    Columns:
      - year
      - sector_code
      - exporter_iso3 (alias for country_b from dep_a_on_b perspective)
      - importer_iso3 (alias for country_a)
      - dep_importer_on_exporter
      - dep_exporter_on_importer
      - asymmetry_diff
      - asymmetry_log_ratio
    """
    df = pairwise_metrics.copy()
    df = df.rename(
        columns={
            "country_a": "importer_iso3",
            "country_b": "exporter_iso3",
            "dep_a_on_b": "dep_importer_on_exporter",
            "dep_b_on_a": "dep_exporter_on_importer",
        }
    )

    cols = [
        "year",
        "sector_code",
        "exporter_iso3",
        "importer_iso3",
        "dep_importer_on_exporter",
        "dep_exporter_on_importer",
        "asymmetry_diff",
        "asymmetry_log_ratio",
    ]
    existing_cols = [c for c in cols if c in df.columns]
    return df[existing_cols].copy()


def build_time_series_for_country(
    country_metrics: pd.DataFrame,
    country_iso3: str,
    sectors: List[str] | None = None,
) -> pd.DataFrame:
    """
    Extract a time series slice for a given country (and optional subset of sectors).

    Columns:
      - year
      - sector_code
      - exposure_index
      - leverage_index
    """
    df = country_metrics[country_metrics["country_iso3"] == country_iso3].copy()
    if sectors is not None:
        df = df[df["sector_code"].isin(sectors)]

    cols = ["year", "sector_code", "exposure_index", "leverage_index"]
    existing_cols = [c for c in cols if c in df.columns]
    df = df[existing_cols].sort_values(["sector_code", "year"])
    return df.reset_index(drop=True)


def validate_country_metrics_ranges(country_metrics: pd.DataFrame) -> Dict[str, float]:
    """
    Run simple range and missing-value checks on country metrics.

    Returns a small summary dictionary with observed min/max for key fields.
    """
    summary: Dict[str, float] = {}
    for col in ["exposure_index", "leverage_index"]:
        if col in country_metrics.columns:
            vals = country_metrics[col].dropna().to_numpy(dtype=float)
            if vals.size:
                summary[f"{col}_min"] = float(np.min(vals))
                summary[f"{col}_max"] = float(np.max(vals))
    return summary


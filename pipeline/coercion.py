"""
Coercion exposure scores derived from trade dependency metrics.

This module takes the country-level and pairwise metrics produced by
`pipeline.metrics.compute_composite_indices` and computes a directed
coercion score for each ordered country pair.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class CoercionWeights:
    """
    Weights for the coercion score components.

    All components are expected to be in [0, 1].
    """

    dep_target_on_source: float = 0.4
    one_minus_dep_source_on_target: float = 0.2
    one_minus_replaceability_target: float = 0.2
    centrality_source: float = 0.2


def compute_coercion_scores(
    country_metrics: pd.DataFrame,
    pairwise_metrics: pd.DataFrame,
    year: Optional[int] = None,
    sector_code: Optional[str] = None,
    weights: Optional[CoercionWeights] = None,
) -> pd.DataFrame:
    """
    Compute a directed coercion score for each ordered country pair.

    Interpretation:
      - A row in `pairwise_metrics` has:
          country_a, country_b, dep_a_on_b, dep_b_on_a, ...
        which we read as:
          - country_a = target (importer)
          - country_b = source (exporter)
      - Coercion score S->T is high when:
          * T strongly depends on S (dep_a_on_b high),
          * S does not depend much on T (dep_b_on_a low),
          * T's imports in that sector are hard to replace,
          * S is central in the sector's trade network.

    Args:
        country_metrics: DataFrame from `compute_composite_indices` containing,
            at minimum:
              year, country_iso3, sector_code,
              replaceability_score, centrality_norm
        pairwise_metrics: DataFrame from `compute_composite_indices` containing,
            at minimum:
              year, sector_code, country_a, country_b,
              dep_a_on_b, dep_b_on_a
        year: optional year filter
        sector_code: optional sector filter
        weights: optional CoercionWeights instance

    Returns:
        DataFrame with columns:
          - year
          - sector_code
          - source_iso3
          - target_iso3
          - coercion_score  (in [0,1])
          - dep_target_on_source
          - dep_source_on_target
          - replaceability_target
          - centrality_source
    """
    if weights is None:
        weights = CoercionWeights()

    df = pairwise_metrics.copy()

    if year is not None:
        df = df[df["year"] == year]
    if sector_code is not None:
        df = df[df["sector_code"] == sector_code]

    if df.empty:
        return df.assign(
            source_iso3=pd.Series(dtype="string"),
            target_iso3=pd.Series(dtype="string"),
            coercion_score=pd.Series(dtype="float64"),
        )

    # Country-level metrics for target (country_a) and source (country_b)
    targets = country_metrics[
        ["year", "sector_code", "country_iso3", "replaceability_score"]
    ].rename(
        columns={
            "country_iso3": "target_iso3",
            "replaceability_score": "replaceability_target",
        }
    )

    sources = country_metrics[
        ["year", "sector_code", "country_iso3", "centrality_norm"]
    ].rename(
        columns={
            "country_iso3": "source_iso3",
            "centrality_norm": "centrality_source",
        }
    )

    # Start from bilateral metrics, rename roles
    df = df.rename(
        columns={
            "country_a": "target_iso3",
            "country_b": "source_iso3",
            "dep_a_on_b": "dep_target_on_source",
            "dep_b_on_a": "dep_source_on_target",
        }
    )

    # Join in target replaceability and source centrality
    df = df.merge(
        targets,
        on=["year", "sector_code", "target_iso3"],
        how="left",
    ).merge(
        sources,
        on=["year", "sector_code", "source_iso3"],
        how="left",
    )

    # Fill missing with zeros where appropriate
    for col in [
        "dep_target_on_source",
        "dep_source_on_target",
        "replaceability_target",
        "centrality_source",
    ]:
        if col in df.columns:
            df[col] = df[col].fillna(0.0)

    # Ensure components lie in [0,1] before combining
    df["dep_target_on_source"] = df["dep_target_on_source"].clip(0.0, 1.0)
    df["dep_source_on_target"] = df["dep_source_on_target"].clip(0.0, 1.0)
    df["replaceability_target"] = df["replaceability_target"].clip(0.0, 1.0)
    df["centrality_source"] = df["centrality_source"].clip(0.0, 1.0)

    # Combine into a single coercion score
    w = weights
    score = (
        w.dep_target_on_source * df["dep_target_on_source"]
        + w.one_minus_dep_source_on_target * (1.0 - df["dep_source_on_target"])
        + w.one_minus_replaceability_target * (1.0 - df["replaceability_target"])
        + w.centrality_source * df["centrality_source"]
    )

    df["coercion_score"] = score.clip(0.0, 1.0)

    cols = [
        "year",
        "sector_code",
        "source_iso3",
        "target_iso3",
        "coercion_score",
        "dep_target_on_source",
        "dep_source_on_target",
        "replaceability_target",
        "centrality_source",
    ]

    return df[cols].copy()


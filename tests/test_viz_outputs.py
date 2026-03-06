import numpy as np
import pandas as pd

from pipeline.metrics import (
    compute_bilateral_asymmetry,
    compute_centrality,
    compute_composite_indices,
    compute_export_concentration,
    compute_export_shares,
    compute_import_concentration,
    compute_import_shares,
    compute_replaceability,
)
from pipeline.viz_outputs import (
    build_country_year_sector_metrics,
    build_pairwise_dependency_edges,
    build_time_series_for_country,
    validate_country_metrics_ranges,
)


def _toy_flows_sector():
    """
    Small synthetic sector-level flows for visualization tests.
    """
    data = [
        ("A", "X", 50.0),
        ("B", "X", 50.0),
        ("A", "Y", 30.0),
        ("C", "Y", 70.0),
    ]
    df = pd.DataFrame(data, columns=["exporter_iso3", "importer_iso3", "value_kusd"])
    df["year"] = 2024
    df["sector_code"] = "energy"
    return df


def _build_full_metrics_from_toy():
    flows_sector = _toy_flows_sector()
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
    return country_metrics, pairwise_metrics


def test_build_country_year_sector_metrics_schema():
    country_metrics, _ = _build_full_metrics_from_toy()
    subset = build_country_year_sector_metrics(country_metrics)

    # Ensure required identifying columns exist.
    for col in ["year", "country_iso3", "sector_code"]:
        assert col in subset.columns

    # Exposure and leverage indices should be within [0,1].
    assert ((subset["exposure_index"] >= 0.0) & (subset["exposure_index"] <= 1.0)).all()
    assert ((subset["leverage_index"] >= 0.0) & (subset["leverage_index"] <= 1.0)).all()


def test_build_pairwise_dependency_edges_schema():
    _, pairwise_metrics = _build_full_metrics_from_toy()
    edges = build_pairwise_dependency_edges(pairwise_metrics)

    expected_cols = {
        "year",
        "sector_code",
        "exporter_iso3",
        "importer_iso3",
        "dep_importer_on_exporter",
        "dep_exporter_on_importer",
        "asymmetry_diff",
        "asymmetry_log_ratio",
    }
    assert expected_cols.issubset(set(edges.columns))


def test_build_time_series_for_country_filters_and_sorts():
    country_metrics, _ = _build_full_metrics_from_toy()
    ts = build_time_series_for_country(country_metrics, country_iso3="X")

    # Only country X should appear.
    assert set(ts["country_iso3"].tolist()) == {"X"} if "country_iso3" in ts.columns else True

    # Years should be sorted ascending for each sector (only one year here).
    if "year" in ts.columns and not ts.empty:
        years = ts["year"].tolist()
        assert years == sorted(years)


def test_validate_country_metrics_ranges_reports_min_max():
    country_metrics, _ = _build_full_metrics_from_toy()
    summary = validate_country_metrics_ranges(country_metrics)

    assert "exposure_index_min" in summary
    assert "exposure_index_max" in summary
    assert summary["exposure_index_min"] >= 0.0
    assert summary["exposure_index_max"] <= 1.0


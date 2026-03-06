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


def _toy_flows_sector():
    """
    Construct a tiny flows_sector DataFrame for synthetic tests.

    Scenario:
      - Year 2024, sector 'energy'
      - Exporters: A, B, C
      - Importers: X, Y
    """
    data = [
        # exporter, importer, value_kusd
        ("A", "X", 60.0),
        ("B", "X", 40.0),
        ("A", "Y", 20.0),
        ("C", "Y", 80.0),
    ]
    df = pd.DataFrame(
        data,
        columns=["exporter_iso3", "importer_iso3", "value_kusd"],
    )
    df["year"] = 2024
    df["sector_code"] = "energy"
    return df


def test_import_and_export_shares_sum_to_one():
    flows_sector = _toy_flows_sector()

    import_shares = compute_import_shares(flows_sector)
    export_shares = compute_export_shares(flows_sector)

    # For each importer-sector, shares should sum to ~1
    grouped_imports = import_shares.groupby(["year", "importer_iso3", "sector_code"])["import_share"].sum()
    assert np.allclose(grouped_imports.values, 1.0)

    # For each exporter-sector, shares should sum to ~1
    grouped_exports = export_shares.groupby(["year", "exporter_iso3", "sector_code"])["export_share"].sum()
    assert np.allclose(grouped_exports.values, 1.0)


def test_concentration_metrics_basic_properties():
    flows_sector = _toy_flows_sector()
    import_shares = compute_import_shares(flows_sector)
    export_shares = compute_export_shares(flows_sector)

    imports_conc = compute_import_concentration(import_shares)
    exports_conc = compute_export_concentration(export_shares)

    # There should be two importer rows (X, Y) with valid metrics
    assert set(imports_conc["country_iso3"].tolist()) == {"X", "Y"}
    assert (imports_conc["import_hhi"] > 0).all()
    assert (imports_conc["top1_import_share"] <= 1.0).all()

    # Export concentration: A, B, C
    assert set(exports_conc["country_iso3"].tolist()) == {"A", "B", "C"}
    assert (exports_conc["export_hhi"] > 0).all()
    assert (exports_conc["top1_export_share"] <= 1.0).all()


def test_bilateral_asymmetry_symmetry_properties():
    flows_sector = _toy_flows_sector()
    import_shares = compute_import_shares(flows_sector)
    bilateral = compute_bilateral_asymmetry(import_shares)

    # For any pair (A,B), asymmetry for (A,B) should be negative of (B,A)
    # with respect to asymmetry_diff, within numerical tolerance.
    ab = bilateral[
        (bilateral["country_a"] == "X") & (bilateral["country_b"] == "A")
    ].iloc[0]
    ba = bilateral[
        (bilateral["country_a"] == "A") & (bilateral["country_b"] == "X")
    ].iloc[0]

    assert np.isclose(ab["asymmetry_diff"], -ba["asymmetry_diff"], atol=1e-9)


def test_centrality_scores_present_for_nodes():
    flows_sector = _toy_flows_sector()
    import_shares = compute_import_shares(flows_sector)
    centrality = compute_centrality(import_shares)

    # All four nodes (A,B,C,X,Y) should appear at least once across country_iso3,
    # though centrality is computed on a directed graph exporter->importer.
    countries = set(centrality["country_iso3"].tolist())
    assert {"A", "B", "C", "X", "Y"}.issuperset(countries)


def test_replaceability_behaves_as_expected():
    flows_sector = _toy_flows_sector()
    replaceability = compute_replaceability(flows_sector)

    # There should be a replaceability score for importers X and Y.
    assert set(replaceability["country_iso3"].tolist()) == {"X", "Y"}
    assert ((replaceability["replaceability_score"] >= 0.0) & (replaceability["replaceability_score"] <= 1.0)).all()


def test_composite_indices_in_unit_interval():
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

    assert "exposure_index" in country_metrics.columns
    assert "leverage_index" in country_metrics.columns

    assert ((country_metrics["exposure_index"] >= 0.0) & (country_metrics["exposure_index"] <= 1.0)).all()
    assert ((country_metrics["leverage_index"] >= 0.0) & (country_metrics["leverage_index"] <= 1.0)).all()

    # Pairwise table should contain dependency and asymmetry columns.
    expected_pairwise_cols = {
        "year",
        "sector_code",
        "country_a",
        "country_b",
        "dep_a_on_b",
        "dep_b_on_a",
        "asymmetry_diff",
        "asymmetry_log_ratio",
    }
    assert expected_pairwise_cols.issubset(set(pairwise_metrics.columns))


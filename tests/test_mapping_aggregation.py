import numpy as np

from pipeline.aggregation import aggregate_flows
from pipeline.ingest import load_baci_raw, load_country_codes, load_product_codes
from pipeline.mapping import enrich_product_codes, hs6_to_hs2


def test_hs6_to_hs2_simple_cases():
    """
    hs6_to_hs2 should extract the first two digits of the HS6 code.
    """
    assert hs6_to_hs2(10110) == 1  # 010110
    assert hs6_to_hs2(10190) == 1  # 010190
    assert hs6_to_hs2(80810) == 8  # 080810
    assert hs6_to_hs2(270900) == 27
    assert hs6_to_hs2(999999) == 99


def test_enrich_product_codes_assigns_hs2_and_sector():
    """
    enrich_product_codes should add hs2_chapter and sector information
    for every product row.
    """
    products = load_product_codes()
    enriched = enrich_product_codes(products)

    assert "hs2_chapter" in enriched.columns
    assert "sector_code" in enriched.columns
    assert "sector_name" in enriched.columns

    # All rows should have non-null HS2 and sector.
    assert enriched["hs2_chapter"].notna().all()
    assert enriched["sector_code"].notna().all()


def test_aggregate_flows_preserves_bilateral_totals():
    """
    The sum of HS2 or sector-level flows for a given exporter-importer-year
    should equal the sum of raw BACI values for that pair.
    """
    baci = load_baci_raw()
    countries = load_country_codes()
    flows_hs2, flows_sector = aggregate_flows()

    # Compute raw pairwise totals (t, exporter_iso3, importer_iso3).
    country_map = countries[["country_code", "country_iso3"]].rename(
        columns={"country_code": "code", "country_iso3": "iso3"}
    )

    baci_iso = (
        baci.merge(country_map.rename(columns={"code": "i", "iso3": "exporter_iso3"}), on="i", how="left")
        .merge(country_map.rename(columns={"code": "j", "iso3": "importer_iso3"}), on="j", how="left")
    )

    raw_pairs = (
        baci_iso.groupby(["t", "exporter_iso3", "importer_iso3"], as_index=False)["v"]
        .sum()
        .rename(columns={"t": "year", "v": "raw_value_kusd"})
    )

    # HS2-aggregated totals for each pair.
    hs2_pairs = (
        flows_hs2.groupby(["year", "exporter_iso3", "importer_iso3"], as_index=False)["value_kusd"]
        .sum()
        .rename(columns={"value_kusd": "agg_value_kusd"})
    )

    merged = raw_pairs.merge(
        hs2_pairs,
        on=["year", "exporter_iso3", "importer_iso3"],
        how="left",
    )

    # There should be no significant discrepancy between raw and aggregated totals.
    diff = np.abs(merged["raw_value_kusd"] - merged["agg_value_kusd"])
    max_diff = float(diff.max())
    assert max_diff < 1e-6, f"Max difference between raw and HS2-aggregated totals too large: {max_diff}"


def test_sector_and_hs2_totals_match_for_pairs():
    """
    For each exporter-importer-year pair, the sum across HS2 chapters
    should match the sum across sectors (since sectors are a partition
    of HS2 chapters).
    """
    flows_hs2, flows_sector = aggregate_flows()

    hs2_pairs = (
        flows_hs2.groupby(["year", "exporter_iso3", "importer_iso3"], as_index=False)["value_kusd"]
        .sum()
        .rename(columns={"value_kusd": "hs2_total_kusd"})
    )

    sector_pairs = (
        flows_sector.groupby(["year", "exporter_iso3", "importer_iso3"], as_index=False)["value_kusd"]
        .sum()
        .rename(columns={"value_kusd": "sector_total_kusd"})
    )

    merged = hs2_pairs.merge(
        sector_pairs,
        on=["year", "exporter_iso3", "importer_iso3"],
        how="inner",
    )

    diff = np.abs(merged["hs2_total_kusd"] - merged["sector_total_kusd"])
    max_diff = float(diff.max())
    assert max_diff < 1e-6, f"Max difference between HS2 and sector totals too large: {max_diff}"


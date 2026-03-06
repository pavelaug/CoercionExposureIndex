import pandas as pd

from pipeline.ingest import load_baci_raw, load_country_codes, load_product_codes, validate_raw_consistency


def test_country_codes_cover_all_baci_countries():
    """
    Every exporter/importer code in the BACI file should appear in the country
    codes mapping.
    """
    baci = load_baci_raw()
    countries = load_country_codes()

    baci_codes = set(baci["i"].unique()) | set(baci["j"].unique())
    country_codes = set(countries["country_code"].astype("int32").tolist())

    missing = baci_codes - country_codes
    assert not missing, f"Missing country codes in mapping: {sorted(missing)[:10]}"


def test_product_codes_cover_all_baci_products():
    """
    Every HS6 product code in the BACI file should appear in the product
    codes mapping.
    """
    baci = load_baci_raw()
    products = load_product_codes()

    baci_products = set(baci["k"].unique())
    product_codes = set(products["code"].astype("int32").tolist())

    missing = baci_products - product_codes
    assert not missing, f"Missing product codes in mapping: {sorted(missing)[:10]}"


def test_validate_raw_consistency_summary():
    """
    validate_raw_consistency should report no missing exporter/importer/product
    codes on a well-formed dataset.
    """
    ok, details = validate_raw_consistency()
    assert ok, f"Raw consistency check failed with details: {details}"
    assert details["exporter_missing_count"] == 0
    assert details["importer_missing_count"] == 0
    assert details["product_missing_count"] == 0


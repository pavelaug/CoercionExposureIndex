"""
Ingestion and basic validation for BACI HS02 trade data.
"""

from __future__ import annotations

from pathlib import Path
from typing import Tuple

import pandas as pd

from .config import Paths, get_default_paths


def load_baci_raw(paths: Paths | None = None) -> pd.DataFrame:
    """
    Load the raw BACI HS02 CSV (single year) into a DataFrame.

    Columns (per CEPII docs):
      - t: year
      - i: exporter (numeric code)
      - j: importer (numeric code)
      - k: product (HS6 code, as int)
      - v: value (thousand USD)
      - q: quantity (metric tons)
    """
    if paths is None:
        paths = get_default_paths()

    path = paths.raw_baci
    df = pd.read_csv(path, dtype={"t": "int32", "i": "int32", "j": "int32", "k": "int32", "v": "float64", "q": "float64"})
    return df


def load_country_codes(paths: Paths | None = None) -> pd.DataFrame:
    """
    Load BACI country codes file.

    Expected columns:
      - country_code: numeric code used in BACI `i` and `j`
      - country_name
      - country_iso2
      - country_iso3
    """
    if paths is None:
        paths = get_default_paths()

    df = pd.read_csv(paths.raw_country_codes, dtype={"country_code": "int32", "country_iso2": "string", "country_iso3": "string"})
    return df


def load_product_codes(paths: Paths | None = None) -> pd.DataFrame:
    """
    Load HS6 product codes file.

    Expected columns:
      - code: HS6 numeric code
      - description: free-text description
    """
    if paths is None:
        paths = get_default_paths()

    df = pd.read_csv(paths.raw_product_codes, dtype={"code": "int32", "description": "string"})
    return df


def validate_raw_consistency(paths: Paths | None = None) -> Tuple[bool, dict]:
    """
    Perform basic consistency checks across raw BACI, country, and product files.

    Returns:
      (ok, details_dict)
    """
    if paths is None:
        paths = get_default_paths()

    issues: dict[str, object] = {}

    baci = load_baci_raw(paths)
    countries = load_country_codes(paths)
    products = load_product_codes(paths)

    # Check year consistency
    unique_years = baci["t"].unique()
    issues["unique_years"] = unique_years.tolist()

    # Map sets
    country_codes_set = set(countries["country_code"].astype("int32").tolist())
    exporter_missing = sorted(set(baci["i"].unique()) - country_codes_set)
    importer_missing = sorted(set(baci["j"].unique()) - country_codes_set)
    issues["exporter_missing_count"] = len(exporter_missing)
    issues["importer_missing_count"] = len(importer_missing)

    product_codes_set = set(products["code"].astype("int32").tolist())
    product_missing = sorted(set(baci["k"].unique()) - product_codes_set)
    issues["product_missing_count"] = len(product_missing)

    ok = (
        len(exporter_missing) == 0
        and len(importer_missing) == 0
        and len(product_missing) == 0
        and len(unique_years) >= 1
    )

    return ok, issues


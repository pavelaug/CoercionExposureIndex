"""
Aggregation logic from HS6-level BACI data to HS2 and sector-level flows.
"""

from __future__ import annotations

import pandas as pd

from .config import Paths, get_default_paths, sector_for_hs2
from .ingest import load_baci_raw, load_country_codes
from .mapping import hs6_to_hs2


def attach_country_iso(baci_df: pd.DataFrame, country_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join BACI data with country codes to attach ISO3 identifiers.
    """
    countries = country_df[["country_code", "country_iso3"]].rename(
        columns={"country_code": "code", "country_iso3": "iso3"}
    )

    merged = (
        baci_df
        .merge(countries.rename(columns={"code": "i", "iso3": "exporter_iso3"}), on="i", how="left")
        .merge(countries.rename(columns={"code": "j", "iso3": "importer_iso3"}), on="j", how="left")
    )
    return merged


def add_hs2_and_sector(baci_with_iso: pd.DataFrame) -> pd.DataFrame:
    """
    Add HS2 chapter and sector code columns to BACI data.
    """
    df = baci_with_iso.copy()
    df["hs2_chapter"] = df["k"].astype("int32").apply(hs6_to_hs2)
    df["sector_code"] = df["hs2_chapter"].apply(sector_for_hs2)
    return df


def aggregate_flows(
    paths: Paths | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Aggregate BACI HS6-level data to HS2 and sector-level bilateral flows.

    Returns:
      flows_hs2, flows_sector
    """
    if paths is None:
        paths = get_default_paths()

    baci = load_baci_raw(paths)
    countries = load_country_codes(paths)

    baci = attach_country_iso(baci, countries)
    baci = add_hs2_and_sector(baci)

    # Aggregate to HS2
    flows_hs2 = (
        baci.groupby(["t", "exporter_iso3", "importer_iso3", "hs2_chapter"], as_index=False)["v"]
        .sum()
        .rename(columns={"t": "year", "v": "value_kusd"})
    )

    # Aggregate to sector
    flows_sector = (
        baci.groupby(["t", "exporter_iso3", "importer_iso3", "sector_code"], as_index=False)["v"]
        .sum()
        .rename(columns={"t": "year", "v": "value_kusd"})
    )

    return flows_hs2, flows_sector


def compute_country_totals(flows_sector: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute per-country per-sector import and export totals from sector-level flows.

    Returns:
      imports_totals, exports_totals
    """
    imports_totals = (
        flows_sector.groupby(["year", "importer_iso3", "sector_code"], as_index=False)["value_kusd"]
        .sum()
        .rename(columns={"importer_iso3": "country_iso3", "value_kusd": "total_imports_kusd"})
    )

    exports_totals = (
        flows_sector.groupby(["year", "exporter_iso3", "sector_code"], as_index=False)["value_kusd"]
        .sum()
        .rename(columns={"exporter_iso3": "country_iso3", "value_kusd": "total_exports_kusd"})
    )

    return imports_totals, exports_totals


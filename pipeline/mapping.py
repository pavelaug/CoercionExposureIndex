"""
Mapping logic for HS6 -> HS2 chapters and sectors.
"""

from __future__ import annotations

import pandas as pd

from .config import SECTOR_NAMES, sector_for_hs2


def hs6_to_hs2(hs6_code: int | str) -> int | None:
    """
    Convert an HS6 code (usually numeric) to its HS2 chapter (1..99).

    Returns None for non-numeric special codes (e.g., '9999AA').
    """
    # HS codes are left-padded to 6 digits; integer division by 10_000 yields HS2.
    hs6_str = str(hs6_code)
    if not hs6_str.isdigit():
        return None  # special/non-numeric code
    return int(hs6_str) // 10_000


def enrich_product_codes(product_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add HS2 and sector information to the product codes DataFrame.

    Input columns:
      - code (HS6)
      - description

    Output columns (added):
      - hs2_chapter (int)
      - sector_code (str)
      - sector_name (str)
    """
    df = product_df.copy()
    df["hs2_chapter"] = df["code"].apply(hs6_to_hs2)
    df["sector_code"] = df["hs2_chapter"].apply(lambda x: sector_for_hs2(int(x)) if pd.notna(x) else "other")
    df["sector_name"] = df["sector_code"].map(SECTOR_NAMES)
    return df


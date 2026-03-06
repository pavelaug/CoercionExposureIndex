"""
Mapping logic for HS6 -> HS2 chapters and sectors.
"""

from __future__ import annotations

import pandas as pd

from .config import SECTOR_NAMES, sector_for_hs2


def hs6_to_hs2(hs6_code: int) -> int:
    """
    Convert an HS6 code (int like 80810 or 80810) to its HS2 chapter (1..99).
    """
    # HS codes are left-padded to 6 digits; integer division by 10_000 yields HS2.
    return hs6_code // 10_000


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
    df["hs2_chapter"] = df["code"].astype("int32").apply(hs6_to_hs2)
    df["sector_code"] = df["hs2_chapter"].apply(sector_for_hs2)
    df["sector_name"] = df["sector_code"].map(SECTOR_NAMES)
    return df


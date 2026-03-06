"""
Configuration and constants for the trade dependency pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict


@dataclass(frozen=True)
class Paths:
    """
    Container for all filesystem paths used by the pipeline.
    """

    project_root: Path

    @property
    def raw_baci(self) -> Path:
        return self.project_root / "BACI_HS02_Y2024_V202601.csv"

    @property
    def raw_country_codes(self) -> Path:
        return self.project_root / "country_codes_V202601.csv"

    @property
    def raw_product_codes(self) -> Path:
        return self.project_root / "product_codes_HS02_V202601.csv"

    @property
    def data_processed(self) -> Path:
        return self.project_root / "data" / "processed"

    @property
    def data_metrics(self) -> Path:
        return self.project_root / "data" / "metrics"


def get_default_paths() -> Paths:
    """
    Returns a Paths instance assuming this file is inside the project root.
    """
    project_root = Path(__file__).resolve().parent.parent
    return Paths(project_root=project_root)


# HS2 chapter to high-level sector mapping.
#
# Keys: integer HS2 chapter (1..99)
# Values: short sector code.
HS2_TO_SECTOR: Dict[int, str] = {
    # Agriculture & Food
    **{ch: "ag_food" for ch in range(1, 25)},
    # Energy & Fuels
    27: "energy",
    # Raw materials & inputs
    25: "raw_materials",
    26: "raw_materials",
    28: "raw_materials",
    29: "raw_materials",
    31: "raw_materials",
    44: "raw_materials",
    45: "raw_materials",
    46: "raw_materials",
    47: "raw_materials",
    48: "raw_materials",
    49: "raw_materials",
    # Chemicals, pharma, plastics, rubber
    30: "chem_pharma",
    32: "chem_pharma",
    33: "chem_pharma",
    34: "chem_pharma",
    35: "chem_pharma",
    36: "chem_pharma",
    37: "chem_pharma",
    38: "chem_pharma",
    39: "chem_pharma",
    40: "chem_pharma",
    # Textiles & Apparel
    **{ch: "textiles_apparel" for ch in range(50, 68)},
    # Metals & metal products
    **{ch: "metals" for ch in range(72, 84)},
    # Machinery & High-Tech
    84: "machinery_hightech",
    85: "machinery_hightech",
    88: "machinery_hightech",
    90: "machinery_hightech",
    # Transport equipment
    86: "transport",
    87: "transport",
    89: "transport",
    # Arms & ammunition
    93: "arms",
}


SECTOR_NAMES: Dict[str, str] = {
    "ag_food": "Agriculture & Food",
    "energy": "Energy & Fuels",
    "raw_materials": "Raw Materials & Basic Inputs",
    "chem_pharma": "Chemicals, Pharma & Plastics",
    "textiles_apparel": "Textiles, Apparel & Footwear",
    "metals": "Metals & Metal Products",
    "machinery_hightech": "Machinery, Electronics & High-Tech",
    "transport": "Transport Equipment",
    "arms": "Arms & Ammunition",
    "other": "Other Manufactures & Misc",
}


def sector_for_hs2(hs2: int) -> str:
    """
    Return the sector code for a given HS2 chapter.

    Chapters not explicitly mapped fall back to \"other\".
    """
    return HS2_TO_SECTOR.get(hs2, "other")


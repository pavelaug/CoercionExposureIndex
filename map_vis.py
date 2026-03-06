from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

PROJECT = Path(__file__).resolve().parent
EDGES = PROJECT / "coercion_edges_energy_2024.csv"  # or machinery_hightech
CENTROIDS = PROJECT / "country_centroids.csv"
COUNTRY_CODES = PROJECT / "country_codes_V202601.csv"
MIN_SCORE = 0.25
FOCUS = None  # e.g. "IND" or None

# Load coercion edges (already ISO3-based)
edges = pd.read_csv(EDGES)

# Load centroids (ISO2) and map to ISO3 using CEPII country codes
centroids_raw = pd.read_csv(CENTROIDS, dtype={"ISO": "string"})
codes = pd.read_csv(COUNTRY_CODES, dtype={"country_iso2": "string", "country_iso3": "string"})

centroids = (
    centroids_raw.merge(
        codes[["country_iso2", "country_iso3"]],
        left_on="ISO",
        right_on="country_iso2",
        how="left",
    )
    .rename(
        columns={
            "country_iso3": "country_iso3",
            "latitude": "lat",
            "longitude": "lon",
        }
    )[["country_iso3", "lat", "lon"]]
)

# join source/target coords by ISO3
edges = edges.merge(
    centroids.rename(
        columns={"country_iso3": "source_iso3", "lat": "source_lat", "lon": "source_lon"}
    ),
    on="source_iso3",
    how="left",
).merge(
    centroids.rename(
        columns={"country_iso3": "target_iso3", "lat": "target_lat", "lon": "target_lon"}
    ),
    on="target_iso3",
    how="left",
)

# basic filter: threshold + coords
edges = edges[
    (edges["coercion_score"] >= MIN_SCORE)
    & edges[["source_lat", "source_lon", "target_lat", "target_lon"]].notna().all(axis=1)
]

# optional focus mode: only show edges that touch this country
if FOCUS:
    edges = edges[(edges["source_iso3"] == FOCUS) | (edges["target_iso3"] == FOCUS)]


def tier_for_score(score: float) -> str:
    if score >= 0.6:
        return "high"
    elif score >= 0.4:
        return "mid"
    else:
        return "low"


edges["tier"] = edges["coercion_score"].apply(tier_for_score)

color_map = {
    "low": "rgba(160,160,160,0.25)",   # faint grey
    "mid": "rgba(255,165,0,0.6)",      # orange
    "high": "rgba(220,20,60,0.9)",     # strong red
}

width_map = {
    "low": 1.0,
    "mid": 2.0,
    "high": 4.0,
}

fig = go.Figure()

for _, row in edges.iterrows():
    w = float(row["coercion_score"])
    tier = row["tier"]
    fig.add_trace(
        go.Scattergeo(
            lon=[row["source_lon"], row["target_lon"]],
            lat=[row["source_lat"], row["target_lat"]],
            mode="lines",
            line=dict(
                width=width_map[tier],
                color=color_map[tier],
            ),
            hoverinfo="text",
            text=(
                f"{row['source_iso3']} → {row['target_iso3']}"
                f"<br>score={w:.2f} (tier={tier})"
            ),
        )
    )

fig.update_layout(
    title=f"Coercion exposure routes – {edges['sector_code'].iloc[0]}, {edges['year'].iloc[0]}",
    showlegend=False,
    geo=dict(
        projection_type="natural earth",
        showcountries=True,
        showland=True,
        landcolor="rgb(240,240,240)",
        coastlinecolor="rgb(180,180,180)",
    ),
)

out_html = PROJECT / f"coercion_routes_{edges['sector_code'].iloc[0]}_{edges['year'].iloc[0]}.html"
fig.write_html(out_html)
print(f"Wrote {out_html}")
from __future__ import annotations

import pandas as pd

try:
    import geopandas as gpd
    from shapely import wkt
except ImportError:  # pragma: no cover
    gpd = None
    wkt = None


class SpatialMatcher:
    def __init__(self, radius_m: float) -> None:
        self.radius_m = radius_m

    def match_stations_to_segments(self, segments: pd.DataFrame, stations: pd.DataFrame) -> pd.DataFrame:
        if segments.empty or stations.empty or gpd is None or wkt is None:
            return pd.DataFrame(columns=["segment_id", "station_id", "distance_m"])

        segment_geo = segments[["segment_id", "geometry_wkt"]].dropna().drop_duplicates().copy()
        segment_geo["geometry"] = segment_geo["geometry_wkt"].map(wkt.loads)
        segment_gdf = gpd.GeoDataFrame(segment_geo, geometry="geometry", crs="EPSG:4326").to_crs(3857)

        station_geo = stations[["station_id", "latitude", "longitude"]].dropna().drop_duplicates().copy()
        station_gdf = gpd.GeoDataFrame(
            station_geo,
            geometry=gpd.points_from_xy(station_geo["longitude"], station_geo["latitude"]),
            crs="EPSG:4326",
        ).to_crs(3857)

        rows = []
        for segment in segment_gdf.itertuples(index=False):
            distances = station_gdf.distance(segment.geometry)
            local = station_gdf.assign(distance_m=distances)
            local = local[local["distance_m"] <= self.radius_m].sort_values("distance_m").head(3)
            for station in local.itertuples(index=False):
                rows.append({"segment_id": segment.segment_id, "station_id": station.station_id, "distance_m": station.distance_m})
        return pd.DataFrame(rows)

    def aggregate_to_communes(self, segment_rollup: pd.DataFrame, segment_ref: pd.DataFrame) -> pd.DataFrame:
        if segment_rollup.empty or segment_ref.empty:
            return pd.DataFrame()

        merged = segment_rollup.merge(
            segment_ref[["segment_id", "commune_insee", "commune_name", "departement_code", "departement", "region"]].drop_duplicates(),
            on="segment_id",
            how="left",
        )
        return (
            merged.groupby(
                ["date", "commune_insee", "commune_name", "departement_code", "departement", "region"],
                dropna=False,
                as_index=False,
            )
            .agg(
                rain_obs_mm=("rain_obs_mm", "max"),
                rain_72h_mm=("rain_72h_mm", "max"),
                rain_forecast_mm=("rain_forecast_mm", "max"),
                official_vigilance_level=("official_vigilance_level", "max"),
                lgv_alert_level=("lgv_alert_level", "max"),
            )
        )

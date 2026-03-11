from __future__ import annotations

from datetime import datetime

import pandas as pd

from lgv_pluvio.connectors.base import BaseConnector, empty_frame


class HydroPortailConnector(BaseConnector):
    provider_name = "hydroportail"

    def fetch_metadata(self, area_spec: pd.DataFrame | None = None) -> pd.DataFrame:
        return empty_frame(
            [
                "station_id",
                "provider",
                "station_code",
                "station_name",
                "station_type",
                "latitude",
                "longitude",
                "geometry_wkt",
                "sandre_code",
                "active",
            ]
        )

    def fetch_observations(self, start: datetime, end: datetime, area_spec: pd.DataFrame) -> pd.DataFrame:
        return empty_frame(
            [
                "observation_id",
                "provider",
                "station_id",
                "datetime_utc",
                "parameter_code",
                "parameter_name",
                "value",
                "unit",
                "quality_flag",
            ]
        )

    def fetch_forecasts(self, start: datetime, end: datetime, area_spec: pd.DataFrame) -> pd.DataFrame:
        return empty_frame(
            [
                "provider",
                "target_location_type",
                "target_location_id",
                "forecast_run_at",
                "forecast_time",
                "rain_mm",
                "rain_probability",
            ]
        )

    def fetch_alerts(self, start: datetime, end: datetime, area_spec: pd.DataFrame | None = None) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "provider": self.provider_name,
                    "zone_name": "HydroPortail",
                    "severity": "info",
                    "published_at": pd.Timestamp.utcnow(),
                    "source_url": self.config["referential_url"],
                }
            ]
        )

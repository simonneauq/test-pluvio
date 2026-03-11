from __future__ import annotations

import re
from datetime import datetime

import pandas as pd

from lgv_pluvio.connectors.base import BaseConnector, empty_frame


class VigicruesConnector(BaseConnector):
    provider_name = "vigicrues"

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
        html = self._get(self.config["bulletin_url"])
        lowered = html.lower()
        severity = "green"
        for level in ["red", "orange", "yellow", "green"]:
            if level in lowered or {"red": "rouge", "orange": "orange", "yellow": "jaune", "green": "vert"}[level] in lowered:
                severity = level
                break

        departments = []
        for match in re.findall(r"\(([\d,\s]+)\)", html):
            departments.extend(code.strip() for code in match.split(",") if code.strip())

        return pd.DataFrame(
            [
                {
                    "provider": self.provider_name,
                    "zone_name": "Bulletin national Vigicrues",
                    "severity": severity,
                    "published_at": pd.Timestamp.utcnow(),
                    "source_url": self.config["bulletin_url"],
                    "department_code": ",".join(sorted(set(departments))),
                }
            ]
        )

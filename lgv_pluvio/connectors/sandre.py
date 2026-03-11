from __future__ import annotations

from datetime import datetime

import pandas as pd

from lgv_pluvio.connectors.base import BaseConnector, empty_frame


class SandreConnector(BaseConnector):
    provider_name = "sandre"

    def fetch_metadata(self, area_spec: pd.DataFrame | None = None) -> pd.DataFrame:
        try:
            payload = self._get(self.config["referential_url"] + "stationhydro.json", params={"limit": 50})
        except Exception:
            return empty_frame(["sandre_code", "label"])

        if isinstance(payload, dict):
            items = payload.get("data") or payload.get("items") or []
        else:
            items = []
        rows = [{"sandre_code": item.get("CdStationHydro"), "label": item.get("LbStationHydro")} for item in items]
        return pd.DataFrame(rows)

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
        return empty_frame(["provider", "zone_name", "severity", "published_at", "source_url"])

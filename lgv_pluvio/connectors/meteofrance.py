from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from lgv_pluvio.connectors.base import BaseConnector, empty_frame


class MeteoFranceConnector(BaseConnector):
    provider_name = "meteofrance"

    def _headers(self) -> dict[str, str]:
        headers = {"accept": "application/json"}
        if self.api_key:
            headers["apikey"] = self.api_key
        return headers

    def fetch_metadata(self, area_spec: pd.DataFrame | None = None) -> pd.DataFrame:
        if area_spec is None or area_spec.empty:
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

        frame = area_spec.copy()
        frame["station_id"] = frame["segment_id"].astype(str) + "::meteofrance"
        frame["provider"] = self.provider_name
        frame["station_code"] = frame["segment_id"].astype(str)
        frame["station_name"] = "Meteo-France " + frame["segment_name"].fillna(frame["segment_id"].astype(str))
        frame["station_type"] = "meteo"
        frame["geometry_wkt"] = None
        frame["sandre_code"] = None
        frame["active"] = bool(self.api_key)
        return frame[
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
        ]

    def fetch_observations(self, start: datetime, end: datetime, area_spec: pd.DataFrame) -> pd.DataFrame:
        if not self.api_key:
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

        rows: list[dict[str, Any]] = []
        url = self.config["base_url"].rstrip("/") + self.config["observations_endpoint"]
        for sample in area_spec.itertuples(index=False):
            try:
                payload = self._get(
                    url,
                    params={
                        "lat": sample.latitude,
                        "lon": sample.longitude,
                        "start_date": start.isoformat(),
                        "end_date": end.isoformat(),
                    },
                    headers=self._headers(),
                )
            except Exception:
                continue

            observations = payload.get("observations", payload if isinstance(payload, list) else [])
            for item in observations:
                timestamp = item.get("time") or item.get("date") or item.get("dt")
                rain_value = item.get("pluie") or item.get("rain") or item.get("precipitation")
                if timestamp is None or rain_value is None:
                    continue
                rows.append(
                    {
                        "observation_id": f"meteofrance::{sample.segment_id}::{timestamp}",
                        "provider": self.provider_name,
                        "station_id": f"{sample.segment_id}::meteofrance",
                        "datetime_utc": pd.to_datetime(timestamp, utc=False),
                        "parameter_code": "rain",
                        "parameter_name": "Rain",
                        "value": rain_value,
                        "unit": "mm",
                        "quality_flag": "official",
                    }
                )
        return pd.DataFrame(rows)

    def fetch_forecasts(self, start: datetime, end: datetime, area_spec: pd.DataFrame) -> pd.DataFrame:
        if not self.api_key:
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

        rows: list[dict[str, Any]] = []
        url = self.config["base_url"].rstrip("/") + self.config["forecast_endpoint"]
        for sample in area_spec.itertuples(index=False):
            try:
                payload = self._get(
                    url,
                    params={"lat": sample.latitude, "lon": sample.longitude},
                    headers=self._headers(),
                )
            except Exception:
                continue

            forecasts = payload.get("forecast", payload.get("previsions", []))
            run_at = pd.Timestamp.utcnow()
            for item in forecasts:
                timestamp = item.get("time") or item.get("date") or item.get("forecast_time")
                rain_value = item.get("pluie") or item.get("rain") or item.get("precipitation")
                if timestamp is None or rain_value is None:
                    continue
                rows.append(
                    {
                        "provider": self.provider_name,
                        "target_location_type": "segment",
                        "target_location_id": sample.segment_id,
                        "forecast_run_at": run_at,
                        "forecast_time": pd.to_datetime(timestamp, utc=False),
                        "rain_mm": rain_value,
                        "rain_probability": item.get("probability") or item.get("proba_precipitation"),
                    }
                )
        return pd.DataFrame(rows)

    def fetch_alerts(self, start: datetime, end: datetime, area_spec: pd.DataFrame | None = None) -> pd.DataFrame:
        return empty_frame(["provider", "zone_name", "severity", "published_at", "source_url"])

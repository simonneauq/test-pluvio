from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from lgv_pluvio.connectors.base import BaseConnector, empty_frame


class OpenMeteoConnector(BaseConnector):
    provider_name = "open_meteo"

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
        frame["station_id"] = frame["segment_id"].astype(str) + "::open_meteo"
        frame["provider"] = self.provider_name
        frame["station_code"] = frame["segment_id"].astype(str)
        frame["station_name"] = "Open-Meteo " + frame["segment_name"].fillna(frame["segment_id"].astype(str))
        frame["station_type"] = "meteo"
        frame["geometry_wkt"] = None
        frame["sandre_code"] = None
        frame["active"] = True
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

    def _fetch_hourly(
        self,
        url: str,
        latitude: float,
        longitude: float,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        payload = {
            "latitude": latitude,
            "longitude": longitude,
            "timezone": "Europe/Paris",
            **params,
        }
        return self._get(url, params=payload)

    def fetch_observations(self, start: datetime, end: datetime, area_spec: pd.DataFrame) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for sample in area_spec.itertuples(index=False):
            data = self._fetch_hourly(
                self.config["archive_url"],
                sample.latitude,
                sample.longitude,
                {
                    "start_date": start.date().isoformat(),
                    "end_date": end.date().isoformat(),
                    "hourly": "precipitation,rain",
                },
            )
            hourly = data.get("hourly", {})
            for timestamp, precip, rain in zip(
                hourly.get("time", []),
                hourly.get("precipitation", []),
                hourly.get("rain", []),
            ):
                rows.append(
                    {
                        "observation_id": f"open-meteo::{sample.segment_id}::{timestamp}",
                        "provider": self.provider_name,
                        "station_id": f"{sample.segment_id}::open_meteo",
                        "datetime_utc": pd.to_datetime(timestamp, utc=False),
                        "parameter_code": "rain",
                        "parameter_name": "Rain",
                        "value": rain if rain is not None else precip,
                        "unit": "mm",
                        "quality_flag": "reanalysis",
                    }
                )
        return pd.DataFrame(rows)

    def fetch_forecasts(self, start: datetime, end: datetime, area_spec: pd.DataFrame) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for sample in area_spec.itertuples(index=False):
            data = self._fetch_hourly(
                self.config["forecast_url"],
                sample.latitude,
                sample.longitude,
                {
                    "forecast_days": max((end.date() - start.date()).days + 1, 1),
                    "hourly": "precipitation_probability,precipitation",
                },
            )
            hourly = data.get("hourly", {})
            forecast_run_at = pd.Timestamp.utcnow()
            for timestamp, precip, probability in zip(
                hourly.get("time", []),
                hourly.get("precipitation", []),
                hourly.get("precipitation_probability", []),
            ):
                rows.append(
                    {
                        "provider": self.provider_name,
                        "target_location_type": "segment",
                        "target_location_id": sample.segment_id,
                        "forecast_run_at": forecast_run_at,
                        "forecast_time": pd.to_datetime(timestamp, utc=False),
                        "rain_mm": precip,
                        "rain_probability": probability,
                    }
                )
        return pd.DataFrame(rows)

    def fetch_alerts(self, start: datetime, end: datetime, area_spec: pd.DataFrame | None = None) -> pd.DataFrame:
        return empty_frame(["provider", "zone_name", "severity", "published_at", "source_url"])

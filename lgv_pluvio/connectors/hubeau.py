from __future__ import annotations

from datetime import datetime

import pandas as pd

from lgv_pluvio.connectors.base import BaseConnector, empty_frame


class HubeauConnector(BaseConnector):
    provider_name = "hubeau"

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

        bbox = ",".join(
            [
                str(area_spec["longitude"].min()),
                str(area_spec["latitude"].min()),
                str(area_spec["longitude"].max()),
                str(area_spec["latitude"].max()),
            ]
        )
        payload = self._get(self.config["stations_url"], params={"bbox": bbox, "size": 500})
        stations = payload.get("data", [])
        rows = []
        for station in stations:
            rows.append(
                {
                    "station_id": station.get("code_station") or station.get("code_site"),
                    "provider": self.provider_name,
                    "station_code": station.get("code_station") or station.get("code_site"),
                    "station_name": station.get("libelle_station") or station.get("libelle_site"),
                    "station_type": "hydro",
                    "latitude": station.get("latitude"),
                    "longitude": station.get("longitude"),
                    "geometry_wkt": None,
                    "sandre_code": station.get("code_station"),
                    "active": True,
                }
            )
        return pd.DataFrame(rows)

    def fetch_observations(self, start: datetime, end: datetime, area_spec: pd.DataFrame) -> pd.DataFrame:
        if area_spec.empty:
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

        stations = self.fetch_metadata(area_spec)
        rows = []
        for station_id in stations["station_id"].dropna().unique().tolist():
            payload = self._get(
                self.config["observations_url"],
                params={
                    "code_entite": station_id,
                    "date_debut_obs": start.strftime("%Y-%m-%dT%H:%M:%S"),
                    "date_fin_obs": end.strftime("%Y-%m-%dT%H:%M:%S"),
                    "grandeur_hydro": "H,Q",
                    "size": 10000,
                },
            )
            observations = payload.get("data", [])
            for item in observations:
                timestamp = item.get("date_obs")
                grandeur = item.get("grandeur_hydro")
                if not timestamp or not grandeur:
                    continue
                rows.append(
                    {
                        "observation_id": f"hubeau::{station_id}::{grandeur}::{timestamp}",
                        "provider": self.provider_name,
                        "station_id": station_id,
                        "datetime_utc": pd.to_datetime(timestamp, utc=False),
                        "parameter_code": grandeur,
                        "parameter_name": "Hydro level" if grandeur == "H" else "Hydro flow",
                        "value": item.get("resultat_obs"),
                        "unit": item.get("symbole_unite"),
                        "quality_flag": item.get("code_qualification") or "raw",
                    }
                )
        return pd.DataFrame(rows)

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

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import pandas as pd

from lgv_pluvio.config import AppConfig
from lgv_pluvio.connectors.factory import build_connectors
from lgv_pluvio.data_pipeline.reference_loader import load_reference_bundle
from lgv_pluvio.data_pipeline.spatial import SpatialMatcher
from lgv_pluvio.domain.alerts import AlertEngine
from lgv_pluvio.sample_data import build_sample_bundle
from lgv_pluvio.storage.repository import Repository


@dataclass(slots=True)
class RefreshResult:
    used_sample_data: bool
    message: str


def _safe_concat(frames: list[pd.DataFrame]) -> pd.DataFrame:
    valid = [frame for frame in frames if frame is not None and not frame.empty]
    return pd.concat(valid, ignore_index=True) if valid else pd.DataFrame()


def _select_primary_weather_provider(frame: pd.DataFrame, priorities: list[str]) -> pd.DataFrame:
    if frame.empty:
        return frame
    ranked = frame.copy()
    ranked["provider_rank"] = ranked["provider"].apply(
        lambda value: priorities.index(value) if value in priorities else len(priorities)
    )
    ranked = ranked.sort_values(["station_id", "datetime_utc", "provider_rank"])
    return ranked.drop_duplicates(subset=["station_id", "datetime_utc"], keep="first").drop(columns=["provider_rank"])


def _build_segment_rollup(
    observations: pd.DataFrame,
    forecasts: pd.DataFrame,
    matches: pd.DataFrame,
    alerts: pd.DataFrame,
    segment_ref: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "segment_id",
        "date",
        "rain_1h_mm",
        "rain_obs_mm",
        "rain_72h_mm",
        "rain_forecast_mm",
        "nearest_hydro_station_id",
        "hydro_level_value",
        "hydro_flow_value",
        "official_vigilance_level",
    ]
    if observations.empty:
        return pd.DataFrame(columns=columns)

    frame = observations.copy()
    frame["date"] = pd.to_datetime(frame["datetime_utc"]).dt.floor("D")
    weather_obs = frame[frame["parameter_code"].astype(str).str.lower().isin(["rain", "precipitation"])].copy()
    weather_obs["segment_id"] = weather_obs["station_id"].str.split("::").str[:3].str.join("::")
    rain_daily = (
        weather_obs.groupby(["segment_id", "date"], as_index=False)
        .agg(rain_1h_mm=("value", "max"), rain_obs_mm=("value", "sum"))
        .sort_values(["segment_id", "date"])
    )
    rain_daily["rain_72h_mm"] = (
        rain_daily.groupby("segment_id")["rain_obs_mm"].rolling(3, min_periods=1).sum().reset_index(level=0, drop=True)
    )

    if forecasts.empty:
        forecast_daily = pd.DataFrame(columns=["segment_id", "date", "rain_forecast_mm"])
    else:
        forecast_daily = forecasts.copy()
        forecast_daily["date"] = pd.to_datetime(forecast_daily["forecast_time"]).dt.floor("D")
        forecast_daily = forecast_daily.groupby(["target_location_id", "date"], as_index=False).agg(rain_forecast_mm=("rain_mm", "sum"))
        forecast_daily = forecast_daily.rename(columns={"target_location_id": "segment_id"})

    hydro_obs = frame[frame["parameter_code"].isin(["H", "Q"])].copy()
    if hydro_obs.empty or matches.empty:
        hydro_daily = pd.DataFrame(columns=["segment_id", "date", "nearest_hydro_station_id", "hydro_level_value", "hydro_flow_value"])
    else:
        hydro_obs = hydro_obs.merge(matches, on="station_id", how="inner")
        hydro_obs["date"] = pd.to_datetime(hydro_obs["datetime_utc"]).dt.floor("D")
        hydro_daily = (
            hydro_obs.pivot_table(index=["segment_id", "date"], columns="parameter_code", values="value", aggfunc="max")
            .reset_index()
            .rename(columns={"H": "hydro_level_value", "Q": "hydro_flow_value"})
        )
        nearest = matches.sort_values("distance_m").drop_duplicates("segment_id")
        hydro_daily = hydro_daily.merge(
            nearest[["segment_id", "station_id"]].rename(columns={"station_id": "nearest_hydro_station_id"}),
            on="segment_id",
            how="left",
        )

    official_level = "green"
    if not alerts.empty and "severity" in alerts.columns:
        order = {"green": 0, "yellow": 1, "orange": 2, "red": 3, "info": 0}
        best = alerts.assign(score=alerts["severity"].map(order).fillna(0)).sort_values("score")
        official_level = best.iloc[-1]["severity"]

    rollup = rain_daily.merge(forecast_daily, on=["segment_id", "date"], how="left").merge(hydro_daily, on=["segment_id", "date"], how="left")
    rollup["official_vigilance_level"] = official_level
    return rollup.merge(segment_ref[["segment_id"]].drop_duplicates(), on="segment_id", how="inner")


def run_refresh(config: AppConfig, mode: str = "daily") -> RefreshResult:
    repository = Repository(config.db_path)

    try:
        refs = load_reference_bundle(config)
        repository.upsert_communes(refs["communes"])
        repository.upsert_lgv_segments(refs["segments"].drop_duplicates(subset=["segment_id"], keep="first"))
    except Exception as error:
        if not config.sample_if_empty:
            raise
        sample = build_sample_bundle()
        repository.upsert_communes(sample["communes"])
        repository.upsert_lgv_segments(sample["segments"])
        repository.upsert_source_stations(sample["stations"])
        repository.upsert_station_matches(sample["station_match"])
        repository.upsert_segment_daily_rollup(sample["segment_daily_rollup"])
        repository.upsert_commune_daily_rollup(sample["commune_daily_rollup"])
        repository.upsert_alert_sources(sample["alert_source"])
        repository.upsert_alert_events(sample["alert_event"])
        repository.log_sync("bootstrap", mode, "sample", str(error))
        return RefreshResult(True, "Referentiels indisponibles, jeu de demonstration charge.")

    connectors = build_connectors(config)
    sample_points = refs["sample_points"]
    station_frames = []
    observation_frames = []
    forecast_frames = []
    alert_frames = []

    now = pd.Timestamp.now(tz=config.timezone).tz_localize(None)
    recent_start = (now - timedelta(days=30)) if mode == "daily" else (now - pd.DateOffset(years=config.history_years))
    forecast_end = now + timedelta(days=7)

    for provider_name, connector in connectors.items():
        try:
            if provider_name in {"open_meteo", "meteofrance"}:
                station_frames.append(connector.fetch_metadata(sample_points))
                observation_frames.append(connector.fetch_observations(recent_start.to_pydatetime(), now.to_pydatetime(), sample_points))
                forecast_frames.append(connector.fetch_forecasts(now.to_pydatetime(), forecast_end.to_pydatetime(), sample_points))
            elif provider_name == "hubeau":
                station_frames.append(connector.fetch_metadata(sample_points))
                observation_frames.append(connector.fetch_observations(recent_start.to_pydatetime(), now.to_pydatetime(), sample_points))
            elif provider_name in {"vigicrues", "hydroportail"}:
                alert_frames.append(connector.fetch_alerts(recent_start.to_pydatetime(), now.to_pydatetime(), sample_points))
            elif provider_name == "sandre":
                connector.fetch_metadata(sample_points)
            repository.log_sync(provider_name, mode, "ok")
        except Exception as error:
            repository.log_sync(provider_name, mode, "error", str(error))

    stations = _safe_concat(station_frames).drop_duplicates(subset=["station_id"], keep="first")
    observations = _safe_concat(observation_frames)
    if not observations.empty:
        observations["datetime_utc"] = pd.to_datetime(observations["datetime_utc"])
        weather_mask = observations["parameter_code"].astype(str).str.lower().isin(["rain", "precipitation"])
        weather = _select_primary_weather_provider(observations.loc[weather_mask], config.providers["priority_weather"])
        observations = pd.concat([weather, observations.loc[~weather_mask]], ignore_index=True)

    forecasts = _safe_concat(forecast_frames)
    alerts = _safe_concat(alert_frames)

    matcher = SpatialMatcher(config.matching["hydro_station_radius_m"])
    hydro_stations = stations[stations["station_type"] == "hydro"] if not stations.empty else pd.DataFrame()
    segment_ref = refs["segments"]
    unique_segments = segment_ref.drop_duplicates(subset=["segment_id"], keep="first")
    matches = matcher.match_stations_to_segments(unique_segments, hydro_stations)
    rollup = _build_segment_rollup(observations, forecasts, matches, alerts, segment_ref)
    rollup, events = AlertEngine(config.thresholds).compute_segment_alerts(rollup)
    communes = matcher.aggregate_to_communes(rollup, segment_ref)

    repository.upsert_source_stations(stations)
    repository.upsert_observations(observations)
    repository.upsert_forecasts(forecasts)
    repository.upsert_station_matches(matches)
    repository.upsert_alert_sources(alerts)
    repository.upsert_segment_daily_rollup(rollup)
    repository.upsert_commune_daily_rollup(communes)
    repository.upsert_alert_events(events)
    repository.log_sync("pipeline", mode, "ok", f"stations={len(stations)} observations={len(observations)}")
    return RefreshResult(False, "Refresh termine.")

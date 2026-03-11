from __future__ import annotations

import pandas as pd


def build_sample_bundle() -> dict[str, pd.DataFrame]:
    dates = pd.date_range("2026-03-01", periods=10, freq="D")
    segments = pd.DataFrame(
        [
            {
                "segment_id": "LGV1::0.0::10.0",
                "segment_name": "Tours sud",
                "pk_start": 0.0,
                "pk_end": 10.0,
                "line_code": "LGV1",
                "commune_insee": "37208",
                "commune_name": "SAINT-AVERTIN",
                "departement_code": "37",
                "departement": "INDRE-ET-LOIRE",
                "region": "CENTRE-VAL DE LOIRE",
                "geometry_wkt": "LINESTRING (0.55 47.35, 0.62 47.28)",
            },
            {
                "segment_id": "LGV1::10.0::20.0",
                "segment_name": "Veigne",
                "pk_start": 10.0,
                "pk_end": 20.0,
                "line_code": "LGV1",
                "commune_insee": "37266",
                "commune_name": "VEIGNE",
                "departement_code": "37",
                "departement": "INDRE-ET-LOIRE",
                "region": "CENTRE-VAL DE LOIRE",
                "geometry_wkt": "LINESTRING (0.62 47.28, 0.72 47.23)",
            },
        ]
    )
    rollups = []
    for i, date in enumerate(dates):
        rollups.extend(
            [
                {
                    "segment_id": "LGV1::0.0::10.0",
                    "date": date,
                    "rain_1h_mm": 5 + i,
                    "rain_obs_mm": 10 + i * 2,
                    "rain_72h_mm": 20 + i * 3,
                    "rain_forecast_mm": 8 + i,
                    "nearest_hydro_station_id": "H123",
                    "hydro_level_value": 1.5 + i * 0.1,
                    "hydro_flow_value": 20 + i,
                    "official_vigilance_level": "yellow" if i > 5 else "green",
                    "lgv_alert_level": "yellow",
                },
                {
                    "segment_id": "LGV1::10.0::20.0",
                    "date": date,
                    "rain_1h_mm": 7 + i,
                    "rain_obs_mm": 12 + i * 2,
                    "rain_72h_mm": 25 + i * 3,
                    "rain_forecast_mm": 10 + i,
                    "nearest_hydro_station_id": "H456",
                    "hydro_level_value": 1.8 + i * 0.2,
                    "hydro_flow_value": 28 + i,
                    "official_vigilance_level": "orange" if i > 7 else "yellow",
                    "lgv_alert_level": "orange" if i > 7 else "yellow",
                },
            ]
        )
    segment_daily_rollup = pd.DataFrame(rollups)
    commune_daily_rollup = segment_daily_rollup.merge(
        segments[["segment_id", "commune_insee", "commune_name", "departement_code", "departement", "region"]],
        on="segment_id",
        how="left",
    )
    stations = pd.DataFrame(
        [
            {
                "station_id": "LGV1::0.0::10.0::open_meteo",
                "provider": "open_meteo",
                "station_code": "LGV1::0.0::10.0",
                "station_name": "Open-Meteo Tours sud",
                "station_type": "meteo",
                "latitude": 47.31,
                "longitude": 0.58,
                "geometry_wkt": None,
                "sandre_code": None,
                "active": True,
            },
            {
                "station_id": "H123",
                "provider": "hubeau",
                "station_code": "H123",
                "station_name": "Station hydro 123",
                "station_type": "hydro",
                "latitude": 47.30,
                "longitude": 0.60,
                "geometry_wkt": None,
                "sandre_code": "H123",
                "active": True,
            },
        ]
    )
    station_match = pd.DataFrame(
        [
            {"segment_id": "LGV1::0.0::10.0", "station_id": "H123", "distance_m": 1234},
            {"segment_id": "LGV1::10.0::20.0", "station_id": "H456", "distance_m": 2345},
        ]
    )
    alert_source = pd.DataFrame(
        [
            {
                "provider": "vigicrues",
                "zone_name": "Bulletin national Vigicrues",
                "severity": "yellow",
                "published_at": pd.Timestamp.utcnow(),
                "source_url": "https://www.vigicrues.gouv.fr/bulletin_national",
                "department_code": "37",
            }
        ]
    )
    alert_event = pd.DataFrame(
        [
            {
                "event_id": "LGV1::10.0::20.0::20260309",
                "segment_id": "LGV1::10.0::20.0",
                "start_at": pd.Timestamp("2026-03-09"),
                "end_at": pd.Timestamp("2026-03-10"),
                "alert_type": "hybrid_lgv",
                "severity": "orange",
                "rule_triggered": "hybrid_threshold",
                "evidence_json": "{}",
            }
        ]
    )
    return {
        "communes": segments[
            ["commune_insee", "commune_name", "departement_code", "departement", "region", "pk_start", "pk_end"]
        ].rename(columns={"commune_insee": "insee"}),
        "segments": segments,
        "sample_points": pd.DataFrame(
            [
                {"segment_id": "LGV1::0.0::10.0", "segment_name": "Tours sud", "pk_start": 0.0, "pk_end": 10.0, "latitude": 47.31, "longitude": 0.58},
                {"segment_id": "LGV1::10.0::20.0", "segment_name": "Veigne", "pk_start": 10.0, "pk_end": 20.0, "latitude": 47.25, "longitude": 0.67},
            ]
        ),
        "stations": stations,
        "station_match": station_match,
        "segment_daily_rollup": segment_daily_rollup,
        "commune_daily_rollup": commune_daily_rollup,
        "alert_source": alert_source,
        "alert_event": alert_event,
    }

from __future__ import annotations

import pandas as pd

from lgv_pluvio.domain.alerts import AlertEngine


def test_compute_segment_alerts_assigns_expected_level() -> None:
    frame = pd.DataFrame(
        [
            {
                "segment_id": "A",
                "date": pd.Timestamp("2026-03-01"),
                "rain_1h_mm": 5,
                "rain_obs_mm": 50,
                "rain_72h_mm": 80,
                "rain_forecast_mm": 20,
                "nearest_hydro_station_id": "H1",
                "hydro_level_value": 2.5,
                "hydro_flow_value": 10,
                "official_vigilance_level": "yellow",
            }
        ]
    )
    thresholds = {
        "rain_1h": {"advisory": 10, "watch": 20, "alert": 30},
        "rain_24h": {"advisory": 20, "watch": 40, "alert": 60},
        "rain_72h": {"advisory": 40, "watch": 70, "alert": 100},
        "hydro_level": {"advisory": 2, "watch": 3, "alert": 4},
        "official_vigilance": {"green": 0, "yellow": 1, "orange": 2, "red": 3},
    }
    rollup, events = AlertEngine(thresholds).compute_segment_alerts(frame)
    assert rollup.iloc[0]["lgv_alert_level"] == "orange"
    assert len(events) == 1

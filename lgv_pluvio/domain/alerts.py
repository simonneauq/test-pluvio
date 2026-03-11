from __future__ import annotations

import pandas as pd


SEVERITY_SCORE = {"green": 0, "yellow": 1, "orange": 2, "red": 3}
SEVERITY_LABEL = {0: "green", 1: "yellow", 2: "orange", 3: "red"}


class AlertEngine:
    def __init__(self, thresholds: dict) -> None:
        self.thresholds = thresholds

    def _score_value(self, value: float | None, threshold_key: str) -> int:
        if value is None or pd.isna(value):
            return 0
        threshold = self.thresholds[threshold_key]
        if value >= threshold["alert"]:
            return 3
        if value >= threshold["watch"]:
            return 2
        if value >= threshold["advisory"]:
            return 1
        return 0

    def compute_segment_alerts(self, segment_daily_rollup: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        if segment_daily_rollup.empty:
            return segment_daily_rollup, pd.DataFrame(
                columns=["event_id", "segment_id", "start_at", "end_at", "alert_type", "severity", "rule_triggered", "evidence_json"]
            )

        frame = segment_daily_rollup.copy()
        rain_1h_score = frame["rain_1h_mm"].apply(lambda value: self._score_value(value, "rain_1h"))
        rain_24h_score = frame["rain_obs_mm"].apply(lambda value: self._score_value(value, "rain_24h"))
        rain_72h_score = frame["rain_72h_mm"].apply(lambda value: self._score_value(value, "rain_72h"))
        hydro_score = frame["hydro_level_value"].apply(lambda value: self._score_value(value, "hydro_level"))
        official_score = frame["official_vigilance_level"].fillna("green").map(SEVERITY_SCORE).fillna(0).astype(int)

        frame["lgv_alert_score"] = pd.concat(
            [rain_1h_score, rain_24h_score, rain_72h_score, hydro_score, official_score],
            axis=1,
        ).max(axis=1)
        frame["lgv_alert_level"] = frame["lgv_alert_score"].map(SEVERITY_LABEL)

        events = frame.loc[frame["lgv_alert_score"] > 0].copy()
        if events.empty:
            return frame, pd.DataFrame(
                columns=["event_id", "segment_id", "start_at", "end_at", "alert_type", "severity", "rule_triggered", "evidence_json"]
            )

        events["event_id"] = (
            events["segment_id"].astype(str)
            + "::"
            + pd.to_datetime(events["date"]).dt.strftime("%Y%m%d")
        )
        events["start_at"] = pd.to_datetime(events["date"])
        events["end_at"] = pd.to_datetime(events["date"]) + pd.Timedelta(days=1)
        events["alert_type"] = "hybrid_lgv"
        events["severity"] = events["lgv_alert_level"]
        events["rule_triggered"] = "hybrid_threshold"
        events["evidence_json"] = events[
            ["rain_1h_mm", "rain_obs_mm", "rain_72h_mm", "hydro_level_value", "official_vigilance_level"]
        ].astype(str).agg("|".join, axis=1)
        return frame, events[
            ["event_id", "segment_id", "start_at", "end_at", "alert_type", "severity", "rule_triggered", "evidence_json"]
        ]

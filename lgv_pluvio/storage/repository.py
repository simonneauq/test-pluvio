from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


class Repository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.execute("create schema if not exists core")
        self.conn.execute("create schema if not exists meta")
        self.conn.execute(
            """
            create table if not exists meta.sync_log (
                provider varchar,
                sync_type varchar,
                synced_at timestamp,
                status varchar,
                details varchar
            )
            """
        )

    def _replace(self, table_name: str, frame: pd.DataFrame) -> None:
        if frame is None:
            frame = pd.DataFrame()
        self.conn.register("tmp_frame", frame)
        self.conn.execute(f"create or replace table {table_name} as select * from tmp_frame")
        self.conn.unregister("tmp_frame")

    def table_exists(self, table_name: str) -> bool:
        query = """
            select count(*)
            from information_schema.tables
            where table_schema || '.' || table_name = ?
        """
        return bool(self.conn.execute(query, [table_name]).fetchone()[0])

    def log_sync(self, provider: str, sync_type: str, status: str, details: str = "") -> None:
        self.conn.execute(
            "insert into meta.sync_log values (?, ?, current_timestamp, ?, ?)",
            [provider, sync_type, status, details],
        )

    def upsert_communes(self, frame: pd.DataFrame) -> None:
        self._replace("core.commune_ref", frame)

    def upsert_lgv_segments(self, frame: pd.DataFrame) -> None:
        self._replace("core.lgv_segment", frame)

    def upsert_source_stations(self, frame: pd.DataFrame) -> None:
        self._replace("core.source_station", frame)

    def upsert_observations(self, frame: pd.DataFrame) -> None:
        self._replace("core.observation", frame)

    def upsert_forecasts(self, frame: pd.DataFrame) -> None:
        self._replace("core.forecast", frame)

    def upsert_station_matches(self, frame: pd.DataFrame) -> None:
        self._replace("core.station_match", frame)

    def upsert_alert_sources(self, frame: pd.DataFrame) -> None:
        self._replace("core.alert_source", frame)

    def upsert_segment_daily_rollup(self, frame: pd.DataFrame) -> None:
        self._replace("core.segment_daily_rollup", frame)

    def upsert_commune_daily_rollup(self, frame: pd.DataFrame) -> None:
        self._replace("core.commune_daily_rollup", frame)

    def upsert_alert_events(self, frame: pd.DataFrame) -> None:
        self._replace("core.alert_event", frame)

    def get_dashboard_dataset(self) -> dict[str, pd.DataFrame]:
        datasets = {}
        for table in [
            "core.segment_daily_rollup",
            "core.commune_daily_rollup",
            "core.source_station",
            "core.alert_event",
            "core.alert_source",
            "core.lgv_segment",
        ]:
            key = table.split(".")[-1]
            datasets[key] = self.conn.execute(f"select * from {table}").df() if self.table_exists(table) else pd.DataFrame()
        return datasets

    def get_pk_view(self) -> pd.DataFrame:
        if not self.table_exists("core.segment_daily_rollup"):
            return pd.DataFrame()
        return self.conn.execute(
            """
            select r.*, s.segment_name, s.pk_start, s.pk_end, s.line_code, s.commune_name, s.geometry_wkt
            from core.segment_daily_rollup r
            left join core.lgv_segment s using(segment_id)
            order by date desc, pk_start asc
            """
        ).df()

    def get_commune_view(self) -> pd.DataFrame:
        if not self.table_exists("core.commune_daily_rollup"):
            return pd.DataFrame()
        return self.conn.execute("select * from core.commune_daily_rollup order by date desc, rain_obs_mm desc").df()

    def get_station_view(self) -> pd.DataFrame:
        if not self.table_exists("core.source_station"):
            return pd.DataFrame()
        return self.conn.execute(
            """
            select s.*, m.segment_id, m.distance_m
            from core.source_station s
            left join core.station_match m using(station_id)
            order by provider, station_type, station_name
            """
        ).df()

    def get_history_view(self) -> pd.DataFrame:
        if not self.table_exists("core.segment_daily_rollup"):
            return pd.DataFrame()
        return self.conn.execute(
            """
            select date, segment_id, rain_obs_mm, rain_72h_mm, rain_forecast_mm, official_vigilance_level, lgv_alert_level
            from core.segment_daily_rollup
            order by date desc, rain_obs_mm desc
            """
        ).df()

    def get_sync_status(self) -> pd.DataFrame:
        if not self.table_exists("meta.sync_log"):
            return pd.DataFrame()
        return self.conn.execute(
            """
            select provider, sync_type, max(synced_at) as last_sync, any_value(status) as status, any_value(details) as details
            from meta.sync_log
            group by provider, sync_type
            order by provider, sync_type
            """
        ).df()

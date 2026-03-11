from __future__ import annotations

from pathlib import Path

import plotly.express as px
import streamlit as st

from lgv_pluvio.config import load_config
from lgv_pluvio.data_pipeline.refresh_service import run_refresh
from lgv_pluvio.sample_data import build_sample_bundle
from lgv_pluvio.storage.repository import Repository


SEVERITY_COLOR = {
    "green": [82, 152, 114],
    "yellow": [233, 194, 71],
    "orange": [232, 127, 52],
    "red": [191, 56, 43],
    "info": [74, 119, 179],
}


@st.cache_resource
def get_config():
    return load_config()


@st.cache_resource
def get_repository():
    return Repository(get_config().db_path)


def _save_uploaded_file(uploaded_file, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(uploaded_file.getbuffer())
    return target


def _render_reference_uploads() -> None:
    config = get_config()
    upload_dir = config.data_root / "uploads"

    st.sidebar.subheader("Referentiels cloud")
    communes_file = st.sidebar.file_uploader("communes_pk_lgv.xlsx", type=["xlsx"], key="communes_xlsx")
    lrs_pk_file = st.sidebar.file_uploader("LRS_PK.gpkg", type=["gpkg"], key="lrs_pk")
    lrs_axes_file = st.sidebar.file_uploader("LRS_AXES.gpkg (optionnel)", type=["gpkg"], key="lrs_axes")

    if communes_file is not None:
        saved = _save_uploaded_file(communes_file, upload_dir / "communes_pk_lgv.xlsx")
        config.raw["paths"]["communes_xlsx_path"] = str(saved)
    if lrs_pk_file is not None:
        saved = _save_uploaded_file(lrs_pk_file, upload_dir / "LRS_PK.gpkg")
        config.raw["paths"]["lrs_pk_path"] = str(saved)
    if lrs_axes_file is not None:
        saved = _save_uploaded_file(lrs_axes_file, upload_dir / "LRS_AXES.gpkg")
        config.raw["paths"]["lrs_axes_path"] = str(saved)

    refs_ready = bool(config.raw["paths"]["communes_xlsx_path"]) and bool(config.raw["paths"]["lrs_pk_path"])
    if refs_ready:
        st.sidebar.success("Referentiels charges pour cette session.")
    else:
        st.sidebar.info("Sans referentiels charges, l'application utilise le mode demonstration.")


def _ensure_seed_data() -> None:
    repository = get_repository()
    if repository.table_exists("core.segment_daily_rollup"):
        return
    sample = build_sample_bundle()
    repository.upsert_communes(sample["communes"])
    repository.upsert_lgv_segments(sample["segments"])
    repository.upsert_source_stations(sample["stations"])
    repository.upsert_station_matches(sample["station_match"])
    repository.upsert_segment_daily_rollup(sample["segment_daily_rollup"])
    repository.upsert_commune_daily_rollup(sample["commune_daily_rollup"])
    repository.upsert_alert_sources(sample["alert_source"])
    repository.upsert_alert_events(sample["alert_event"])


def render_app_shell() -> None:
    config = get_config()
    _render_reference_uploads()
    _ensure_seed_data()
    st.sidebar.title(config.title)
    st.sidebar.caption("Pilotage pluie, hydro et vigilance sur la LGV")
    if st.sidebar.button("Rafraichir maintenant", use_container_width=True):
        with st.spinner("Actualisation des donnees..."):
            result = run_refresh(config, mode="daily")
            if result.used_sample_data:
                st.sidebar.warning(result.message)
            else:
                st.sidebar.success(result.message)

    sync = get_repository().get_sync_status()
    st.sidebar.subheader("Synchronisations")
    if sync.empty:
        st.sidebar.info("Aucune synchronisation en base.")
    else:
        st.sidebar.dataframe(sync, use_container_width=True, hide_index=True)


def render_dashboard_page() -> None:
    repository = get_repository()
    data = repository.get_dashboard_dataset()
    segment_rollup = data["segment_daily_rollup"]
    commune_rollup = data["commune_daily_rollup"]
    stations = data["source_station"]
    alerts = data["alert_source"]

    st.title("Tableau de bord LGV")
    if segment_rollup.empty:
        st.info("Aucune donnee de pilotage disponible.")
        return

    latest_date = segment_rollup["date"].max()
    latest = segment_rollup[segment_rollup["date"] == latest_date]
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Date de reference", str(latest_date)[:10])
    col2.metric("Pluie max 24h", f"{latest['rain_obs_mm'].max():.1f} mm")
    col3.metric("Pluie max 72h", f"{latest['rain_72h_mm'].max():.1f} mm")
    col4.metric("Segments en alerte", int(latest["lgv_alert_level"].isin(["yellow", "orange", "red"]).sum()))
    col5.metric("Stations hydro", int((stations["station_type"] == "hydro").sum()) if not stations.empty else 0)

    left, right = st.columns([1.5, 1])
    with left:
        st.subheader("Profil pluie")
        plot = latest.sort_values("pk_start") if "pk_start" in latest.columns else latest
        if "pk_start" not in plot.columns:
            plot = plot.merge(data["lgv_segment"][["segment_id", "pk_start", "pk_end"]], on="segment_id", how="left")
        fig = px.bar(plot.sort_values("pk_start"), x="pk_start", y="rain_obs_mm", color="lgv_alert_level", hover_data=["segment_id", "pk_end"])
        st.plotly_chart(fig, use_container_width=True)
    with right:
        st.subheader("Vigilance officielle")
        if alerts.empty:
            st.info("Aucune vigilance officielle.")
        else:
            st.dataframe(alerts, use_container_width=True, hide_index=True)

    st.subheader("Top communes")
    if commune_rollup.empty:
        st.info("Aucune aggregation communale.")
    else:
        top = commune_rollup[commune_rollup["date"] == commune_rollup["date"].max()].sort_values("rain_obs_mm", ascending=False).head(10)
        st.dataframe(top, use_container_width=True, hide_index=True)

    st.subheader("Tendance pluie")
    trend = segment_rollup.groupby("date", as_index=False).agg(rain_obs_mm=("rain_obs_mm", "max"), rain_forecast_mm=("rain_forecast_mm", "max"))
    fig = px.line(trend.sort_values("date"), x="date", y=["rain_obs_mm", "rain_forecast_mm"], markers=True)
    st.plotly_chart(fig, use_container_width=True)

from __future__ import annotations

import plotly.express as px
import streamlit as st

from lgv_pluvio.ui.common import get_repository, render_app_shell


st.set_page_config(page_title="Vue PK", layout="wide")
render_app_shell()
st.title("Vue lineaire PK")

frame = get_repository().get_pk_view()
if frame.empty:
    st.info("Aucune donnee PK disponible.")
    st.stop()

segment_ids = sorted(frame["segment_id"].dropna().unique().tolist())
selected = st.sidebar.selectbox("Segment", options=segment_ids)
local = frame[frame["segment_id"] == selected].copy()

col1, col2 = st.columns([1.2, 1])
with col1:
    fig = px.bar(local.sort_values("date"), x="date", y=["rain_obs_mm", "rain_forecast_mm", "rain_72h_mm"], barmode="group", title="Historique pluie")
    st.plotly_chart(fig, use_container_width=True)
with col2:
    fig = px.line(local.sort_values("date"), x="date", y=["hydro_level_value", "hydro_flow_value"], markers=True, title="Contexte hydro")
    st.plotly_chart(fig, use_container_width=True)

st.dataframe(local.sort_values("date", ascending=False), use_container_width=True, hide_index=True)

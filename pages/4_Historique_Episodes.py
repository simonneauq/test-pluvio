from __future__ import annotations

import plotly.express as px
import streamlit as st

from lgv_pluvio.ui.common import get_repository, render_app_shell


st.set_page_config(page_title="Historique", layout="wide")
render_app_shell()
st.title("Historique et episodes")

frame = get_repository().get_history_view()
if frame.empty:
    st.info("Aucun historique disponible.")
    st.stop()

segments = sorted(frame["segment_id"].dropna().unique().tolist())
selected = st.sidebar.multiselect("Segments", segments, default=segments[: min(5, len(segments))])
filtered = frame[frame["segment_id"].isin(selected)]

fig = px.line(filtered.sort_values("date"), x="date", y="rain_obs_mm", color="segment_id", line_dash="lgv_alert_level", title="Comparaison d'episodes pluvieux")
st.plotly_chart(fig, use_container_width=True)
st.dataframe(filtered.sort_values(["date", "rain_obs_mm"], ascending=[False, False]), use_container_width=True, hide_index=True)

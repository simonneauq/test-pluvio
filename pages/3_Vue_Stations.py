from __future__ import annotations

import plotly.express as px
import streamlit as st

from lgv_pluvio.ui.common import get_repository, render_app_shell


st.set_page_config(page_title="Vue stations", layout="wide")
render_app_shell()
st.title("Vue stations sources")

frame = get_repository().get_station_view()
if frame.empty:
    st.info("Aucune station en base.")
    st.stop()

providers = sorted(frame["provider"].dropna().unique().tolist())
selected = st.sidebar.multiselect("Provider", providers, default=providers)
filtered = frame[frame["provider"].isin(selected)]

fig = px.scatter_map(filtered.dropna(subset=["latitude", "longitude"]), lat="latitude", lon="longitude", color="provider", hover_name="station_name", hover_data=["station_type", "segment_id", "distance_m"], zoom=5, height=550)
st.plotly_chart(fig, use_container_width=True)
st.dataframe(filtered, use_container_width=True, hide_index=True)

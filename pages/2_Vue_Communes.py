from __future__ import annotations

import plotly.express as px
import streamlit as st

from lgv_pluvio.ui.common import get_repository, render_app_shell


st.set_page_config(page_title="Vue communes", layout="wide")
render_app_shell()
st.title("Vue communes")

frame = get_repository().get_commune_view()
if frame.empty:
    st.info("Aucune donnee commune disponible.")
    st.stop()

departements = sorted(frame["departement_code"].dropna().astype(str).unique().tolist())
selected = st.sidebar.multiselect("Departement", departements, default=departements)
filtered = frame[frame["departement_code"].astype(str).isin(selected)]

fig = px.scatter(filtered, x="date", y="rain_obs_mm", color="lgv_alert_level", size="rain_72h_mm", hover_name="commune_name", title="Criticite par commune")
st.plotly_chart(fig, use_container_width=True)
st.dataframe(filtered.sort_values(["date", "rain_obs_mm"], ascending=[False, False]), use_container_width=True, hide_index=True)

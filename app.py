from __future__ import annotations

import streamlit as st

from lgv_pluvio.ui.common import render_app_shell, render_dashboard_page


def main() -> None:
    st.set_page_config(
        page_title="Suivi pluviometrie LGV",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    render_app_shell()
    render_dashboard_page()


if __name__ == "__main__":
    main()

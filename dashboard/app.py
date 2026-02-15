"""Halifax Transit Tracker — Streamlit dashboard entry point."""

import streamlit as st

from lib.db import get_connection
from lib.queries import get_last_poll

st.set_page_config(
    page_title="Halifax Transit Tracker",
    page_icon="🚌",
    layout="wide",
)

# ── Sidebar ──────────────────────────────────────────────────────────
with st.sidebar:
    st.title("Halifax Transit Tracker")
    st.caption("On-time = between 1 min early and 5 min late")

    conn = get_connection()
    last_poll = get_last_poll(conn)
    if not last_poll.empty:
        ts = last_poll.iloc[0]["polled_at"]
        st.metric("Last poll", str(ts)[:19])
    else:
        st.info("No poll data yet.")

# ── Main content ─────────────────────────────────────────────────────
home = st.Page("pages/1_Route_Overview.py", title="Route Overview", icon="📊", default=True)
hourly = st.Page("pages/2_Hourly_Patterns.py", title="Hourly Patterns", icon="🕐")
delays = st.Page("pages/3_Delay_Distribution.py", title="Delay Distribution", icon="📈")
stop_map = st.Page("pages/4_Stop_Map.py", title="Stop Map", icon="🗺️")
health = st.Page("pages/5_System_Health.py", title="System Health", icon="💚")

pg = st.navigation([home, hourly, delays, stop_map, health])
pg.run()

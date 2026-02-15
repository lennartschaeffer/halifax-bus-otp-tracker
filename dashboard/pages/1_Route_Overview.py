"""Page 1: Route Overview — Daily OTP scorecard across all routes."""

from datetime import timedelta

import plotly.express as px
import streamlit as st

from lib.db import get_connection
from lib.queries import get_daily_summary, get_date_range, get_routes

conn = get_connection()

st.header("Route Overview")

min_date, max_date = get_date_range(conn)

with st.sidebar:
    st.subheader("Filters")

    date_col1, date_col2 = st.columns(2)
    with date_col1:
        start_date = st.date_input(
            "Start date",
            value=max(min_date, max_date - timedelta(days=7)),
            min_value=min_date,
            max_value=max_date,
        )
    with date_col2:
        end_date = st.date_input(
            "End date",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
        )

    routes_df = get_routes(conn)
    route_options = dict(
        zip(routes_df["route_short_name"], routes_df["route_id"])
    )
    selected_names = st.multiselect(
        "Routes",
        options=list(route_options.keys()),
        placeholder="All routes",
    )
    selected_route_ids = [route_options[n] for n in selected_names] if selected_names else None

df = get_daily_summary(conn, start_date, end_date, selected_route_ids)

if df.empty:
    st.warning("No data available for the selected filters.")
    st.stop()

fleet_otp = (
    df["on_time_percentage"].mul(df["total_observations"]).sum()
    / df["total_observations"].sum()
)
fleet_avg_delay = (
    df["avg_delay_seconds"].mul(df["total_observations"]).sum()
    / df["total_observations"].sum()
)
total_obs = df["total_observations"].sum()

kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric("Fleet OTP", f"{fleet_otp:.1f}%")
kpi2.metric("Avg Delay", f"{fleet_avg_delay:.0f}s")
kpi3.metric("Observations", f"{total_obs:,}")

st.subheader("OTP Trend")

fig_trend = px.line(
    df,
    x="service_date",
    y="on_time_percentage",
    color="route_short_name",
    labels={
        "service_date": "Date",
        "on_time_percentage": "On-Time %",
        "route_short_name": "Route",
    },
    markers=True,
)
fig_trend.update_layout(
    yaxis_range=[0, 100],
    hovermode="x unified",
    legend_title_text="Route",
    height=400,
)
st.plotly_chart(fig_trend, use_container_width=True)

st.subheader("Route Leaderboard")

leaderboard = (
    df.groupby(["route_id", "route_short_name", "route_long_name"])
    .agg(
        otp_pct=("on_time_percentage", "mean"),
        avg_delay=("avg_delay_seconds", "mean"),
        p95_delay=("p95_delay_seconds", "mean"),
        trips=("unique_trips", "sum"),
        observations=("total_observations", "sum"),
    )
    .reset_index()
    .sort_values("otp_pct", ascending=False)
)

st.dataframe(
    leaderboard[["route_short_name", "route_long_name", "otp_pct", "avg_delay", "p95_delay", "trips", "observations"]]
    .rename(columns={
        "route_short_name": "Route",
        "route_long_name": "Route Name",
        "otp_pct": "OTP %",
        "avg_delay": "Avg Delay (s)",
        "p95_delay": "P95 Delay (s)",
        "trips": "Trips",
        "observations": "Observations",
    })
    .style.format({
        "OTP %": "{:.1f}",
        "Avg Delay (s)": "{:.0f}",
        "P95 Delay (s)": "{:.0f}",
        "Trips": "{:,.0f}",
        "Observations": "{:,.0f}",
    }),
    use_container_width=True,
    hide_index=True,
)

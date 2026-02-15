"""Dashboard-specific SQL queries returning DataFrames."""

from datetime import date

import duckdb
import pandas as pd
import streamlit as st


@st.cache_data(ttl=300)
def get_routes(_conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return all routes for filter dropdowns."""
    return _conn.execute(
        "SELECT route_id, route_short_name, route_long_name FROM gtfs_routes ORDER BY route_short_name"
    ).fetchdf()


@st.cache_data(ttl=300)
def get_date_range(_conn: duckdb.DuckDBPyConnection) -> tuple[date, date]:
    """Return the min and max service_date available in daily summaries."""
    row = _conn.execute(
        "SELECT MIN(service_date), MAX(service_date) FROM daily_route_summary"
    ).fetchone()
    if row and row[0]:
        return row[0], row[1]
    today = date.today()
    return today, today


@st.cache_data(ttl=300)
def get_daily_summary(
    _conn: duckdb.DuckDBPyConnection,
    start_date: date,
    end_date: date,
    route_ids: list[str] | None = None,
) -> pd.DataFrame:
    """Fetch daily route summary data for the given date range and routes."""
    query = """
        SELECT
            d.service_date,
            d.route_id,
            r.route_short_name,
            r.route_long_name,
            d.on_time_percentage,
            d.avg_delay_seconds,
            d.p95_delay_seconds,
            d.total_observations,
            d.unique_trips
        FROM daily_route_summary d
        JOIN gtfs_routes r USING (route_id)
        WHERE d.service_date BETWEEN ? AND ?
    """
    params: list = [start_date, end_date]

    if route_ids:
        placeholders = ", ".join(["?"] * len(route_ids))
        query += f" AND d.route_id IN ({placeholders})"
        params.extend(route_ids)

    query += " ORDER BY d.service_date, d.route_id"
    return _conn.execute(query, params).fetchdf()


@st.cache_data(ttl=300)
def get_hourly_summary(
    _conn: duckdb.DuckDBPyConnection,
    start_date: date,
    end_date: date,
    route_ids: list[str] | None = None,
) -> pd.DataFrame:
    """Fetch hourly route summary aggregated across the date range."""
    query = """
        SELECT
            h.route_id,
            r.route_short_name,
            h.hour_of_day,
            AVG(h.on_time_percentage) AS avg_otp,
            AVG(h.avg_delay_seconds) AS avg_delay,
            SUM(h.total_observations) AS total_obs
        FROM hourly_route_summary h
        JOIN gtfs_routes r USING (route_id)
        WHERE h.service_date BETWEEN ? AND ?
    """
    params: list = [start_date, end_date]

    if route_ids:
        placeholders = ", ".join(["?"] * len(route_ids))
        query += f" AND h.route_id IN ({placeholders})"
        params.extend(route_ids)

    query += " GROUP BY h.route_id, r.route_short_name, h.hour_of_day"
    query += " ORDER BY h.route_id, h.hour_of_day"
    return _conn.execute(query, params).fetchdf()


@st.cache_data(ttl=300)
def get_last_poll(_conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Fetch the most recent poll log entry."""
    return _conn.execute(
        "SELECT * FROM poll_log ORDER BY polled_at DESC LIMIT 1"
    ).fetchdf()

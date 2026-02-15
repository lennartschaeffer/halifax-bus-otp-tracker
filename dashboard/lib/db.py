"""Cached DuckDB connection for the Streamlit dashboard."""

from pathlib import Path

import duckdb
import streamlit as st

DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "transit.duckdb"


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """Return a read-only DuckDB connection, cached across reruns."""
    return duckdb.connect(str(DB_PATH), read_only=True)

"""DuckDB connection management."""

import duckdb

from src.config import DB_PATH


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Get a connection to the DuckDB database.

    Args:
        read_only: If True, open the database in read-only mode.

    Returns:
        A DuckDB connection object.
    """
    return duckdb.connect(str(DB_PATH), read_only=read_only)

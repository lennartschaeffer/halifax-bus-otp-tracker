"""Database schema creation for DuckDB."""

import duckdb

from .connection import get_connection


def create_tables(conn: duckdb.DuckDBPyConnection | None = None) -> None:
    """Create all database tables if they don't exist.

    Args:
        conn: Optional existing connection. If None, creates a new one.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    try:
        # Core fact table for delay events
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stop_delay_events (
                observed_at         TIMESTAMP NOT NULL,
                trip_id             VARCHAR NOT NULL,
                stop_id             VARCHAR NOT NULL,
                stop_sequence       INTEGER NOT NULL,
                service_date        DATE NOT NULL,
                route_id            VARCHAR NOT NULL,
                direction_id        TINYINT,
                vehicle_id          VARCHAR,
                arrival_delay       INTEGER,
                departure_delay     INTEGER,
                predicted_arrival   TIMESTAMP,
                predicted_departure TIMESTAMP,
                feed_timestamp      TIMESTAMP NOT NULL,
                hour_of_day         TINYINT NOT NULL,
                day_of_week         TINYINT NOT NULL,
                is_on_time          BOOLEAN,
                PRIMARY KEY (trip_id, stop_id, stop_sequence, service_date)
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_delay_route_date
            ON stop_delay_events (route_id, service_date)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_delay_stop_date
            ON stop_delay_events (stop_id, service_date)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_delay_hour
            ON stop_delay_events (route_id, hour_of_day)
        """)

        # Daily route summary table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_route_summary (
                service_date        DATE NOT NULL,
                route_id            VARCHAR NOT NULL,
                total_observations  INTEGER NOT NULL,
                on_time_count       INTEGER NOT NULL,
                early_count         INTEGER NOT NULL,
                late_count          INTEGER NOT NULL,
                avg_delay_seconds   FLOAT,
                median_delay_seconds FLOAT,
                p95_delay_seconds   FLOAT,
                max_delay_seconds   INTEGER,
                min_delay_seconds   INTEGER,
                on_time_percentage  FLOAT NOT NULL,
                unique_trips        INTEGER,
                unique_vehicles     INTEGER,
                unique_stops        INTEGER,
                PRIMARY KEY (service_date, route_id)
            )
        """)

        # Hourly route summary table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS hourly_route_summary (
                service_date        DATE NOT NULL,
                route_id            VARCHAR NOT NULL,
                hour_of_day         TINYINT NOT NULL,
                total_observations  INTEGER NOT NULL,
                on_time_count       INTEGER NOT NULL,
                avg_delay_seconds   FLOAT,
                on_time_percentage  FLOAT NOT NULL,
                PRIMARY KEY (service_date, route_id, hour_of_day)
            )
        """)

        # Poll log for health monitoring
        conn.execute("""
            CREATE TABLE IF NOT EXISTS poll_log (
                poll_id                 INTEGER PRIMARY KEY,
                polled_at               TIMESTAMP NOT NULL,
                trip_updates_count      INTEGER,
                fetch_duration_ms       INTEGER,
                process_duration_ms     INTEGER,
                error_message           VARCHAR,
                trip_feed_timestamp     TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS poll_log_seq START 1
        """)

        # Static GTFS reference tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gtfs_routes (
                route_id            VARCHAR PRIMARY KEY,
                route_short_name    VARCHAR,
                route_long_name     VARCHAR,
                route_type          INTEGER
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS gtfs_stops (
                stop_id             VARCHAR PRIMARY KEY,
                stop_name           VARCHAR,
                stop_lat            DOUBLE,
                stop_lon            DOUBLE
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS gtfs_trips (
                trip_id             VARCHAR PRIMARY KEY,
                route_id            VARCHAR,
                service_id          VARCHAR,
                trip_headsign       VARCHAR,
                direction_id        TINYINT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS gtfs_stop_times (
                trip_id             VARCHAR NOT NULL,
                stop_sequence       INTEGER NOT NULL,
                stop_id             VARCHAR NOT NULL,
                arrival_time        VARCHAR,
                departure_time      VARCHAR,
                PRIMARY KEY (trip_id, stop_sequence)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS gtfs_calendar (
                service_id          VARCHAR PRIMARY KEY,
                monday              BOOLEAN,
                tuesday             BOOLEAN,
                wednesday           BOOLEAN,
                thursday            BOOLEAN,
                friday              BOOLEAN,
                saturday            BOOLEAN,
                sunday              BOOLEAN,
                start_date          DATE,
                end_date            DATE
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS gtfs_calendar_dates (
                service_id          VARCHAR NOT NULL,
                date                DATE NOT NULL,
                exception_type      INTEGER,
                PRIMARY KEY (service_id, date)
            )
        """)

    finally:
        if should_close:
            conn.close()

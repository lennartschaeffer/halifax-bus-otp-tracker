"""Common database query functions."""

from datetime import datetime

import duckdb

from src.models import (
    PollLogRecord,
    StopDelayEvent,
)


def insert_stop_delay_events(
    conn: duckdb.DuckDBPyConnection,
    events: list[StopDelayEvent],
) -> int:
    """Upsert stop delay events into the database.

    Args:
        conn: Database connection.
        events: List of StopDelayEvent records to insert.

    Returns:
        Number of rows inserted.
    """
    if not events:
        return 0

    values = [
        (
            e.observed_at,
            e.trip_id,
            e.stop_id,
            e.stop_sequence,
            e.service_date,
            e.route_id,
            e.direction_id,
            e.vehicle_id,
            e.arrival_delay,
            e.departure_delay,
            e.predicted_arrival,
            e.predicted_departure,
            e.feed_timestamp,
            e.hour_of_day,
            e.day_of_week,
            e.is_on_time,
        )
        for e in events
    ]

    conn.executemany(
        """
        INSERT OR REPLACE INTO stop_delay_events (
            observed_at, trip_id, stop_id, stop_sequence, service_date,
            route_id, direction_id, vehicle_id, arrival_delay, departure_delay,
            predicted_arrival, predicted_departure, feed_timestamp,
            hour_of_day, day_of_week, is_on_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        values,
    )

    return len(events)


def log_poll(
    conn: duckdb.DuckDBPyConnection,
    record: PollLogRecord,
) -> None:
    """Log a poll cycle for health monitoring.

    Args:
        conn: Database connection.
        record: PollLogRecord with poll statistics.
    """
    conn.execute(
        """
        INSERT INTO poll_log (
            poll_id, polled_at, trip_updates_count,
            fetch_duration_ms, process_duration_ms,
            error_message, trip_feed_timestamp
        ) VALUES (
            nextval('poll_log_seq'), ?, ?, ?, ?, ?, ?
        )
        """,
        [
            record.polled_at,
            record.trip_updates_count,
            record.fetch_duration_ms,
            record.process_duration_ms,
            record.error_message,
            record.trip_feed_timestamp,
        ],
    )


def get_route_otp(
    conn: duckdb.DuckDBPyConnection,
    route_id: str,
    start_date: datetime,
    end_date: datetime,
) -> dict:
    """Get on-time performance statistics for a route.

    Args:
        conn: Database connection.
        route_id: Route ID to query.
        start_date: Start of date range.
        end_date: End of date range.

    Returns:
        Dictionary with OTP statistics.
    """
    result = conn.execute(
        """
        SELECT
            COUNT(*) as total_observations,
            SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) as on_time_count,
            AVG(arrival_delay) as avg_delay,
            MEDIAN(arrival_delay) as median_delay,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY arrival_delay) as p95_delay
        FROM stop_delay_events
        WHERE route_id = ?
          AND service_date BETWEEN ? AND ?
        """,
        [route_id, start_date.date(), end_date.date()],
    ).fetchone()

    if result and result[0] > 0:
        return {
            "route_id": route_id,
            "total_observations": result[0],
            "on_time_count": result[1],
            "on_time_percentage": (result[1] / result[0]) * 100,
            "avg_delay_seconds": result[2],
            "median_delay_seconds": result[3],
            "p95_delay_seconds": result[4],
        }

    return {
        "route_id": route_id,
        "total_observations": 0,
        "on_time_count": 0,
        "on_time_percentage": 0.0,
        "avg_delay_seconds": None,
        "median_delay_seconds": None,
        "p95_delay_seconds": None,
    }

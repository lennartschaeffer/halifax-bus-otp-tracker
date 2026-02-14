"""Daily aggregation job for summarizing delay events."""

import logging
from datetime import date, timedelta

import duckdb

from src.config import EARLY_THRESHOLD, LATE_THRESHOLD
from src.db.connection import get_connection

logger = logging.getLogger(__name__)


def aggregate_daily_route_summary(
    conn: duckdb.DuckDBPyConnection,
    service_date: date,
) -> int:
    """Aggregate delay events into daily route summaries.

    Args:
        conn: Database connection.
        service_date: Date to aggregate.

    Returns:
        Number of route summaries created.
    """
    # Delete existing summaries for this date
    conn.execute(
        "DELETE FROM daily_route_summary WHERE service_date = ?",
        [service_date],
    )

    # Insert aggregated data
    result = conn.execute(
        f"""
        INSERT INTO daily_route_summary (
            service_date, route_id,
            total_observations, on_time_count, early_count, late_count,
            avg_delay_seconds, median_delay_seconds, p95_delay_seconds,
            max_delay_seconds, min_delay_seconds,
            on_time_percentage, unique_trips, unique_vehicles, unique_stops
        )
        SELECT
            service_date,
            route_id,
            COUNT(*) as total_observations,
            SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) as on_time_count,
            SUM(CASE WHEN arrival_delay < {EARLY_THRESHOLD} THEN 1 ELSE 0 END) as early_count,
            SUM(CASE WHEN arrival_delay > {LATE_THRESHOLD} THEN 1 ELSE 0 END) as late_count,
            AVG(arrival_delay) as avg_delay_seconds,
            MEDIAN(arrival_delay) as median_delay_seconds,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY arrival_delay) as p95_delay_seconds,
            MAX(arrival_delay) as max_delay_seconds,
            MIN(arrival_delay) as min_delay_seconds,
            (SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100 as on_time_percentage,
            COUNT(DISTINCT trip_id) as unique_trips,
            COUNT(DISTINCT vehicle_id) as unique_vehicles,
            COUNT(DISTINCT stop_id) as unique_stops
        FROM stop_delay_events
        WHERE service_date = ?
          AND arrival_delay IS NOT NULL
        GROUP BY service_date, route_id
        """,
        [service_date],
    )

    # Get count of inserted rows
    count = conn.execute(
        "SELECT COUNT(*) FROM daily_route_summary WHERE service_date = ?",
        [service_date],
    ).fetchone()[0]

    return count


def aggregate_hourly_route_summary(
    conn: duckdb.DuckDBPyConnection,
    service_date: date,
) -> int:
    """Aggregate delay events into hourly route summaries.

    Args:
        conn: Database connection.
        service_date: Date to aggregate.

    Returns:
        Number of hourly summaries created.
    """
    # Delete existing summaries for this date
    conn.execute(
        "DELETE FROM hourly_route_summary WHERE service_date = ?",
        [service_date],
    )

    # Insert aggregated data
    conn.execute(
        """
        INSERT INTO hourly_route_summary (
            service_date, route_id, hour_of_day,
            total_observations, on_time_count, avg_delay_seconds, on_time_percentage
        )
        SELECT
            service_date,
            route_id,
            hour_of_day,
            COUNT(*) as total_observations,
            SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) as on_time_count,
            AVG(arrival_delay) as avg_delay_seconds,
            (SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100 as on_time_percentage
        FROM stop_delay_events
        WHERE service_date = ?
          AND arrival_delay IS NOT NULL
        GROUP BY service_date, route_id, hour_of_day
        """,
        [service_date],
    )

    # Get count of inserted rows
    count = conn.execute(
        "SELECT COUNT(*) FROM hourly_route_summary WHERE service_date = ?",
        [service_date],
    ).fetchone()[0]

    return count


def run_daily_aggregation(
    target_date: date | None = None,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, int]:
    """Run daily aggregation for a specific date.

    Args:
        target_date: Date to aggregate. Defaults to yesterday.
        conn: Optional existing connection. If None, creates a new one.

    Returns:
        Dictionary with counts of created summaries.
    """
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    should_close = conn is None
    if conn is None:
        conn = get_connection()

    try:
        logger.info(f"Running daily aggregation for {target_date}")

        daily_count = aggregate_daily_route_summary(conn, target_date)
        logger.info(f"Created {daily_count} daily route summaries")

        hourly_count = aggregate_hourly_route_summary(conn, target_date)
        logger.info(f"Created {hourly_count} hourly route summaries")

        return {
            "date": str(target_date),
            "daily_route_summaries": daily_count,
            "hourly_route_summaries": hourly_count,
        }

    finally:
        if should_close:
            conn.close()


def backfill_aggregations(
    start_date: date,
    end_date: date,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> list[dict]:
    """Backfill aggregations for a date range.

    Args:
        start_date: Start of date range (inclusive).
        end_date: End of date range (inclusive).
        conn: Optional existing connection. If None, creates a new one.

    Returns:
        List of result dictionaries for each date.
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    results = []
    current_date = start_date

    try:
        while current_date <= end_date:
            result = run_daily_aggregation(current_date, conn)
            results.append(result)
            current_date += timedelta(days=1)

        return results

    finally:
        if should_close:
            conn.close()

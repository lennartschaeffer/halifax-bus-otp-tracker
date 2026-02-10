"""Load static GTFS data into DuckDB reference tables."""

import csv
import io
import zipfile
from pathlib import Path

import duckdb
import requests

from src.config import GTFS_DIR, STATIC_GTFS_URL


def _convert_gtfs_date(date_str: str | None) -> str | None:
    """Convert GTFS date format (YYYYMMDD) to ISO format (YYYY-MM-DD)."""
    if not date_str or len(date_str) != 8:
        return None
    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"


def download_static_gtfs(output_dir: Path | None = None) -> Path:
    """Download the static GTFS zip file from Halifax Transit.

    Args:
        output_dir: Directory to extract files to. Defaults to GTFS_DIR.

    Returns:
        Path to the extracted GTFS directory.
    """
    if output_dir is None:
        output_dir = GTFS_DIR

    output_dir.mkdir(parents=True, exist_ok=True)

    response = requests.get(STATIC_GTFS_URL, timeout=60)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        zf.extractall(output_dir)

    return output_dir


def load_static_gtfs(
    conn: duckdb.DuckDBPyConnection,
    gtfs_dir: Path | None = None,
) -> dict[str, int]:
    """Load static GTFS data from text files into DuckDB.

    Args:
        conn: Database connection.
        gtfs_dir: Directory containing GTFS text files. Defaults to GTFS_DIR.

    Returns:
        Dictionary with counts of loaded records per table.
    """
    if gtfs_dir is None:
        gtfs_dir = GTFS_DIR

    counts = {}

    # Load routes.txt
    routes_file = gtfs_dir / "routes.txt"
    if routes_file.exists():
        conn.execute("DELETE FROM gtfs_routes")
        with open(routes_file, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = [
                (
                    row["route_id"],
                    row.get("route_short_name"),
                    row.get("route_long_name"),
                    int(row["route_type"]) if row.get("route_type") else None,
                )
                for row in reader
            ]
            conn.executemany(
                """
                INSERT INTO gtfs_routes (route_id, route_short_name, route_long_name, route_type)
                VALUES (?, ?, ?, ?)
                """,
                rows,
            )
            counts["routes"] = len(rows)

    # Load stops.txt
    stops_file = gtfs_dir / "stops.txt"
    if stops_file.exists():
        conn.execute("DELETE FROM gtfs_stops")
        with open(stops_file, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = [
                (
                    row["stop_id"],
                    row.get("stop_name"),
                    float(row["stop_lat"]) if row.get("stop_lat") else None,
                    float(row["stop_lon"]) if row.get("stop_lon") else None,
                )
                for row in reader
            ]
            conn.executemany(
                """
                INSERT INTO gtfs_stops (stop_id, stop_name, stop_lat, stop_lon)
                VALUES (?, ?, ?, ?)
                """,
                rows,
            )
            counts["stops"] = len(rows)

    # Load trips.txt
    trips_file = gtfs_dir / "trips.txt"
    if trips_file.exists():
        conn.execute("DELETE FROM gtfs_trips")
        with open(trips_file, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = [
                (
                    row["trip_id"],
                    row.get("route_id"),
                    row.get("service_id"),
                    row.get("trip_headsign"),
                    int(row["direction_id"]) if row.get("direction_id") else None,
                )
                for row in reader
            ]
            conn.executemany(
                """
                INSERT INTO gtfs_trips (trip_id, route_id, service_id, trip_headsign, direction_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )
            counts["trips"] = len(rows)

    # Load stop_times.txt
    stop_times_file = gtfs_dir / "stop_times.txt"
    if stop_times_file.exists():
        conn.execute("DELETE FROM gtfs_stop_times")
        with open(stop_times_file, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = [
                (
                    row["trip_id"],
                    int(row["stop_sequence"]),
                    row["stop_id"],
                    row.get("arrival_time"),
                    row.get("departure_time"),
                )
                for row in reader
            ]
            conn.executemany(
                """
                INSERT INTO gtfs_stop_times (trip_id, stop_sequence, stop_id, arrival_time, departure_time)
                VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )
            counts["stop_times"] = len(rows)

    # Load calendar.txt
    calendar_file = gtfs_dir / "calendar.txt"
    if calendar_file.exists():
        conn.execute("DELETE FROM gtfs_calendar")
        with open(calendar_file, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = [
                (
                    row["service_id"],
                    row.get("monday") == "1",
                    row.get("tuesday") == "1",
                    row.get("wednesday") == "1",
                    row.get("thursday") == "1",
                    row.get("friday") == "1",
                    row.get("saturday") == "1",
                    row.get("sunday") == "1",
                    _convert_gtfs_date(row.get("start_date")),
                    _convert_gtfs_date(row.get("end_date")),
                )
                for row in reader
            ]
            conn.executemany(
                """
                INSERT INTO gtfs_calendar (
                    service_id, monday, tuesday, wednesday, thursday,
                    friday, saturday, sunday, start_date, end_date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            counts["calendar"] = len(rows)

    # Load calendar_dates.txt
    calendar_dates_file = gtfs_dir / "calendar_dates.txt"
    if calendar_dates_file.exists():
        conn.execute("DELETE FROM gtfs_calendar_dates")
        with open(calendar_dates_file, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = [
                (
                    row["service_id"],
                    _convert_gtfs_date(row["date"]),
                    int(row["exception_type"]) if row.get("exception_type") else None,
                )
                for row in reader
            ]
            conn.executemany(
                """
                INSERT INTO gtfs_calendar_dates (service_id, date, exception_type)
                VALUES (?, ?, ?)
                """,
                rows,
            )
            counts["calendar_dates"] = len(rows)

    return counts

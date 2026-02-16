"""Load static GTFS data into DuckDB reference tables."""

import io
import zipfile
from pathlib import Path

import duckdb
import requests

from src.config import GTFS_DIR, STATIC_GTFS_URL


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
        conn.execute(
            """
            INSERT INTO gtfs_routes (route_id, route_short_name, route_long_name, route_type)
            SELECT route_id, route_short_name, route_long_name, CAST(route_type AS INTEGER)
            FROM read_csv_auto(?, header=true, all_varchar=true)
            """,
            [str(routes_file)],
        )
        counts["routes"] = conn.execute("SELECT count(*) FROM gtfs_routes").fetchone()[0]

    # Load stops.txt
    stops_file = gtfs_dir / "stops.txt"
    if stops_file.exists():
        conn.execute("DELETE FROM gtfs_stops")
        conn.execute(
            """
            INSERT INTO gtfs_stops (stop_id, stop_name, stop_lat, stop_lon)
            SELECT stop_id, stop_name, CAST(stop_lat AS DOUBLE), CAST(stop_lon AS DOUBLE)
            FROM read_csv_auto(?, header=true, all_varchar=true)
            """,
            [str(stops_file)],
        )
        counts["stops"] = conn.execute("SELECT count(*) FROM gtfs_stops").fetchone()[0]

    # Load trips.txt
    trips_file = gtfs_dir / "trips.txt"
    if trips_file.exists():
        conn.execute("DELETE FROM gtfs_trips")
        conn.execute(
            """
            INSERT INTO gtfs_trips (trip_id, route_id, service_id, trip_headsign, direction_id)
            SELECT trip_id, route_id, service_id, trip_headsign, CAST(direction_id AS TINYINT)
            FROM read_csv_auto(?, header=true, all_varchar=true)
            """,
            [str(trips_file)],
        )
        counts["trips"] = conn.execute("SELECT count(*) FROM gtfs_trips").fetchone()[0]

    # Load stop_times.txt
    stop_times_file = gtfs_dir / "stop_times.txt"
    if stop_times_file.exists():
        conn.execute("DELETE FROM gtfs_stop_times")
        conn.execute(
            """
            INSERT INTO gtfs_stop_times (trip_id, stop_sequence, stop_id, arrival_time, departure_time)
            SELECT trip_id, CAST(stop_sequence AS INTEGER), stop_id, arrival_time, departure_time
            FROM read_csv_auto(?, header=true, all_varchar=true)
            """,
            [str(stop_times_file)],
        )
        counts["stop_times"] = conn.execute("SELECT count(*) FROM gtfs_stop_times").fetchone()[0]

    # Load calendar.txt
    calendar_file = gtfs_dir / "calendar.txt"
    if calendar_file.exists():
        conn.execute("DELETE FROM gtfs_calendar")
        conn.execute(
            """
            INSERT INTO gtfs_calendar (
                service_id, monday, tuesday, wednesday, thursday,
                friday, saturday, sunday, start_date, end_date
            )
            SELECT
                service_id,
                monday = '1',
                tuesday = '1',
                wednesday = '1',
                thursday = '1',
                friday = '1',
                saturday = '1',
                sunday = '1',
                strptime(start_date, '%Y%m%d')::DATE,
                strptime(end_date, '%Y%m%d')::DATE
            FROM read_csv_auto(?, header=true, all_varchar=true)
            """,
            [str(calendar_file)],
        )
        counts["calendar"] = conn.execute("SELECT count(*) FROM gtfs_calendar").fetchone()[0]

    # Load calendar_dates.txt
    calendar_dates_file = gtfs_dir / "calendar_dates.txt"
    if calendar_dates_file.exists():
        conn.execute("DELETE FROM gtfs_calendar_dates")
        conn.execute(
            """
            INSERT INTO gtfs_calendar_dates (service_id, date, exception_type)
            SELECT
                service_id,
                strptime(date, '%Y%m%d')::DATE,
                CAST(exception_type AS INTEGER)
            FROM read_csv_auto(?, header=true, all_varchar=true)
            """,
            [str(calendar_dates_file)],
        )
        counts["calendar_dates"] = conn.execute("SELECT count(*) FROM gtfs_calendar_dates").fetchone()[0]

    return counts

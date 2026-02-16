"""Configuration settings for Halifax Transit reliability tracker."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
GTFS_DIR = DATA_DIR / "gtfs"
ARCHIVE_DIR = DATA_DIR / "archive"
DB_PATH = DATA_DIR / "transit.duckdb"

# GTFS-RT Feed URLs (Halifax Transit)
TRIP_UPDATES_URL = "https://gtfs.halifax.ca/realtime/TripUpdate/TripUpdates.pb"

# Static GTFS download URL
STATIC_GTFS_URL = "https://gtfs.halifax.ca/static/google_transit.zip"

# Polling configuration
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))

# On-time performance thresholds (in seconds)
EARLY_THRESHOLD = int(os.getenv("EARLY_THRESHOLD", "-60"))  # 1 minute early
LATE_THRESHOLD = int(os.getenv("LATE_THRESHOLD", "300"))  # 5 minutes late

# Feed staleness threshold
MAX_FEED_AGE_SECONDS = int(os.getenv("MAX_FEED_AGE_SECONDS", "300"))

# Archive retention
ARCHIVE_RETENTION_DAYS = int(os.getenv("ARCHIVE_RETENTION_DAYS", "90"))


def is_on_time(delay_seconds: int | None) -> bool | None:
    """Determine if a delay value is considered on-time.

    On-time is defined as:
    - No more than 1 minute early (EARLY_THRESHOLD = -60 seconds)
    - No more than 5 minutes late (LATE_THRESHOLD = 300 seconds)

    Returns None if delay is unknown (missing from feed).
    """
    if delay_seconds is None:
        return None
    return EARLY_THRESHOLD <= delay_seconds <= LATE_THRESHOLD

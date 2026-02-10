"""GTFS-RT feed polling and data collection."""

import gzip
import logging
import time
from datetime import datetime
from pathlib import Path

import requests
from google.transit import gtfs_realtime_pb2

from src.config import (
    ARCHIVE_DIR,
    MAX_FEED_AGE_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
    TRIP_UPDATES_URL,
)

logger = logging.getLogger(__name__)


class GTFSRealtimePoller:
    """Polls GTFS-RT feeds and returns parsed protobuf messages."""

    def __init__(
        self,
        trip_updates_url: str = TRIP_UPDATES_URL,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
        archive_dir: Path | None = ARCHIVE_DIR,
    ):
        """Initialize the poller.

        Args:
            trip_updates_url: URL for TripUpdates feed.
            timeout: Request timeout in seconds.
            archive_dir: Directory to archive raw .pb files. None to disable.
        """
        self.trip_updates_url = trip_updates_url
        self.timeout = timeout
        self.archive_dir = archive_dir

        if self.archive_dir:
            self.archive_dir.mkdir(parents=True, exist_ok=True)

    def _fetch_feed(self, url: str) -> bytes | None:
        """Fetch raw protobuf bytes from a feed URL.

        Args:
            url: GTFS-RT feed URL.

        Returns:
            Raw protobuf bytes or None on error.
        """
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    def _parse_feed(
        self,
        data: bytes,
    ) -> gtfs_realtime_pb2.FeedMessage | None: # type: ignore
        """Parse raw bytes into a FeedMessage.

        Args:
            data: Raw protobuf bytes.

        Returns:
            Parsed FeedMessage or None on error.
        """
        try:
            feed = gtfs_realtime_pb2.FeedMessage() # type: ignore
            feed.ParseFromString(data)
            return feed
        except Exception as e:
            logger.error(f"Failed to parse feed: {e}")
            return None

    def _is_feed_stale(
        self,
        feed: gtfs_realtime_pb2.FeedMessage, # type: ignore
        max_age: int = MAX_FEED_AGE_SECONDS,
    ) -> bool:
        """Check if a feed is too old to use.

        Args:
            feed: Parsed FeedMessage.
            max_age: Maximum age in seconds.

        Returns:
            True if feed is stale, False otherwise.
        """
        if not feed.header.HasField("timestamp"):
            return False

        feed_time = feed.header.timestamp
        current_time = int(time.time())
        age = current_time - feed_time

        if age > max_age:
            logger.warning(f"Feed is {age}s old, exceeds max age of {max_age}s")
            return True

        return False

    def _archive_feed(self, data: bytes, feed_type: str) -> None:
        """Archive raw feed data to gzipped file.

        Args:
            data: Raw protobuf bytes.
            feed_type: Type of feed (trip_updates).
        """
        if not self.archive_dir:
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        date_dir = self.archive_dir / datetime.now().strftime("%Y%m%d")
        date_dir.mkdir(parents=True, exist_ok=True)

        filename = date_dir / f"{feed_type}_{timestamp}.pb.gz"

        with gzip.open(filename, "wb") as f:
            f.write(data)

    def fetch_trip_updates(
        self,
        archive: bool = True,
    ) -> gtfs_realtime_pb2.FeedMessage | None: # type: ignore
        """Fetch and parse the TripUpdates feed.

        Args:
            archive: Whether to archive the raw data.

        Returns:
            Parsed FeedMessage or None on error.
        """
        data = self._fetch_feed(self.trip_updates_url)
        if data is None:
            return None

        if archive:
            self._archive_feed(data, "trip_updates")

        feed = self._parse_feed(data)
        if feed and self._is_feed_stale(feed):
            logger.warning("TripUpdates feed is stale")

        return feed

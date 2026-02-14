"""Halifax Transit GTFS-RT poller — collects live delay data into DuckDB."""

import logging
import time
from datetime import datetime

from src.config import POLL_INTERVAL_SECONDS
from src.db.connection import get_connection
from src.db.queries import insert_stop_delay_events, log_poll
from src.gtfs.realtime_poller import GTFSRealtimePoller
from src.models import PollLogRecord
from src.processing.trip_updates import process_trip_updates

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# How long to run the poller (seconds). Set to None for indefinite.
RUN_DURATION_SECONDS = 10 * 60  # 10 minutes


def poll_once(poller: GTFSRealtimePoller, conn) -> None:
    """Execute a single poll cycle: fetch → parse → store → log."""
    polled_at = datetime.now()
    error_message = None
    trip_updates_count = None
    feed_timestamp = None

    try:
        # 1. Fetch feed
        fetch_start = time.time()
        feed = poller.fetch_trip_updates(archive=False)
        fetch_ms = int((time.time() - fetch_start) * 1000)

        if feed is None:
            error_message = "Failed to fetch feed"
            logger.error(error_message)
            return

        feed_timestamp = datetime.fromtimestamp(feed.header.timestamp)

        # 2. Process into StopDelayEvents
        process_start = time.time()
        events = process_trip_updates(feed)
        process_ms = int((time.time() - process_start) * 1000)

        trip_updates_count = len(events)

        # 3. Insert into DuckDB
        inserted = insert_stop_delay_events(conn, events)
        logger.info(
            f"Poll complete: {inserted} events inserted "
            f"(fetch={fetch_ms}ms, process={process_ms}ms, feed_ts={feed_timestamp})"
        )

    except Exception as e:
        error_message = str(e)
        logger.exception("Error during poll cycle")
        fetch_ms = None
        process_ms = None

    finally:
        # 4. Log poll health
        log_poll(
            conn,
            PollLogRecord(
                polled_at=polled_at,
                trip_updates_count=trip_updates_count,
                fetch_duration_ms=fetch_ms if error_message is None else None,
                process_duration_ms=process_ms if error_message is None else None,
                error_message=error_message,
                trip_feed_timestamp=feed_timestamp,
            ),
        )


def main() -> None:
    logger.info(
        f"Starting poller — polling every {POLL_INTERVAL_SECONDS}s "
        f"for {RUN_DURATION_SECONDS}s"
    )

    poller = GTFSRealtimePoller()
    conn = get_connection()
    start_time = time.time()

    try:
        while True:
            poll_once(poller, conn)

            elapsed = time.time() - start_time
            if RUN_DURATION_SECONDS and elapsed >= RUN_DURATION_SECONDS:
                logger.info(f"Run duration reached ({RUN_DURATION_SECONDS}s). Stopping.")
                break

            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Interrupted by user. Stopping.")
    finally:
        conn.close()
        logger.info("Connection closed. Done.")


if __name__ == "__main__":
    main()

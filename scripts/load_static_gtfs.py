#!/usr/bin/env python3
"""Load static GTFS data into DuckDB reference tables."""

import argparse
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import GTFS_DIR
from src.db.connection import get_connection
from src.db.schema import create_tables
from src.gtfs.static_loader import download_static_gtfs, load_static_gtfs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Load static GTFS data."""
    parser = argparse.ArgumentParser(description="Load static GTFS data into DuckDB")
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download fresh GTFS data from Halifax Transit",
    )
    parser.add_argument(
        "--gtfs-dir",
        type=Path,
        default=GTFS_DIR,
        help="Directory containing GTFS text files",
    )
    args = parser.parse_args()

    gtfs_dir = args.gtfs_dir

    if args.download:
        logger.info("Downloading static GTFS data from Halifax Transit...")
        gtfs_dir = download_static_gtfs(gtfs_dir)
        logger.info(f"Downloaded GTFS data to {gtfs_dir}")

    if not gtfs_dir.exists():
        logger.error(f"GTFS directory not found: {gtfs_dir}")
        logger.error("Use --download to fetch GTFS data, or specify --gtfs-dir")
        sys.exit(1)

    conn = get_connection()
    try:
        # Ensure tables exist
        create_tables(conn)

        logger.info(f"Loading static GTFS from {gtfs_dir}")
        counts = load_static_gtfs(conn, gtfs_dir)

        logger.info("Loaded GTFS data:")
        for table, count in counts.items():
            logger.info(f"  - {table}: {count:,} records")

    finally:
        conn.close()


if __name__ == "__main__":
    main()

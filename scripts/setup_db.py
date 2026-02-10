#!/usr/bin/env python3
"""Initialize the DuckDB database with all required tables."""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import DB_PATH
from src.db.connection import get_connection
from src.db.schema import create_tables

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info(f"Initializing database at {DB_PATH}")

    # Ensure data directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    try:
        create_tables(conn)
        logger.info("Database tables created successfully")

        # Verify tables were created
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()

        logger.info(f"Created {len(tables)} tables:")
        for (table_name,) in tables:
            logger.info(f"  - {table_name}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()

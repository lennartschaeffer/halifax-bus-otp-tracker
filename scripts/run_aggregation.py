#!/usr/bin/env python3
"""Run daily aggregation job to summarize delay events."""

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.aggregation.daily_summary import backfill_aggregations, run_daily_aggregation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> date:
    """Parse a date string in YYYY-MM-DD format."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def main() -> None:
    """Run daily aggregation."""
    parser = argparse.ArgumentParser(description="Run daily aggregation for delay data")
    parser.add_argument(
        "--date",
        type=parse_date,
        default=None,
        help="Date to aggregate (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--backfill-start",
        type=parse_date,
        default=None,
        help="Start date for backfill (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--backfill-end",
        type=parse_date,
        default=None,
        help="End date for backfill (YYYY-MM-DD). Defaults to yesterday.",
    )
    args = parser.parse_args()

    if args.backfill_start:
        # Backfill mode
        end_date = args.backfill_end or (date.today() - timedelta(days=1))
        logger.info(f"Running backfill from {args.backfill_start} to {end_date}")

        results = backfill_aggregations(args.backfill_start, end_date)

        total_daily = sum(r["daily_route_summaries"] for r in results)
        total_hourly = sum(r["hourly_route_summaries"] for r in results)

        logger.info(f"Backfill complete: {len(results)} days processed")
        logger.info(f"  Total daily summaries: {total_daily:,}")
        logger.info(f"  Total hourly summaries: {total_hourly:,}")

    else:
        # Single day mode
        target_date = args.date or (date.today() - timedelta(days=1))
        result = run_daily_aggregation(target_date)

        logger.info(f"Aggregation complete for {result['date']}:")
        logger.info(f"  Daily route summaries: {result['daily_route_summaries']:,}")
        logger.info(f"  Hourly route summaries: {result['hourly_route_summaries']:,}")


if __name__ == "__main__":
    main()

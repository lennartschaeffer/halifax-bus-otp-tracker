"""Process TripUpdates feed into StopDelayEvent records."""

from datetime import date, datetime

from google.transit import gtfs_realtime_pb2

from src.config import is_on_time
from src.models import StopDelayEvent


def parse_service_date(date_str: str) -> date:
    """Parse GTFS date string (YYYYMMDD) to date object.

    Args:
        date_str: Date string in YYYYMMDD format.

    Returns:
        Parsed date object.
    """
    return datetime.strptime(date_str, "%Y%m%d").date()


def process_trip_updates(
    feed: gtfs_realtime_pb2.FeedMessage, # type: ignore
) -> list[StopDelayEvent]:
    """Process a TripUpdates feed into StopDelayEvent records.

    Args:
        feed: Parsed GTFS-RT FeedMessage containing trip updates.

    Returns:
        List of StopDelayEvent records ready for database insertion.
    """
    events: list[StopDelayEvent] = []
    observed_at = datetime.now()
    feed_timestamp = datetime.fromtimestamp(feed.header.timestamp)

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip_update = entity.trip_update
        trip = trip_update.trip

        # Extract trip metadata
        trip_id = trip.trip_id
        route_id = trip.route_id if trip.HasField("route_id") else None
        direction_id = trip.direction_id if trip.HasField("direction_id") else None

        # Parse service date
        service_date = None
        if trip.HasField("start_date"):
            try:
                service_date = parse_service_date(trip.start_date)
            except ValueError:
                continue

        if not service_date:
            continue

        if not route_id:
            continue

        # Extract vehicle ID
        vehicle_id = None
        if trip_update.HasField("vehicle"):
            if trip_update.vehicle.HasField("id"):
                vehicle_id = trip_update.vehicle.id
            elif trip_update.vehicle.HasField("label"):
                vehicle_id = trip_update.vehicle.label

        # Process each stop time update
        for stu in trip_update.stop_time_update:
            stop_id = stu.stop_id if stu.HasField("stop_id") else None
            stop_sequence = stu.stop_sequence if stu.HasField("stop_sequence") else None

            if not stop_id or stop_sequence is None:
                continue

            # Extract arrival delay and time
            arrival_delay = None
            predicted_arrival = None
            if stu.HasField("arrival"):
                if stu.arrival.HasField("delay"):
                    arrival_delay = stu.arrival.delay
                if stu.arrival.HasField("time"):
                    predicted_arrival = datetime.fromtimestamp(stu.arrival.time)

            # Extract departure delay and time
            departure_delay = None
            predicted_departure = None
            if stu.HasField("departure"):
                if stu.departure.HasField("delay"):
                    departure_delay = stu.departure.delay
                if stu.departure.HasField("time"):
                    predicted_departure = datetime.fromtimestamp(stu.departure.time)

            # Use arrival delay for on-time calculation, fall back to departure
            delay_for_otp = arrival_delay if arrival_delay is not None else departure_delay

            # Calculate time dimensions
            hour_of_day = observed_at.hour
            day_of_week = observed_at.weekday()

            event = StopDelayEvent(
                observed_at=observed_at,
                trip_id=trip_id,
                stop_id=stop_id,
                stop_sequence=stop_sequence,
                service_date=service_date,
                route_id=route_id,
                direction_id=direction_id,
                vehicle_id=vehicle_id,
                arrival_delay=arrival_delay,
                departure_delay=departure_delay,
                predicted_arrival=predicted_arrival,
                predicted_departure=predicted_departure,
                feed_timestamp=feed_timestamp,
                hour_of_day=hour_of_day,
                day_of_week=day_of_week,
                is_on_time=is_on_time(delay_for_otp),
            )
            events.append(event)

    return events

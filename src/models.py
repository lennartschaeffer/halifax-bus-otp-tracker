"""Pydantic models for GTFS-RT data and database records."""

from datetime import date, datetime

from pydantic import BaseModel


# --- Core GTFS-RT Models ---


class TripDescriptor(BaseModel):
    """Shared trip context from GTFS-RT feeds."""

    trip_id: str
    route_id: str
    start_date: str  # YYYYMMDD format
    direction_id: int | None = None


class VehicleDescriptor(BaseModel):
    """Vehicle identification from GTFS-RT feeds."""

    id: str | None = None
    label: str | None = None


class StopTimeEvent(BaseModel):
    """Predicted arrival or departure time."""

    delay: int | None = None  # seconds
    time: int | None = None  # Unix timestamp


class StopTimeUpdate(BaseModel):
    """Stop time prediction from TripUpdates feed."""

    stop_sequence: int
    stop_id: str
    arrival: StopTimeEvent | None = None
    departure: StopTimeEvent | None = None


class TripUpdate(BaseModel):
    """Parsed trip update from TripUpdates feed."""

    trip: TripDescriptor
    vehicle: VehicleDescriptor | None = None
    stop_time_update: list[StopTimeUpdate]
    timestamp: int  # Unix timestamp


# --- Database Record Models ---


class StopDelayEvent(BaseModel):
    """Primary fact table record - one per stop observation."""

    observed_at: datetime
    trip_id: str
    stop_id: str
    stop_sequence: int
    service_date: date
    route_id: str
    direction_id: int | None = None
    vehicle_id: str | None = None
    arrival_delay: int | None = None  # seconds
    departure_delay: int | None = None  # seconds
    predicted_arrival: datetime | None = None
    predicted_departure: datetime | None = None
    feed_timestamp: datetime
    hour_of_day: int  # 0-23
    day_of_week: int  # 0=Mon, 6=Sun
    is_on_time: bool | None


class PollLogRecord(BaseModel):
    """Health monitoring record for each poll cycle."""

    polled_at: datetime
    trip_updates_count: int | None = None
    fetch_duration_ms: int | None = None
    process_duration_ms: int | None = None
    error_message: str | None = None
    trip_feed_timestamp: datetime | None = None

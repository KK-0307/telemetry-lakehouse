"""Telemetry event contract, version 1.

This module is the single source of truth for the event contract. It defines

* the *wire* model (`TelemetryEvent`, a strict Pydantic model used to validate one
  source record), and
* the *storage* schema (`EVENT_SCHEMA`, an explicit PyArrow schema).

Policies (see docs/data_contract.md):

* **Nullability** - every field is required and non-null in v1. A missing reading is an
  invalid event, not a NULL measurement. Relaxing this is a schema-version bump.
* **Ranges** - measurements outside the closed ranges below are invalid, never clamped.
* **Time** - timestamps must carry an explicit UTC offset; they are normalised to UTC and
  stored with microsecond precision.
* **Schema versions** - only versions in `SUPPORTED_SCHEMA_VERSIONS` are accepted.
* **Identity** - `event_id` is the logical identity of an event; `(device_id,
  sequence_number)` is a second, independent identity that must map 1:1 to `event_id`.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated

import pyarrow as pa
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, field_validator

SCHEMA_VERSION = 1
SUPPORTED_SCHEMA_VERSIONS: frozenset[int] = frozenset({1})

TEMPERATURE_C_RANGE = (-60.0, 100.0)
HUMIDITY_PCT_RANGE = (0.0, 100.0)
BATTERY_PCT_RANGE = (0.0, 100.0)
SIGNAL_DBM_RANGE = (-130, 0)
MAX_SEQUENCE_NUMBER = 2**63 - 1

PARTITION_COLUMN = "event_date"


class DeviceStatus(StrEnum):
    OK = "OK"
    DEGRADED = "DEGRADED"
    LOW_BATTERY = "LOW_BATTERY"
    MAINTENANCE = "MAINTENANCE"


# Identifiers are restricted to a path- and SQL-safe alphabet.
Identifier = Annotated[
    str, StringConstraints(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.:\-]+$")
]


class TelemetryEvent(BaseModel):
    """One telemetry event as it must arrive from the source (strict, no coercion)."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    event_id: Identifier
    device_id: Identifier
    sequence_number: Annotated[int, Field(ge=1, le=MAX_SEQUENCE_NUMBER)]
    event_time: AwareDatetime
    ingestion_time: AwareDatetime
    schema_version: int
    temperature_c: Annotated[
        float, Field(ge=TEMPERATURE_C_RANGE[0], le=TEMPERATURE_C_RANGE[1], allow_inf_nan=False)
    ]
    humidity_pct: Annotated[
        float, Field(ge=HUMIDITY_PCT_RANGE[0], le=HUMIDITY_PCT_RANGE[1], allow_inf_nan=False)
    ]
    battery_pct: Annotated[
        float, Field(ge=BATTERY_PCT_RANGE[0], le=BATTERY_PCT_RANGE[1], allow_inf_nan=False)
    ]
    signal_dbm: Annotated[int, Field(ge=SIGNAL_DBM_RANGE[0], le=SIGNAL_DBM_RANGE[1])]
    status: DeviceStatus

    @field_validator("schema_version")
    @classmethod
    def _supported_version(cls, v: int) -> int:
        if v not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValueError(
                f"unsupported schema_version {v}; supported: {sorted(SUPPORTED_SCHEMA_VERSIONS)}"
            )
        return v

    @field_validator("event_time", "ingestion_time")
    @classmethod
    def _normalise_utc(cls, v: datetime) -> datetime:
        return v.astimezone(UTC)


# Storage schema. `event_date` is a Hive partition column derived from `event_time` (UTC) and
# is deliberately *not* stored inside the files.
EVENT_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("device_id", pa.string(), nullable=False),
        pa.field("sequence_number", pa.int64(), nullable=False),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("ingestion_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("schema_version", pa.int32(), nullable=False),
        pa.field("temperature_c", pa.float64(), nullable=False),
        pa.field("humidity_pct", pa.float64(), nullable=False),
        pa.field("battery_pct", pa.float64(), nullable=False),
        pa.field("signal_dbm", pa.int32(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
    ]
)

EVENT_COLUMNS: tuple[str, ...] = tuple(EVENT_SCHEMA.names)


def table_from_events(events: Sequence[TelemetryEvent]) -> pa.Table:
    """Build a columnar table with the exact `EVENT_SCHEMA` from validated events."""
    return pa.table(
        {
            "event_id": [e.event_id for e in events],
            "device_id": [e.device_id for e in events],
            "sequence_number": [e.sequence_number for e in events],
            "event_time": [e.event_time for e in events],
            "ingestion_time": [e.ingestion_time for e in events],
            "schema_version": [e.schema_version for e in events],
            "temperature_c": [e.temperature_c for e in events],
            "humidity_pct": [e.humidity_pct for e in events],
            "battery_pct": [e.battery_pct for e in events],
            "signal_dbm": [e.signal_dbm for e in events],
            "status": [e.status.value for e in events],
        },
        schema=EVENT_SCHEMA,
    )

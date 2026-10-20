from __future__ import annotations

import json
import math
from datetime import UTC, datetime

import pyarrow as pa
import pytest
from pydantic import ValidationError

from telemetry_lakehouse.schemas import EVENT_SCHEMA, TelemetryEvent, table_from_events
from tests.conftest import valid_wire_event


def parse(**overrides):
    return TelemetryEvent.model_validate_json(json.dumps(valid_wire_event(**overrides)))


def test_valid_event_parses():
    event = parse()
    assert event.event_id == "evt-0001"
    assert event.status.value == "OK"


@pytest.mark.parametrize(
    "overrides",
    [
        {"temperature_c": -60.01},
        {"temperature_c": 100.01},
        {"humidity_pct": -0.01},
        {"humidity_pct": 100.01},
        {"battery_pct": 100.5},
        {"battery_pct": -1},
        {"signal_dbm": 1},
        {"signal_dbm": -131},
        {"sequence_number": 0},
        {"sequence_number": -3},
    ],
)
def test_out_of_range_values_are_rejected_not_clamped(overrides):
    with pytest.raises(ValidationError):
        parse(**overrides)


@pytest.mark.parametrize("value", [-60.0, 100.0])
def test_range_bounds_are_inclusive(value):
    assert parse(temperature_c=value).temperature_c == value


@pytest.mark.parametrize("field", list(valid_wire_event()))
def test_every_field_is_required(field):
    wire = valid_wire_event()
    del wire[field]
    with pytest.raises(ValidationError):
        TelemetryEvent.model_validate_json(json.dumps(wire))


@pytest.mark.parametrize("field", list(valid_wire_event()))
def test_null_is_rejected_for_every_field(field):
    with pytest.raises(ValidationError):
        parse(**{field: None})


def test_unsupported_schema_version_is_rejected():
    with pytest.raises(ValidationError, match="unsupported schema_version 2"):
        parse(schema_version=2)


def test_unknown_fields_are_rejected():
    with pytest.raises(ValidationError):
        parse(firmware="1.2.3")


def test_unknown_status_is_rejected():
    with pytest.raises(ValidationError):
        parse(status="EXPLODING")


def test_strict_types_no_string_coercion():
    with pytest.raises(ValidationError):
        parse(temperature_c="21.5")
    with pytest.raises(ValidationError):
        parse(sequence_number="1")


def test_nan_and_infinity_are_rejected():
    for token in ("NaN", "Infinity"):
        raw = json.dumps(valid_wire_event()).replace("21.5", token)
        with pytest.raises(ValidationError):
            TelemetryEvent.model_validate_json(raw)
    assert math.isfinite(parse().temperature_c)


def test_naive_timestamps_are_rejected():
    with pytest.raises(ValidationError):
        parse(event_time="2026-01-01T12:00:00")


def test_offset_timestamps_are_normalised_to_utc():
    event = parse(event_time="2026-01-01T23:30:00-05:00")
    assert event.event_time == datetime(2026, 1, 2, 4, 30, tzinfo=UTC)
    assert event.event_time.utcoffset().total_seconds() == 0


def test_z_suffix_is_utc():
    assert parse(event_time="2026-01-01T00:00:00Z").event_time.tzinfo is not None


@pytest.mark.parametrize("bad_id", ["", "has space", "a/b", "x" * 65, "'; DROP"])
def test_unsafe_identifiers_are_rejected(bad_id):
    with pytest.raises(ValidationError):
        parse(event_id=bad_id)


def test_table_matches_explicit_schema():
    table = table_from_events([parse(), parse(event_id="evt-2", sequence_number=2)])
    assert table.schema.equals(EVENT_SCHEMA)
    assert table.num_rows == 2
    assert all(not f.nullable for f in table.schema)
    assert table.schema.field("event_time").type == pa.timestamp("us", tz="UTC")
    assert table["event_time"][0].as_py() == datetime(2026, 1, 1, 12, tzinfo=UTC)

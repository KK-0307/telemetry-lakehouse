from __future__ import annotations

import json
import tempfile
from datetime import UTC, datetime, timedelta, timezone
from itertools import pairwise
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from telemetry_lakehouse.batcher import MicroBatcher, SourceRecord, compute_batch_id
from telemetry_lakehouse.config import BatchConfig, StorageConfig
from telemetry_lakehouse.ingest import JsonlSource, run_ingest
from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.reader import connect
from telemetry_lakehouse.schemas import TelemetryEvent


@given(
    count=st.integers(min_value=0, max_value=300),
    start=st.integers(min_value=0, max_value=10_000),
    size=st.integers(min_value=1, max_value=64),
)
def test_batching_partitions_the_stream_exactly(count, start, size):
    stream = [SourceRecord(start + i, bytes([i % 256])) for i in range(count)]
    batches = list(MicroBatcher("src", size).batches(stream))

    # Concatenation reproduces the input exactly: nothing lost, duplicated or reordered.
    assert [r for b in batches for r in b.records] == stream
    # Intervals tile the offset range with no gap or overlap, and each is bounded by `size`.
    assert all(1 <= len(b) <= size for b in batches)
    assert all(b.end_offset - b.start_offset == len(b) for b in batches)
    assert all(a.end_offset == b.start_offset for a, b in pairwise(batches))
    # Batch identity is a pure function of the interval.
    assert all(b.batch_id == compute_batch_id("src", b.start_offset, b.end_offset) for b in batches)
    assert len({b.batch_id for b in batches}) == len(batches)


offsets = st.integers(min_value=-14 * 60, max_value=14 * 60).map(
    lambda m: timezone(timedelta(minutes=m))
)
times = st.datetimes(
    min_value=datetime(2000, 1, 1), max_value=datetime(2100, 1, 1), timezones=offsets
)


@st.composite
def wire_events(draw):
    n = draw(st.integers(min_value=1, max_value=40))
    out = []
    for i in range(n):
        et = draw(times)
        out.append(
            {
                "event_id": f"e{i}",
                "device_id": f"d{draw(st.integers(0, 4))}",
                "sequence_number": i + 1,
                "event_time": et.isoformat(),
                "ingestion_time": (et + timedelta(seconds=1)).isoformat(),
                "schema_version": 1,
                "temperature_c": draw(st.floats(-60, 100, allow_nan=False)),
                "humidity_pct": draw(st.floats(0, 100, allow_nan=False)),
                "battery_pct": draw(st.floats(0, 100, allow_nan=False)),
                "signal_dbm": draw(st.integers(-130, 0)),
                "status": draw(st.sampled_from(["OK", "DEGRADED", "LOW_BATTERY", "MAINTENANCE"])),
            }
        )
    return out


@settings(max_examples=60, deadline=None)
@given(rows=wire_events())
def test_any_valid_events_round_trip_through_parquet_into_the_right_partition(rows):
    events = [TelemetryEvent.model_validate_json(json.dumps(r)) for r in rows]
    with tempfile.TemporaryDirectory() as tmp:
        layout = StorageLayout(Path(tmp))
        layout.ensure()
        layout.default_source.write_bytes(b"".join(json.dumps(r).encode() + b"\n" for r in rows))
        summary = run_ingest(
            JsonlSource.open(layout.default_source),
            layout,
            BatchConfig(batch_size=7),
            StorageConfig(),
        )
        assert summary.blocked == []  # the gate accepts every valid event
        con = connect(layout)
        try:
            got = con.execute(
                "SELECT event_id, event_date::VARCHAR, "
                "CAST(event_time AT TIME ZONE 'UTC' AS DATE)::VARCHAR, temperature_c, "
                "epoch_us(event_time) FROM events ORDER BY event_id"
            ).fetchall()
        finally:
            con.close()
    assert sorted(e.event_id for e in events) == [g[0] for g in got]
    by_id = {e.event_id: e for e in events}
    for event_id, partition, utc_date, temperature, event_us in got:
        e = by_id[event_id]
        assert partition == utc_date == e.event_time.astimezone(UTC).date().isoformat()
        assert temperature == e.temperature_c  # float64 round-trips exactly
        assert event_us == (e.event_time - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(
            microseconds=1
        )

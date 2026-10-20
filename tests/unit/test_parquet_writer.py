from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime

import pyarrow.parquet as pq
import pytest

from telemetry_lakehouse.config import StorageConfig
from telemetry_lakehouse.fsutil import ImmutableFileExistsError, write_new_file
from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.parquet_writer import split_by_event_date, write_batch
from telemetry_lakehouse.publisher import cleanup_staged, place_staged
from telemetry_lakehouse.schemas import EVENT_SCHEMA, TelemetryEvent, table_from_events
from tests.conftest import valid_wire_event

CFG = StorageConfig()


def table(*specs: tuple[str, int, str]):
    """Rows from (device_id, sequence_number, event_time) triples."""
    events = [
        TelemetryEvent.model_validate_json(
            json.dumps(
                valid_wire_event(
                    event_id=f"{dev}-{seq}", device_id=dev, sequence_number=seq, event_time=ts
                )
            )
        )
        for dev, seq, ts in specs
    ]
    return table_from_events(events)


def test_partitions_by_utc_date_not_local_date():
    # 23:30 at UTC-05:00 is 04:30 the NEXT day in UTC.
    t = table(
        ("d1", 1, "2026-01-01T12:00:00+00:00"),
        ("d1", 2, "2026-01-01T23:30:00-05:00"),
        ("d1", 3, "2026-01-01T01:00:00+09:00"),  # 2025-12-31T16:00Z
    )
    parts = {d: p.num_rows for d, p in split_by_event_date(t)}
    assert parts == {date(2025, 12, 31): 1, date(2026, 1, 1): 1, date(2026, 1, 2): 1}


def test_midnight_boundaries_are_half_open():
    t = table(
        ("d", 1, "2026-01-01T23:59:59.999999+00:00"),
        ("d", 2, "2026-01-02T00:00:00+00:00"),
    )
    assert [d for d, _ in split_by_event_date(t)] == [date(2026, 1, 1), date(2026, 1, 2)]


def test_batch_spanning_dates_writes_one_file_per_date(layout: StorageLayout):
    t = table(
        ("d1", 1, "2026-01-01T10:00:00+00:00"),
        ("d1", 2, "2026-01-02T10:00:00+00:00"),
        ("d2", 1, "2026-01-02T11:00:00+00:00"),
    )
    staged = write_batch(t, "b1", layout, CFG)
    assert [(s.event_date, s.rows) for s in staged] == [
        (date(2026, 1, 1), 1),
        (date(2026, 1, 2), 2),
    ]
    assert sum(s.rows for s in staged) == t.num_rows
    for s in staged:
        assert s.path == layout.staging_dir / "b1" / f"event_date={s.event_date}" / "b1.parquet"
        assert pq.read_metadata(s.path).num_rows == s.rows


def test_file_schema_is_explicit_and_partition_column_is_not_stored(layout: StorageLayout):
    staged = write_batch(table(("d", 1, "2026-01-01T00:00:00+00:00")), "b1", layout, CFG)
    schema = pq.read_schema(staged[0].path)
    assert schema.equals(EVENT_SCHEMA)
    assert "event_date" not in schema.names


@pytest.mark.parametrize(
    ("name", "expected"), [("zstd", "ZSTD"), ("snappy", "SNAPPY"), ("none", "UNCOMPRESSED")]
)
def test_compression_is_configurable_and_defaults_to_zstd(layout: StorageLayout, name, expected):
    assert StorageConfig().compression == "zstd"
    t = table(("d", 1, "2026-01-01T00:00:00+00:00"))
    staged = write_batch(t, "b1", layout, StorageConfig(compression=name))
    codec = pq.read_metadata(staged[0].path).row_group(0).column(0).compression
    assert codec == expected


def test_row_group_size_is_honoured(layout: StorageLayout):
    t = table(*[("d", i, "2026-01-01T00:00:00+00:00") for i in range(1, 26)])
    staged = write_batch(t, "b1", layout, StorageConfig(row_group_size=10))
    assert pq.read_metadata(staged[0].path).num_row_groups == 3


def test_rows_are_sorted_by_device_then_sequence(layout: StorageLayout):
    t = table(
        ("d2", 1, "2026-01-01T00:00:00+00:00"),
        ("d1", 2, "2026-01-01T00:00:00+00:00"),
        ("d1", 1, "2026-01-01T00:00:00+00:00"),
    )
    staged = write_batch(t, "b1", layout, CFG)
    rows = pq.read_table(staged[0].path).to_pylist()
    assert [(r["device_id"], r["sequence_number"]) for r in rows] == [
        ("d1", 1),
        ("d1", 2),
        ("d2", 1),
    ]


def test_staged_metadata_matches_the_bytes_on_disk(layout: StorageLayout):
    staged = write_batch(table(("d", 1, "2026-01-01T00:00:00+00:00")), "b1", layout, CFG)[0]
    data = staged.path.read_bytes()
    assert staged.size_bytes == len(data)
    assert staged.sha256 == hashlib.sha256(data).hexdigest()
    assert staged.min_event_time == staged.max_event_time == datetime(2026, 1, 1, tzinfo=UTC)


def test_staging_is_deterministic(tmp_path):
    t = table(("d", 1, "2026-01-01T00:00:00+00:00"), ("d", 2, "2026-01-02T00:00:00+00:00"))
    a, b = StorageLayout(tmp_path / "a"), StorageLayout(tmp_path / "b")
    sa = write_batch(t, "b1", a, CFG)
    sb = write_batch(t, "b1", b, CFG)
    assert [s.sha256 for s in sa] == [s.sha256 for s in sb]


def test_staged_file_is_never_overwritten(layout: StorageLayout):
    t = table(("d", 1, "2026-01-01T00:00:00+00:00"))
    first = write_batch(t, "b1", layout, CFG)[0]
    before = first.path.read_bytes()
    with pytest.raises(ImmutableFileExistsError):
        write_batch(t, "b1", layout, CFG)
    assert first.path.read_bytes() == before
    assert not list(layout.staging_dir.rglob(".*.tmp"))


def test_wrong_schema_is_refused(layout: StorageLayout):
    with pytest.raises(ValueError, match="EVENT_SCHEMA"):
        write_batch(
            table(("d", 1, "2026-01-01T00:00:00+00:00")).drop(["status"]), "b1", layout, CFG
        )


def test_empty_table_writes_nothing(layout: StorageLayout):
    empty = table(("d", 1, "2026-01-01T00:00:00+00:00")).slice(0, 0)
    assert write_batch(empty, "b1", layout, CFG) == []


def test_write_new_file_refuses_existing_target(tmp_path):
    target = tmp_path / "x" / "f.bin"
    write_new_file(target, b"one")
    with pytest.raises(ImmutableFileExistsError):
        write_new_file(target, b"two")
    assert target.read_bytes() == b"one"
    assert [p.name for p in target.parent.iterdir()] == ["f.bin"]


def test_place_links_files_into_data_and_cleanup_removes_staging(layout: StorageLayout):
    t = table(("d", 1, "2026-01-01T00:00:00+00:00"), ("d", 2, "2026-01-02T00:00:00+00:00"))
    staged = write_batch(t, "b1", layout, CFG)
    place_staged(staged, layout)
    for s in staged:
        placed = layout.data_path(s.relative_path)
        assert hashlib.sha256(placed.read_bytes()).hexdigest() == s.sha256
        assert s.path.exists()  # staging copy is kept until the commit succeeded
    cleanup_staged(staged)
    assert not any(layout.staging_dir.iterdir())
    assert all(layout.data_path(s.relative_path).exists() for s in staged)  # data survives cleanup


def test_place_refuses_to_overwrite_and_places_nothing(layout: StorageLayout):
    t = table(("d", 1, "2026-01-01T00:00:00+00:00"), ("d", 2, "2026-01-02T00:00:00+00:00"))
    existing = layout.data_path("event_date=2026-01-02/b1.parquet")
    write_new_file(existing, b"already here")
    staged = write_batch(t, "b1", layout, CFG)
    with pytest.raises(ImmutableFileExistsError):
        place_staged(staged, layout)
    assert existing.read_bytes() == b"already here"
    assert not layout.data_path("event_date=2026-01-01/b1.parquet").exists()
    assert all(s.path.exists() for s in staged)  # staged evidence preserved

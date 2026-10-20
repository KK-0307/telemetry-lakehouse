"""Stage a batch as immutable, date-partitioned Parquet files.

Staging is *not* publication: files written here live in the staging area and are untrusted
until the publisher makes them visible (Milestone 3: until a manifest commit references them).

Determinism: rows are sorted by ``(device_id, sequence_number)`` inside each partition file and
file paths depend only on ``(event_date, batch_id)``, so re-staging the same batch with the same
library versions reproduces the same bytes.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from telemetry_lakehouse.config import StorageConfig
from telemetry_lakehouse.fsutil import write_new_file
from telemetry_lakehouse.layout import StorageLayout, parquet_relative_path
from telemetry_lakehouse.schemas import EVENT_SCHEMA


@dataclass(frozen=True, slots=True)
class StagedFile:
    event_date: date
    relative_path: str  # e.g. event_date=2026-01-01/b0123abcd.parquet
    path: Path  # absolute location in the staging area
    rows: int
    size_bytes: int
    sha256: str
    min_event_time: datetime
    max_event_time: datetime


def event_dates(table: pa.Table) -> pa.Array:
    """UTC calendar date of each row's `event_time` (the partition key)."""
    return pc.cast(pc.floor_temporal(table["event_time"], unit="day"), pa.date32())


def split_by_event_date(table: pa.Table) -> list[tuple[date, pa.Table]]:
    """Split into one table per UTC event date, in ascending date order."""
    dates = event_dates(table)
    unique = sorted(d.as_py() for d in pc.unique(dates))
    if len(unique) == 1:
        return [(unique[0], table)]
    return [(d, table.filter(pc.equal(dates, pa.scalar(d, pa.date32())))) for d in unique]


def write_batch(
    table: pa.Table, batch_id: str, layout: StorageLayout, config: StorageConfig
) -> list[StagedFile]:
    """Write `table` to the staging area, one immutable file per UTC event date.

    Raises `ImmutableFileExistsError` if a staged file for this batch already exists
    (e.g. a prior attempt that was never cleaned up) - it is never silently overwritten.
    """
    if not table.schema.equals(EVENT_SCHEMA):
        raise ValueError("table does not match EVENT_SCHEMA")
    staged: list[StagedFile] = []
    if table.num_rows == 0:
        return staged
    for event_date, part in split_by_event_date(table):
        part = part.sort_by([("device_id", "ascending"), ("sequence_number", "ascending")])
        sink = pa.BufferOutputStream()
        pq.write_table(
            part,
            sink,
            compression=config.compression.upper(),
            row_group_size=config.row_group_size,
            write_statistics=True,
        )
        buf = sink.getvalue()
        path = layout.staged_path(batch_id, event_date)
        write_new_file(path, memoryview(buf))
        bounds = pc.min_max(part["event_time"])
        staged.append(
            StagedFile(
                event_date=event_date,
                relative_path=parquet_relative_path(event_date, batch_id),
                path=path,
                rows=part.num_rows,
                size_bytes=buf.size,
                sha256=hashlib.sha256(memoryview(buf)).hexdigest(),
                min_event_time=bounds["min"].as_py(),
                max_event_time=bounds["max"].as_py(),
            )
        )
    return staged

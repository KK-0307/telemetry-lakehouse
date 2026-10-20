"""End-to-end: simulator -> source log -> micro-batches -> Parquet -> DuckDB."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from telemetry_lakehouse.config import BatchConfig, SimulatorConfig, StorageConfig
from telemetry_lakehouse.ingest import JsonlSource, run_ingest
from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.reader import connect, run_named_query
from telemetry_lakehouse.simulator import reference_table, write_source

# 4000 events at 0.05/s starting 20:00Z spans ~22h => crosses midnight into a second date.
SIM = SimulatorConfig(
    devices=40,
    events=4000,
    seed=11,
    rate_per_second=0.05,
    start_time=datetime(2026, 1, 1, 20, tzinfo=UTC),
)


def ingest(layout: StorageLayout, batch_size: int = 700, **storage):
    meta_path = layout.default_source
    if not meta_path.exists():
        write_source(SIM, meta_path)
    return run_ingest(
        JsonlSource.open(meta_path),
        layout,
        BatchConfig(batch_size=batch_size),
        StorageConfig(**storage),
    )


def published_rows(layout: StorageLayout) -> list[dict]:
    con = connect(layout)
    try:
        table = con.execute(
            "SELECT * EXCLUDE (event_date) FROM events ORDER BY event_id"
        ).to_arrow_table()
    finally:
        con.close()
    return table.to_pylist()


def test_published_dataset_equals_reference_dataset(layout: StorageLayout):
    summary = ingest(layout)
    assert summary.records == SIM.events
    assert summary.batches == 6  # ceil(4000 / 700)

    reference = sorted(reference_table(SIM).to_pylist(), key=lambda r: r["event_id"])
    assert published_rows(layout) == reference


def test_dataset_is_partitioned_by_utc_event_date(layout: StorageLayout):
    ingest(layout)
    dirs = sorted(p.name for p in layout.data_dir.iterdir())
    assert dirs == ["event_date=2026-01-01", "event_date=2026-01-02"]
    con = connect(layout)
    try:
        bad = con.execute(
            "SELECT count(*) FROM events "
            "WHERE event_date <> CAST(event_time AT TIME ZONE 'UTC' AS DATE)"
        ).fetchone()[0]
        per_date = dict(
            con.execute("SELECT event_date::VARCHAR, count(*) FROM events GROUP BY 1").fetchall()
        )
    finally:
        con.close()
    assert bad == 0
    assert sum(per_date.values()) == SIM.events
    assert set(per_date) == {"2026-01-01", "2026-01-02"}


def test_row_accounting_matches_between_summary_files_and_duckdb(layout: StorageLayout):
    summary = ingest(layout)
    files = sorted(layout.data_dir.rglob("*.parquet"))
    assert summary.files == len(files)
    assert sum(pq.read_metadata(f).num_rows for f in files) == summary.records
    assert summary.bytes_written == sum(f.stat().st_size for f in files)
    con = connect(layout)
    try:
        assert con.execute("SELECT count(*) FROM events").fetchone()[0] == summary.records
    finally:
        con.close()


def test_batch_boundaries_do_not_change_content(tmp_path: Path):
    small, large = StorageLayout(tmp_path / "s"), StorageLayout(tmp_path / "l")
    for lay, size in ((small, 300), (large, 3999)):
        lay.ensure()
        write_source(SIM, lay.default_source)
        ingest(lay, batch_size=size)
    assert published_rows(small) == published_rows(large)


@pytest.mark.parametrize("compression", ["zstd", "snappy", "none"])
def test_every_compression_round_trips(layout: StorageLayout, compression):
    ingest(layout, compression=compression)
    assert published_rows(layout) == sorted(
        reference_table(SIM).to_pylist(), key=lambda r: r["event_id"]
    )


def test_ingest_is_deterministic_down_to_the_bytes(tmp_path: Path):
    digests = []
    for name in ("a", "b"):
        lay = StorageLayout(tmp_path / name)
        lay.ensure()
        write_source(SIM, lay.default_source)
        ingest(lay)
        digests.append(
            {
                str(f.relative_to(lay.data_dir)): hashlib.sha256(f.read_bytes()).hexdigest()
                for f in lay.data_dir.rglob("*.parquet")
            }
        )
    assert digests[0] == digests[1]


def test_no_temp_or_staged_files_remain_after_success(layout: StorageLayout):
    ingest(layout)
    assert not list(layout.root.rglob(".*.tmp"))
    assert not any(layout.staging_dir.rglob("*.parquet"))


def test_resuming_from_an_offset_reads_only_the_tail(layout: StorageLayout):
    write_source(SIM, layout.default_source)
    source = JsonlSource.open(layout.default_source, start_offset=3500)
    summary = run_ingest(source, layout, BatchConfig(batch_size=400), StorageConfig())
    assert summary.records == 500
    assert [(m.start_offset, m.end_offset) for m in summary.batch_metrics] == [
        (3500, 3900),
        (3900, 4000),
    ]


def test_rerunning_ingest_is_a_batch_level_noop(layout: StorageLayout):
    # Same source, same batch boundaries => same batch IDs => the manifest says COMMITTED => skip.
    first = ingest(layout)
    files_before = {f: f.read_bytes() for f in layout.data_dir.rglob("*.parquet")}
    second = ingest(layout)
    assert second.skipped_batches == first.batches and second.skipped_records == first.records
    assert second.accounting["consumed"] == 0 and second.files == 0 and second.batch_metrics == []
    assert {f: f.read_bytes() for f in layout.data_dir.rglob("*.parquet")} == files_before


def test_rerunning_with_different_batch_boundaries_still_cannot_duplicate(layout: StorageLayout):
    # Different boundaries => new batch IDs, so the event-level gate is the second defence.
    first = ingest(layout, batch_size=700)
    second = ingest(layout, batch_size=999)
    assert second.skipped_batches == 0
    assert second.accounting["duplicates"] == first.records
    assert second.accounting["committed"] == 0
    assert len(published_rows(layout)) == SIM.events


def test_invalid_record_blocks_its_batch_and_halts_the_run(layout: StorageLayout):
    write_source(SIM, layout.default_source)
    lines = layout.default_source.read_bytes().splitlines(keepends=True)
    lines[1500] = lines[1500].replace(b'"schema_version":1', b'"schema_version":9')
    layout.default_source.write_bytes(b"".join(lines))

    summary = ingest(layout, batch_size=700)
    assert summary.halted and [(b.start_offset, b.end_offset) for b in summary.blocked] == [
        (1400, 2100)
    ]
    assert summary.blocked[0].reason_counts == {"UNSUPPORTED_SCHEMA_VERSION": 1}
    # Batches [0,700) and [700,1400) were published; nothing at or after the blocked one.
    con = connect(layout)
    try:
        assert con.execute("SELECT count(*) FROM events").fetchone()[0] == 1400
    finally:
        con.close()
    assert not any(layout.staging_dir.rglob("*.parquet"))


def test_empty_dataset_is_queryable(layout: StorageLayout):
    columns, rows = run_named_query(layout, "fleet-summary")
    assert rows == []
    assert columns[0] == "event_date"


def test_named_analytical_queries(layout: StorageLayout):
    ingest(layout)
    columns, rows = run_named_query(layout, "hourly-device-health", limit=1000)
    assert columns[:3] == ["hour_utc", "device_id", "readings"]
    assert 0 < len(rows) <= 1000
    assert all(r[2] >= 1 for r in rows)

    _, fleet = run_named_query(layout, "fleet-summary")
    assert [r[0].isoformat() for r in fleet] == ["2026-01-01", "2026-01-02"]
    assert sum(r[1] for r in fleet) == SIM.events

    with pytest.raises(KeyError):
        run_named_query(layout, "nope")

"""Readers see exactly what the manifest says is committed - never what merely exists on disk."""

from __future__ import annotations

import shutil
from datetime import UTC, datetime

import pytest

from telemetry_lakehouse import failpoints
from telemetry_lakehouse.config import BatchConfig, SimulatorConfig, StorageConfig
from telemetry_lakehouse.ingest import JsonlSource, run_ingest
from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.manifest import BatchState, Manifest
from telemetry_lakehouse.reader import connect, visible_files
from telemetry_lakehouse.simulator import write_source
from tests.helpers import committed_rows

SIM = SimulatorConfig(
    devices=20,
    events=3000,
    seed=5,
    rate_per_second=0.05,
    start_time=datetime(2026, 1, 1, 20, tzinfo=UTC),
)


def ingest(layout: StorageLayout, batch_size: int = 1000):
    return run_ingest(
        JsonlSource.open(layout.default_source),
        layout,
        BatchConfig(batch_size=batch_size),
        StorageConfig(),
    )


@pytest.fixture
def source(layout: StorageLayout) -> StorageLayout:
    write_source(SIM, layout.default_source)
    return layout


def test_an_empty_lakehouse_has_nothing_visible(layout: StorageLayout):
    assert visible_files(layout) == [] and committed_rows(layout) == 0


def test_visible_files_are_exactly_the_manifest_files(source: StorageLayout):
    summary = ingest(source)
    with Manifest.open(source) as m:
        committed = m.committed_files()
    assert len(committed) == summary.files
    assert sorted(visible_files(source)) == sorted(source.data_dir.rglob("*.parquet"))
    assert [f.commit_seq for f in committed] == sorted(f.commit_seq for f in committed)


def test_a_parquet_file_planted_in_data_is_not_trusted(source: StorageLayout):
    ingest(source)
    before = committed_rows(source)
    victim = next(source.data_dir.rglob("*.parquet"))
    planted = victim.with_name("planted.parquet")
    shutil.copy(victim, planted)  # a perfectly valid Parquet file - with duplicate events
    assert planted.exists() and committed_rows(source) == before
    assert planted not in visible_files(source)


def test_placed_but_uncommitted_files_are_invisible(source: StorageLayout):
    with failpoints.armed("after_place", nth=2), pytest.raises(failpoints.SimulatedCrash):
        ingest(source)
    on_disk = list(source.data_dir.rglob("*.parquet"))
    assert len(on_disk) > len(visible_files(source))  # batch 2's files exist but are unreferenced
    assert committed_rows(source) == 1000  # only batch 1


def test_batch_visibility_is_all_or_nothing_across_files(source: StorageLayout):
    """Batch 1 spans midnight (two files). A crash inside the commit exposes neither."""
    with failpoints.armed("in_commit", nth=1), pytest.raises(failpoints.SimulatedCrash):
        ingest(source)
    on_disk = list(source.data_dir.rglob("*.parquet"))
    assert len(on_disk) == 2  # both files of the batch were placed...
    assert visible_files(source) == [] and committed_rows(source) == 0  # ...neither is visible
    ingest_ok = False
    with Manifest.open(source) as m:
        assert [b.state for b in m.batches()] == [BatchState.VERIFIED]
        ingest_ok = m._con.execute("SELECT count(*) FROM event_index").fetchone()[0] == 0
    assert ingest_ok


def test_manifest_matches_physical_files_and_accounting(source: StorageLayout):
    import hashlib

    import pyarrow.parquet as pq

    summary = ingest(source)
    with Manifest.open(source) as m:
        files = m.committed_files()
        batches = m.batches(BatchState.COMMITTED)
        n_index = m._con.execute("SELECT count(*) FROM event_index").fetchone()[0]
    for f in files:
        path = source.data_path(f.relative_path)
        data = path.read_bytes()
        assert hashlib.sha256(data).hexdigest() == f.sha256 and len(data) == f.size_bytes
        assert pq.read_metadata(path).num_rows == f.rows
    assert sum(f.rows for f in files) == sum(b.committed_rows for b in batches) == SIM.events
    assert n_index == SIM.events == summary.accounting["committed"]
    assert all(b.committed_rows == b.accepted for b in batches)  # A3


def test_duplicate_publication_on_retry_is_impossible(source: StorageLayout):
    ingest(source)
    rows = committed_rows(source)
    for _ in range(3):
        again = ingest(source)
        assert again.skipped_batches == 3 and again.files == 0
    assert committed_rows(source) == rows == SIM.events
    with Manifest.open(source) as m:
        assert m.state_counts() == {"COMMITTED": 3}


def test_resume_starts_after_the_committed_frontier(source: StorageLayout):
    with failpoints.armed("before_stage", nth=3), pytest.raises(failpoints.SimulatedCrash):
        ingest(source)
    with Manifest.open(source) as m:
        assert m.committed_frontier("sim-seed5") == 2000


def test_readers_get_a_consistent_snapshot_while_the_writer_commits(source: StorageLayout):
    """A DuckDB connection opened mid-run sees whole batches only."""
    observed: list[int] = []
    real_commit = Manifest.commit

    def spying_commit(self, batch_id, events):
        observed.append(committed_rows(source))  # reader view just before this commit
        return real_commit(self, batch_id, events)

    Manifest.commit = spying_commit  # type: ignore[method-assign]
    try:
        ingest(source)
    finally:
        Manifest.commit = real_commit  # type: ignore[method-assign]
    assert observed == [0, 1000, 2000]
    assert committed_rows(source) == 3000
    con = connect(source)
    con.close()

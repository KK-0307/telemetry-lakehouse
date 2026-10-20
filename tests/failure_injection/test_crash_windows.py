"""Crash at every window of the publication protocol; the visible dataset must never be partial.

Milestone 3 asserts SAFETY: whatever happens, trusted readers see only whole committed batches.
Milestone 4 will extend these scenarios with automatic RECOVERY.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from telemetry_lakehouse import failpoints
from telemetry_lakehouse.config import BatchConfig, SimulatorConfig, StorageConfig
from telemetry_lakehouse.ingest import JsonlSource, run_ingest
from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.manifest import BatchState, InFlightBatchError, Manifest
from telemetry_lakehouse.simulator import write_source
from tests.helpers import committed_rows

SIM = SimulatorConfig(
    devices=20,
    events=3000,
    seed=5,
    rate_per_second=0.05,
    start_time=datetime(2026, 1, 1, 20, tzinfo=UTC),
)
ROOT = Path(__file__).resolve().parents[2]

# Batch 1 spans midnight (2 files), so batch 2's first file is the 3rd `write_tmp_partial` hit.
NTH = {"write_tmp_partial": 3}

# point -> (state of the crashed batch 2, rows visible afterwards)
WINDOWS = {
    "before_stage": (BatchState.VALIDATING, 1000),  # window 1
    "write_tmp_partial": (BatchState.VALIDATING, 1000),  # window 2
    "after_stage": (BatchState.STAGED, 1000),  # window 3
    "after_place": (BatchState.VERIFIED, 1000),  # window 3 (files already in data/)
    "in_commit": (BatchState.VERIFIED, 1000),  # window 4
    "after_commit": (BatchState.COMMITTED, 2000),  # window 5
}


def ingest(layout: StorageLayout):
    return run_ingest(
        JsonlSource.open(layout.default_source),
        layout,
        BatchConfig(batch_size=1000),
        StorageConfig(),
    )


def assert_state_after_crash(layout: StorageLayout, point: str):
    state, visible = WINDOWS[point]
    assert committed_rows(layout) == visible
    with Manifest.open(layout) as m:
        batches = m.batches()
        assert batches[0].state is BatchState.COMMITTED  # batch 1 untouched
        assert batches[1].state is state
        assert len(batches) == 2  # batch 3 never started
        # Identities are committed with their batch or not at all.
        n = m._con.execute("SELECT count(*) FROM event_index").fetchone()[0]
        assert n == visible


@pytest.fixture
def source(layout: StorageLayout) -> StorageLayout:
    write_source(SIM, layout.default_source)
    return layout


@pytest.mark.parametrize("point", WINDOWS)
def test_in_process_crash_never_exposes_partial_data(source: StorageLayout, point: str):
    with failpoints.armed(point, nth=NTH.get(point, 2)), pytest.raises(failpoints.SimulatedCrash):
        ingest(source)
    assert_state_after_crash(source, point)


@pytest.mark.parametrize("point", WINDOWS)
def test_real_process_kill_never_exposes_partial_data(source: StorageLayout, point: str):
    """`os._exit(137)` mid-protocol: no cleanup code runs; only SQLite WAL recovery."""
    env = {
        **os.environ,
        "PYTHONPATH": str(ROOT / "src"),
        "TELEMETRY_FAILPOINT": f"{point}:{NTH.get(point, 2)}",
    }
    proc = subprocess.run(
        [sys.executable, "-m", "telemetry_lakehouse.cli", "ingest", "--root", str(source.root),
         "--batch-size", "1000"],
        env=env, capture_output=True, text=True, timeout=120,
    )  # fmt: skip
    assert proc.returncode == 137, proc.stderr[-2000:]
    assert_state_after_crash(source, point)


@pytest.mark.parametrize("point", [p for p in WINDOWS if p != "after_commit"])
def test_ingest_refuses_to_proceed_past_an_unfinished_batch(source: StorageLayout, point: str):
    with failpoints.armed(point, nth=NTH.get(point, 2)), pytest.raises(failpoints.SimulatedCrash):
        ingest(source)
    visible = committed_rows(source)
    with pytest.raises(InFlightBatchError) as exc:
        ingest(source)  # safe: it does not guess, delete, or double-publish
    assert exc.value.state is WINDOWS[point][0]
    assert committed_rows(source) == visible


def test_crash_after_commit_before_ack_retry_is_idempotent(source: StorageLayout):
    """Window 5: the batch is durably committed; the producer retries; nothing is duplicated."""
    with failpoints.armed("after_commit", nth=2), pytest.raises(failpoints.SimulatedCrash):
        ingest(source)
    assert committed_rows(source) == 2000
    retry = ingest(source)
    assert retry.skipped_batches == 2  # batches 1 and 2 recognised as COMMITTED
    assert retry.accounting["committed"] == 1000  # only batch 3 is new
    assert committed_rows(source) == 3000
    with Manifest.open(source) as m:
        assert m.state_counts() == {"COMMITTED": 3}
        dup = m._con.execute(
            "SELECT count(*) FROM (SELECT event_id FROM event_index GROUP BY 1 HAVING count(*) > 1)"
        ).fetchone()[0]
        assert dup == 0

"""Crash at every window, recover, finish - and end up exactly where a clean run would have."""

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
from telemetry_lakehouse.manifest import BatchState, Manifest
from telemetry_lakehouse.reader import connect
from telemetry_lakehouse.recovery import check_committed_files, recover
from telemetry_lakehouse.simulator import reference_table, write_source

SIM = SimulatorConfig(
    devices=20,
    events=3000,
    seed=5,
    rate_per_second=0.05,
    start_time=datetime(2026, 1, 1, 20, tzinfo=UTC),
)
ROOT = Path(__file__).resolve().parents[2]
NTH = {"write_tmp_partial": 3}  # batch 1 spans midnight => batch 2's first file is hit #3
EXPECTED_ACTIONS = {
    "before_stage": ["interrupted_for_replay", "replayed"],
    "write_tmp_partial": ["interrupted_for_replay", "replayed"],
    "after_stage": ["resumed_and_committed"],
    "after_place": ["resumed_and_committed"],
    "in_commit": ["resumed_and_committed"],
    "after_commit": [],
}


def ingest(layout: StorageLayout):
    return run_ingest(
        JsonlSource.open(layout.default_source),
        layout,
        BatchConfig(batch_size=1000),
        StorageConfig(),
    )


def do_recover(layout: StorageLayout, **kw):
    source = kw.pop("source", JsonlSource.open(layout.default_source))
    with Manifest.open(layout) as m:
        return recover(layout, m, source=source, **kw)


def crash(layout: StorageLayout, point: str):
    with failpoints.armed(point, nth=NTH.get(point, 2)), pytest.raises(failpoints.SimulatedCrash):
        ingest(layout)


def published(layout: StorageLayout) -> dict[str, dict]:
    con = connect(layout)
    try:
        t = con.execute("SELECT * EXCLUDE (event_date) FROM events").to_arrow_table()
    finally:
        con.close()
    return {r["event_id"]: r for r in t.to_pylist()}


def digests(layout: StorageLayout) -> dict[str, tuple[str, str]]:
    with Manifest.open(layout) as m:
        rows = m.batches(BatchState.COMMITTED)
        return {b.batch_id: (b.output_digest, b.logical_digest) for b in rows}


def assert_pristine(layout: StorageLayout):
    with Manifest.open(layout) as m:
        assert m.state_counts() == {"COMMITTED": 3}
        assert m.check_consistency() == []
        assert check_committed_files(layout, m) == []
        committed = {f.relative_path for f in m.committed_files()}
    on_disk = {
        p.relative_to(layout.data_dir).as_posix() for p in layout.data_dir.rglob("*") if p.is_file()
    }
    assert on_disk == committed  # no orphans, no strays
    assert not [p for p in layout.staging_dir.rglob("*") if p.is_file()]
    assert published(layout).keys() == {r["event_id"] for r in reference_table(SIM).to_pylist()}


@pytest.fixture
def source(layout: StorageLayout) -> StorageLayout:
    write_source(SIM, layout.default_source)
    return layout


@pytest.fixture
def clean_digests(tmp_path: Path):
    lay = StorageLayout(tmp_path / "pristine")
    lay.ensure()
    write_source(SIM, lay.default_source)
    ingest(lay)
    return digests(lay)


@pytest.mark.parametrize("point", EXPECTED_ACTIONS)
def test_crash_recover_finish_equals_a_clean_run(source, clean_digests, point):
    crash(source, point)
    report = do_recover(source)

    assert [a["action"] for a in report.actions] == EXPECTED_ACTIONS[point]
    assert report.ok and report.unresolved == []
    ingest(source)  # the producer simply carries on
    assert_pristine(source)
    # Same batches, same logical content, and even byte-identical Parquet as a run with no crash.
    assert digests(source) == clean_digests


@pytest.mark.parametrize("point", EXPECTED_ACTIONS)
def test_real_process_kill_then_recover(source, clean_digests, point):
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
    assert proc.returncode == 137
    assert do_recover(source).ok
    ingest(source)
    assert_pristine(source)
    assert digests(source) == clean_digests


def test_recovery_is_idempotent(source):
    crash(source, "after_place")
    first = do_recover(source)
    second = do_recover(source)
    assert first.actions and second.actions == [] and second.ok
    ingest(source)
    assert_pristine(source)


def test_recovery_without_a_source_resumes_what_it_can_and_reports_the_rest(source):
    crash(source, "after_place")
    with Manifest.open(source) as m:
        report = recover(source, m, source=None)
    assert report.ok and [a["action"] for a in report.actions] == ["resumed_and_committed"]

    other = StorageLayout(source.root.parent / "other")
    other.ensure()
    write_source(SIM, other.default_source)
    crash(other, "before_stage")
    with Manifest.open(other) as m:
        report = recover(other, m, source=None)
    assert not report.ok and "needs source" in report.unresolved[0]
    with Manifest.open(other) as m:
        assert [b.state for b in m.in_flight()] == [BatchState.INTERRUPTED]
    # ...and the source can be supplied later, by recover or simply by the next ingest.
    ingest(other)
    assert_pristine(other)


@pytest.mark.parametrize(
    ("crash_point", "recovery_point"),
    [("after_place", "recover_before_commit"), ("before_stage", "recover_after_interrupt")],
)
def test_crash_during_recovery_is_safe_and_recoverable(source, crash_point, recovery_point):
    crash(source, crash_point)
    with failpoints.armed(recovery_point), pytest.raises(failpoints.SimulatedCrash):
        do_recover(source)
    assert len(published(source)) == 1000  # still only the first batch is visible
    assert do_recover(source).ok
    ingest(source)
    assert_pristine(source)


def test_corrupted_staged_file_forces_a_replay_and_keeps_the_evidence(source, clean_digests):
    crash(source, "after_stage")
    staged = next(source.staging_dir.rglob("*.parquet"))
    data = bytearray(staged.read_bytes())
    data[len(data) // 2] ^= 0xFF
    staged.write_bytes(bytes(data))

    report = do_recover(source)
    assert [a["action"] for a in report.actions] == ["interrupted_for_replay", "replayed"]
    ingest(source)
    assert_pristine(source)
    assert digests(source) == clean_digests
    # The failed attempt is preserved, not erased.
    remnants = list(source.rejected_dir.rglob("staging-remnants"))
    assert len(remnants) == 1 and (remnants[0].parent / "interrupted.json").exists()
    with Manifest.open(source) as m:
        log = m.attempt_log()
    assert [(o, d["state_at_crash"]) for _, _, o, d in log] == [("INTERRUPTED", "STAGED")]


def test_damaged_placed_copy_is_quarantined_and_replaced_from_the_intact_staged_copy(source):
    crash(source, "after_place")
    placed = next(
        p for p in source.data_dir.rglob("*.parquet") if "b" in p.stem and p.stat().st_size
    )
    with Manifest.open(source) as m:
        victim = m.staged_files(m.in_flight()[0].batch_id)[0]
    placed = source.data_path(victim.relative_path)
    placed.unlink()  # break the hard link first so the staged copy stays intact
    placed.write_bytes(b"not a parquet file")

    report = do_recover(source)
    assert [a["action"] for a in report.actions] == ["resumed_and_committed"]
    assert any(
        p.read_bytes() == b"not a parquet file" for p in source.rejected_dir.rglob("*.parquet")
    )
    ingest(source)
    assert_pristine(source)


def test_orphans_are_moved_to_evidence_never_deleted(source):
    ingest(source)
    victim = next(source.data_dir.rglob("*.parquet"))
    planted = victim.with_name("zz-planted.parquet")
    planted.write_bytes(victim.read_bytes())
    tmp = victim.with_name(".torn.123.tmp")
    tmp.write_bytes(b"half")
    stale = source.staging_dir / "bdeadbeef" / "event_date=2026-01-01"
    stale.mkdir(parents=True)
    (stale / "bdeadbeef.parquet").write_bytes(b"leftover")

    report = do_recover(source)
    assert not planted.exists() and not tmp.exists() and not stale.exists()
    assert report.orphans_quarantined == [planted.relative_to(source.data_dir).as_posix()]
    assert (source.rejected_dir / "orphans" / planted.relative_to(source.data_dir)).exists()
    assert report.staging_cleaned == ["bdeadbeef"]
    assert_pristine(source)


# ---------------------------------------------------------------- committed files go bad


@pytest.fixture
def committed(source) -> StorageLayout:
    ingest(source)
    return source


def first_file(layout: StorageLayout) -> Path:
    with Manifest.open(layout) as m:
        return layout.data_path(m.committed_files()[0].relative_path)


def test_missing_parquet_file_is_detected_and_repaired_by_replay(committed):
    target = first_file(committed)
    original = target.read_bytes()
    target.unlink()
    with Manifest.open(committed) as m:
        assert [p.kind for p in check_committed_files(committed, m)] == ["MISSING"]

    report = do_recover(committed, repair=True)
    assert report.repaired == [target.relative_to(committed.data_dir).as_posix()] and report.ok
    assert target.read_bytes() == original  # byte-identical restoration
    assert_pristine(committed)


@pytest.mark.parametrize("damage", ["flip", "truncate", "empty"])
def test_corrupted_parquet_file_is_detected_and_repaired_by_replay(committed, damage):
    target = first_file(committed)
    original = target.read_bytes()
    if damage == "flip":
        b = bytearray(original)
        b[len(b) // 3] ^= 0x55
        target.write_bytes(bytes(b))
    elif damage == "truncate":
        target.write_bytes(original[: len(original) // 2])
    else:
        target.write_bytes(b"")
    with Manifest.open(committed) as m:
        kinds = [p.kind for p in check_committed_files(committed, m)]
    assert kinds == ["SHA_MISMATCH" if damage == "flip" else "SIZE_MISMATCH"]

    detect_only = do_recover(committed)  # without --repair nothing is "fixed" silently
    assert not detect_only.ok and target.read_bytes() != original

    repaired = do_recover(committed, repair=True)
    assert repaired.ok and target.read_bytes() == original
    assert list(committed.rejected_dir.glob("orphans/**/*.parquet"))  # the damaged copy is kept
    assert_pristine(committed)


def test_repair_is_refused_when_the_source_no_longer_reproduces_the_batch(committed):
    target = first_file(committed)
    target.write_bytes(b"garbage")
    lines = committed.default_source.read_bytes().splitlines(keepends=True)
    lines[10] = lines[10].replace(b'"humidity_pct":', b'"humidity_pct":1.5,"x":')  # now invalid
    committed.default_source.write_bytes(b"".join(lines))
    report = do_recover(committed, repair=True)
    assert not report.ok and report.repaired == []
    assert "no longer reproduces" in report.unresolved[0]
    assert target.read_bytes() == b"garbage"  # nothing was overwritten with something unproven


def test_manifest_cross_table_inconsistency_is_reported(committed):
    with Manifest.open(committed) as m:
        m._con.execute(
            "DELETE FROM event_index WHERE event_id = (SELECT event_id FROM event_index LIMIT 1)"
        )
    report = do_recover(committed)
    assert not report.ok and "event_index rows" in report.consistency_problems[0]


def test_repair_refuses_bytes_that_differ_from_the_committed_fingerprint(committed):
    """Same logical rows but a different codec => different bytes => not the committed file."""
    target = first_file(committed)
    target.write_bytes(b"garbage")
    report = do_recover(committed, repair=True, storage=StorageConfig(compression="snappy"))
    assert not report.ok and report.repaired == []
    assert "does not reproduce the committed bytes" in report.unresolved[0]
    assert target.read_bytes() == b"garbage"


@pytest.mark.parametrize("column", ["logical_digest", "accepted"])
def test_intact_files_that_do_not_match_what_the_gate_approved_are_not_resumed(source, column):
    """Hash-clean files are not enough: their *content* must equal the validated content."""
    crash(source, "after_stage")
    with Manifest.open(source) as m:
        batch_id = m.in_flight()[0].batch_id
        if column == "logical_digest":
            m._con.execute("UPDATE batches SET logical_digest='00' WHERE batch_id=?", (batch_id,))
        else:
            m._con.execute("UPDATE batches SET accepted=accepted-1 WHERE batch_id=?", (batch_id,))
    report = do_recover(source)
    assert [a["action"] for a in report.actions] == ["interrupted_for_replay", "replayed"]
    ingest(source)
    assert published(source).keys() == {r["event_id"] for r in reference_table(SIM).to_pylist()}

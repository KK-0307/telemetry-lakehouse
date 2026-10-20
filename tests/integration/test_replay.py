"""Deterministic replay: the source plus the manifest reproduce the committed dataset exactly."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from telemetry_lakehouse.cli import EXIT_INTEGRITY, EXIT_OK, main
from telemetry_lakehouse.config import (
    BatchConfig,
    FaultConfig,
    GateConfig,
    SimulatorConfig,
    StorageConfig,
)
from telemetry_lakehouse.ingest import JsonlSource, run_ingest
from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.manifest import BatchState, Manifest
from telemetry_lakehouse.recovery import replay_committed
from tests.helpers import build_source

SIM = SimulatorConfig(
    devices=30,
    events=4000,
    seed=17,
    rate_per_second=0.05,
    start_time=datetime(2026, 1, 1, 20, tzinfo=UTC),
)


def ingest(layout, batch_size=900, **gate):
    return run_ingest(
        JsonlSource.open(layout.default_source),
        layout,
        BatchConfig(batch_size=batch_size),
        StorageConfig(),
        GateConfig(**gate),
    )


def replay(layout, **kw):
    with Manifest.open(layout) as m:
        return replay_committed(
            layout, m, JsonlSource.open(layout.default_source), StorageConfig(), **kw
        )


def test_replaying_every_committed_batch_reproduces_it_exactly(layout):
    build_source(layout, SIM)
    summary = ingest(layout)
    results = replay(layout)
    assert len(results) == summary.batches == 5
    assert all(r.ok and r.logical_match and r.bytes_match for r in results)
    assert sum(r.rows for r in results) == SIM.events


def test_replay_reproduces_batches_that_survived_faults_and_quarantine(layout):
    faults = FaultConfig(
        duplicate=0.03, reorder=0.05, malformed=0.01, invalid_value=0.01, conflict=0.005, drop=0.01
    )
    build_source(layout, SIM, faults)
    summary = ingest(layout, mode="quarantine")
    assert summary.accounting["rejected"] > 0 and summary.accounting["duplicates"] > 0
    results = replay(layout)
    assert results and all(r.ok for r in results)


def test_two_independent_runs_produce_identical_digests(tmp_path):
    runs = []
    for name in ("a", "b"):
        lay = StorageLayout(tmp_path / name)
        build_source(lay, SIM, FaultConfig(duplicate=0.02, reorder=0.05))
        ingest(lay)
        with Manifest.open(lay) as m:
            runs.append({b.batch_id: (b.output_digest, b.logical_digest) for b in m.batches()})
    assert runs[0] == runs[1]


def test_replay_detects_a_source_that_changed_since_commit(layout):
    build_source(layout, SIM)
    ingest(layout)
    lines = layout.default_source.read_bytes().splitlines(keepends=True)
    changed = json.loads(lines[100])
    changed["temperature_c"] = round(changed["temperature_c"] + 1, 2)
    lines[100] = json.dumps(changed, separators=(",", ":")).encode() + b"\n"
    layout.default_source.write_bytes(b"".join(lines))
    results = replay(layout)
    bad = [r for r in results if not r.ok]
    assert [r.offsets for r in bad] == [(0, 900)]
    assert "no longer reproduces" in bad[0].detail
    assert all(r.ok for r in results if r.offsets != (0, 900))


def test_replay_of_one_batch_by_id(layout):
    build_source(layout, SIM)
    ingest(layout)
    with Manifest.open(layout) as m:
        target = m.batches(BatchState.COMMITTED)[2].batch_id
    results = replay(layout, batch_id=target)
    assert [r.batch_id for r in results] == [target] and results[0].ok


def test_replay_never_touches_the_published_dataset(layout):
    build_source(layout, SIM)
    ingest(layout)
    before = {p: p.read_bytes() for p in layout.data_dir.rglob("*") if p.is_file()}
    replay(layout)
    assert {p: p.read_bytes() for p in layout.data_dir.rglob("*") if p.is_file()} == before
    assert not list(layout.root.glob(".replay-*"))  # scratch space is cleaned up


def test_cli_replay_and_recover(tmp_path, capsys):
    root = str(tmp_path / "lh")
    main(["simulate", "--root", root, "--devices", "20", "--events", "1500", "--rate", "0.05"])
    main(["ingest", "--root", root, "--batch-size", "500"])
    capsys.readouterr()

    assert main(["replay", "--root", root]) == EXIT_OK
    out = json.loads(capsys.readouterr().out)
    assert (
        out["batches"] == out["identical"] == 3
        and out["rows"] == 1500
        and out["rows_per_second"] > 0
    )

    assert main(["recover", "--root", root]) == EXIT_OK
    assert json.loads(capsys.readouterr().out)["ok"] is True

    victim = next(Path(root, "data").rglob("*.parquet"))
    victim.write_bytes(b"x")
    assert main(["recover", "--root", root]) == EXIT_INTEGRITY  # detected, not hidden
    capsys.readouterr()
    assert main(["recover", "--root", root, "--repair"]) == EXIT_OK
    assert json.loads(capsys.readouterr().out)["repaired"]


def test_cli_replay_without_a_source_is_a_usage_error(tmp_path, capsys):
    assert main(["replay", "--root", str(tmp_path / "none")]) == 2


@pytest.mark.parametrize("cmd", ["recover", "replay", "status"])
def test_cli_commands_work_on_an_empty_lakehouse(tmp_path, capsys, cmd):
    code = main([cmd, "--root", str(tmp_path / "empty")])
    assert code in (EXIT_OK, 2, EXIT_INTEGRITY)

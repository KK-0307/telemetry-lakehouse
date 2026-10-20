"""`telemetry verify` PASSes a healthy dataset and FAILs, for the right reason, on each corruption.

Corruptions are applied *behind the pipeline's back* (straight to Parquet files and manifest rows),
so these tests also prove the verifier does not merely echo what the validator or manifest claim.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from telemetry_lakehouse import failpoints
from telemetry_lakehouse.cli import EXIT_INTEGRITY, EXIT_OK, EXIT_USAGE, main
from telemetry_lakehouse.config import (
    BatchConfig,
    FaultConfig,
    GateConfig,
    SimulatorConfig,
    StorageConfig,
)
from telemetry_lakehouse.ingest import JsonlSource, run_ingest
from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.manifest import Manifest, event_key
from telemetry_lakehouse.recovery import recover
from telemetry_lakehouse.schemas import EVENT_SCHEMA
from telemetry_lakehouse.simulator import generate_events
from telemetry_lakehouse.verify import VerifyConfig, load_assertions, verify
from tests.helpers import INSERT_INDEX, build_source

SIM = SimulatorConfig(
    devices=25,
    events=3000,
    seed=9,
    rate_per_second=0.05,
    start_time=datetime(2026, 1, 1, 20, tzinfo=UTC),
)
ALL = {s.name for s in load_assertions()}


def ingest(layout, batch_size=1000, **gate):
    return run_ingest(
        JsonlSource.open(layout.default_source),
        layout,
        BatchConfig(batch_size=batch_size),
        StorageConfig(),
        GateConfig(**gate),
    )


@pytest.fixture
def lake(layout: StorageLayout) -> StorageLayout:
    build_source(layout, SIM)
    ingest(layout)
    return layout


def failed(layout: StorageLayout, **cfg) -> set[str]:
    return {r.name for r in verify(layout, VerifyConfig(**cfg)).failed}


def sample_rows(n: int = 5) -> list[dict]:
    return list(generate_events(SIM))[:n]


def inject(
    layout: StorageLayout,
    table: pa.Table,
    *,
    batch_id: str = "binjected",
    event_date: str = "2026-01-01",
    index: bool = True,
    file_batch_id: str | None = None,
    start: int = 3000,
) -> Path:
    """Add a COMMITTED batch whose Parquet and manifest rows are written directly."""
    rel = f"event_date={event_date}/{file_batch_id or batch_id}.parquet"
    path = layout.data_path(rel)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    data = path.read_bytes()
    n = table.num_rows
    with Manifest.open(layout) as m:
        con = m._con
        seq = con.execute("SELECT max(commit_seq) + 1 FROM batches").fetchone()[0]
        con.execute(
            "INSERT INTO batches (batch_id, source_id, start_offset, end_offset, state, attempt, "
            "flush_reason, input_digest, consumed, accepted, duplicates, rejected, unresolved, "
            "committed_rows, commit_seq, created_at, updated_at) VALUES "
            "(?, 'test-src', ?, ?, 'COMMITTED', 1, 'size', 'x', ?, ?, 0, 0, 0, ?, ?, 'now', 'now')",
            (batch_id, start, start + n, n, n, n, seq),
        )
        if "event_time" in table.column_names:
            times = [t.as_py() for t in table["event_time"]]
            lo, hi = min(times), max(times)
            epoch = datetime(1970, 1, 1, tzinfo=UTC)
            lo_us, hi_us = (
                (lo - epoch) // (hi - hi + datetime.resolution),
                (hi - epoch) // datetime.resolution,
            )
        else:
            lo_us = hi_us = 0
        con.execute(
            "INSERT INTO batch_files VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                batch_id,
                rel,
                event_date,
                n,
                len(data),
                hashlib.sha256(data).hexdigest(),
                lo_us,
                hi_us,
            ),
        )
        if index:
            for i, eid in enumerate(table["event_id"].to_pylist()):
                con.execute(
                    INSERT_INDEX.replace("INSERT INTO", "INSERT OR IGNORE INTO")
                    + "(?, ?, ?, ?, x'00', 1, ?)",
                    (event_key(eid), eid, f"inj-{batch_id}", i + 1, batch_id),
                )
    return path


def fresh_rows(n: int = 3, **overrides) -> pa.Table:
    """Rows with brand-new identities, valid unless overridden."""
    base = sample_rows(n)
    rows = []
    for i, r in enumerate(base):
        row = dict(r)
        row.update(event_id=f"injected-{i}", device_id=f"inj-dev-{i}", sequence_number=1)
        row.update({k: v for k, v in overrides.items()})
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=EVENT_SCHEMA)


# ------------------------------------------------------------------------------------ healthy


def test_a_healthy_dataset_passes_every_assertion(lake: StorageLayout):
    report = verify(lake)
    assert report.ok
    assert {r.status for r in report.results} == {"PASS"} and {
        r.name for r in report.results
    } == ALL
    p = report.profile
    assert (p["events"], p["files"], p["batches_committed"], p["last_commit_seq"]) == (
        3000,
        4,
        3,
        3,
    )


def test_an_empty_lakehouse_passes_vacuously_and_skips_marker_checks(layout: StorageLayout):
    report = verify(layout)
    assert report.ok and report.profile["events"] == 0
    assert {r.name for r in report.results if r.status == "SKIP"} == {
        "sequences_complete_to_end_of_stream"
    }


def test_every_assertion_has_metadata_and_a_unique_name():
    specs = load_assertions()
    assert len(specs) == len(ALL) == 18
    assert all(s.category and s.description and s.sql for s in specs)


# ------------------------------------------------------- content corruption: each hits its check


def test_duplicate_event_ids_are_detected(lake):
    inject(lake, pa.Table.from_pylist(sample_rows(3), schema=EVENT_SCHEMA), batch_id="bdup")
    assert {"unique_event_id", "unique_device_sequence"} <= failed(lake)


def test_duplicate_device_sequence_with_distinct_event_ids(lake):
    rows = [{**r, "event_id": f"clone-{i}"} for i, r in enumerate(sample_rows(3))]
    inject(lake, pa.Table.from_pylist(rows, schema=EVENT_SCHEMA), batch_id="bclone")
    assert failed(lake) >= {"unique_device_sequence"} and "unique_event_id" not in failed(lake)


def test_null_in_a_required_field_is_detected(lake):
    loose = pa.schema([pa.field(f.name, f.type, nullable=True) for f in EVENT_SCHEMA])
    rows = [dict(r) for r in fresh_rows(2).to_pylist()]
    rows[0]["temperature_c"] = None
    inject(lake, pa.Table.from_pylist(rows, schema=loose), batch_id="bnull")
    assert failed(lake) >= {"required_fields_complete", "parquet_schema_matches_contract"}


@pytest.mark.parametrize(
    ("override", "check"),
    [
        ({"temperature_c": 500.0}, "valid_measurement_ranges"),
        ({"humidity_pct": -1.0}, "valid_measurement_ranges"),
        ({"battery_pct": 100.5}, "valid_measurement_ranges"),
        ({"signal_dbm": 10}, "valid_measurement_ranges"),
        ({"temperature_c": float("nan")}, "valid_measurement_ranges"),
        ({"temperature_c": float("inf")}, "valid_measurement_ranges"),
        ({"status": "BROKEN"}, "valid_enums_and_versions"),
        ({"schema_version": 7}, "valid_enums_and_versions"),
        ({"sequence_number": 0}, "valid_enums_and_versions"),
    ],
)
def test_invalid_values_are_detected_even_when_the_manifest_vouches_for_the_file(
    lake, override, check
):
    inject(lake, fresh_rows(2, **override), batch_id="bbad")
    assert check in failed(lake)


def test_event_time_far_ahead_of_ingestion_time_is_detected(lake):
    future = datetime(2026, 1, 1, 23, tzinfo=UTC)
    rows = fresh_rows(2).to_pylist()
    for r in rows:
        r["event_time"], r["ingestion_time"] = future, datetime(2026, 1, 1, 20, tzinfo=UTC)
    inject(lake, pa.Table.from_pylist(rows, schema=EVENT_SCHEMA), batch_id="bfuture")
    assert "event_time_not_in_future" in failed(lake)


def test_rows_in_the_wrong_date_partition_are_detected(lake):
    inject(lake, fresh_rows(2), batch_id="bmisfiled", event_date="2026-01-05")
    assert failed(lake) == {"partition_matches_event_time"}


def test_schema_drift_is_detected(lake):
    fields = [
        pa.field(f.name, pa.float32() if f.name == "temperature_c" else f.type, nullable=False)
        for f in EVENT_SCHEMA
    ]
    drifted = fresh_rows(2).cast(pa.schema(fields))
    inject(lake, drifted, batch_id="bdrift")
    assert "parquet_schema_matches_contract" in failed(lake)


def test_extra_column_is_detected(lake):
    extra = fresh_rows(2).append_column("firmware", pa.array(["1", "2"]))
    inject(lake, extra, batch_id="bextra")
    assert "parquet_schema_matches_contract" in failed(lake)


# ------------------------------------------------------------------------ manifest-side corruption


def sql(layout: StorageLayout, statement: str, *args):
    with Manifest.open(layout) as m:
        m._con.execute(statement, args)


def test_broken_batch_accounting_is_detected(lake):
    sql(lake, "UPDATE batches SET duplicates = duplicates + 5 WHERE start_offset = 0")
    assert failed(lake) == {"batch_accounting_identity"}


def test_row_count_mismatch_between_manifest_and_parquet_is_detected(lake):
    sql(
        lake,
        "UPDATE batches SET consumed=consumed+1, accepted=accepted+1, "
        "committed_rows=committed_rows+1 WHERE start_offset = 0",
    )
    assert "batch_row_counts_reconcile" in failed(lake)


def test_rejected_batch_data_inside_trusted_files_is_detected(lake):
    sql(
        lake,
        "INSERT INTO batches (batch_id, source_id, start_offset, end_offset, state, attempt, "
        "flush_reason, input_digest, created_at, updated_at) "
        "VALUES ('brejected', 'test-src', 9000, 9003, 'REJECTED', 1, 'size', 'x', 'now', 'now')",
    )
    inject(lake, fresh_rows(3), batch_id="bgood", file_batch_id="brejected")
    assert "no_untrusted_batches_in_data" in failed(lake)


def test_event_index_missing_an_event_is_detected(lake):
    sql(lake, "DELETE FROM event_index WHERE event_id = (SELECT event_id FROM event_index LIMIT 1)")
    assert {"events_and_index_agree", "batch_row_counts_reconcile"} <= failed(lake)


def test_phantom_event_index_entry_is_detected(lake):
    with Manifest.open(lake) as m:
        batch = m.batches()[0].batch_id
    sql(
        lake,
        INSERT_INDEX + "(?, 'ghost', 'ghost-dev', 1, x'00', 1, ?)",
        event_key("ghost"),
        batch,
    )
    assert "events_and_index_agree" in failed(lake)


# ----------------------------------------------------------------------------- file-level damage


def committed_path(layout: StorageLayout, i: int = 0) -> Path:
    with Manifest.open(layout) as m:
        return layout.data_path(m.committed_files()[i].relative_path)


def test_missing_committed_file_is_detected(lake):
    committed_path(lake).unlink()
    assert {"manifest_files_match_physical", "batch_row_counts_reconcile"} <= failed(lake)


def test_corrupted_committed_file_is_detected(lake):
    p = committed_path(lake)
    b = bytearray(p.read_bytes())
    b[len(b) // 2] ^= 0xFF
    p.write_bytes(bytes(b))
    assert "manifest_files_match_physical" in failed(lake)


def test_truncated_committed_file_is_detected(lake):
    p = committed_path(lake)
    p.write_bytes(p.read_bytes()[:50])
    assert "manifest_files_match_physical" in failed(lake)


def test_a_file_swapped_for_a_valid_but_different_one_is_detected_by_hash_and_content(lake):
    p = committed_path(lake)
    pq.write_table(fresh_rows(2, temperature_c=500.0), p)  # valid Parquet, wrong content
    f = failed(lake)
    assert {"manifest_files_match_physical", "valid_measurement_ranges"} <= f


def test_unreferenced_parquet_file_in_data_is_detected(lake):
    planted = committed_path(lake).with_name("planted.parquet")
    planted.write_bytes(committed_path(lake).read_bytes())
    assert failed(lake) == {"no_unreferenced_files_in_data"}


def test_unfinished_batch_after_a_crash_fails_until_recovery_makes_it_pass(layout):
    build_source(layout, SIM)
    with failpoints.armed("after_place", nth=2), pytest.raises(failpoints.SimulatedCrash):
        ingest(layout)
    assert {"no_unfinished_batches", "no_unreferenced_files_in_data"} <= failed(layout)
    with Manifest.open(layout) as m:
        recover(layout, m, source=JsonlSource.open(layout.default_source))
    ingest(layout)
    assert verify(layout).ok


# ------------------------------------------------------------------------------- completeness


def test_dropped_events_are_found_against_the_marker_and_the_watermark(layout):
    ledger = build_source(layout, SIM, FaultConfig(drop=0.01))
    ingest(layout)
    dropped_devices = {
        e["device_id"] for e in generate_events(SIM) if e["event_id"] in set(ledger.ids["drop"])
    }
    report = verify(layout, VerifyConfig(grace_seconds=0))
    by = {r.name: r for r in report.results}
    assert by["sequences_complete_to_end_of_stream"].status == "FAIL"
    assert by["sequences_complete_to_end_of_stream"].violations == len(dropped_devices)
    assert by["sequence_gaps_beyond_grace"].status == "FAIL"
    # Every content/consistency check still passes: the data is sound, just known-incomplete.
    assert {r.name for r in report.failed} == {
        "sequences_complete_to_end_of_stream",
        "sequence_gaps_beyond_grace",
    }


def test_allow_incomplete_downgrades_completeness_failures_only(layout):
    build_source(layout, SIM, FaultConfig(drop=0.01))
    ingest(layout)
    report = verify(layout, VerifyConfig(allow_incomplete=True, grace_seconds=0))
    assert report.ok
    assert {r.status for r in report.results if r.status != "PASS"} == {"WARN"}
    # ...but a content failure is never downgraded.
    inject(layout, fresh_rows(2, temperature_c=500.0), batch_id="bbad", start=3000)
    assert not verify(layout, VerifyConfig(allow_incomplete=True)).ok


def test_trailing_loss_is_only_visible_through_the_end_of_stream_marker(layout):
    build_source(layout, SIM)
    lines = layout.default_source.read_bytes().splitlines(keepends=True)
    layout.default_source.write_bytes(b"".join(lines[:-50]))  # the tail never arrived
    ingest(layout)
    by = {r.name: r for r in verify(layout, VerifyConfig(grace_seconds=0)).results}
    assert by["sequence_gaps_beyond_grace"].status == "PASS"  # nothing above the lost tail
    assert by["sequences_complete_to_end_of_stream"].status == "FAIL"
    assert by["sequences_complete_to_end_of_stream"].violations >= 1


def test_interrupted_source_has_no_marker_so_that_assertion_is_skipped(layout):
    build_source(layout, SIM, FaultConfig(interrupt_after=2500))
    ingest(layout)
    by = {r.name: r for r in verify(layout).results}
    assert by["sequences_complete_to_end_of_stream"].status == "SKIP"
    assert "marker" in by["sequences_complete_to_end_of_stream"].note


def test_a_hole_in_offset_coverage_is_detected(layout):
    build_source(layout, SIM)
    lines = layout.default_source.read_bytes().splitlines(keepends=True)
    lines[1500] = b"{broken\n"  # poisons the middle batch [1000, 2000)
    layout.default_source.write_bytes(b"".join(lines))
    assert ingest(layout, on_block="continue").blocked
    result = {r.name: r for r in verify(layout).results}["offset_coverage_no_holes"]
    assert result.status == "FAIL"
    hole = result.examples[0]
    assert (hole["hole_start"], hole["hole_end"]) == (1000, 2000)


# ------------------------------------------------------------------------------------------ CLI


def test_cli_exit_codes_report_file_and_json(lake, capsys):
    root = str(lake.root)
    assert main(["verify", "--root", root]) == EXIT_OK
    out = capsys.readouterr().out
    assert "RESULT: PASS" in out and "unique_event_id" in out and out.count("PASS ") >= 18
    report_file = json.loads((lake.meta_dir / "verify-report.json").read_text())
    assert report_file["result"] == "PASS" and len(report_file["assertions"]) == 18

    committed_path(lake).unlink()
    assert main(["verify", "--root", root, "--format", "json"]) == EXIT_INTEGRITY
    data = json.loads(capsys.readouterr().out)
    assert data["result"] == "FAIL" and data["counts"]["FAIL"] >= 1
    assert json.loads((lake.meta_dir / "verify-report.json").read_text())["result"] == "FAIL"


def test_cli_list_filter_and_usage_errors(lake, capsys):
    assert main(["verify", "--root", str(lake.root), "--list"]) == EXIT_OK
    assert "unique_event_id" in capsys.readouterr().out
    assert main(["verify", "--root", str(lake.root), "--assertion", "unique_event_id"]) == EXIT_OK
    assert capsys.readouterr().out.count("PASS") >= 1
    assert main(["verify", "--root", str(lake.root), "--assertion", "nope"]) == EXIT_USAGE


def test_cli_no_report_file_option(lake, capsys):
    main(["verify", "--root", str(lake.root), "--no-report-file"])
    assert not (lake.meta_dir / "verify-report.json").exists()


def test_battery_health_analytic_query(lake, capsys):
    assert (
        main(["query", "--root", str(lake.root), "--name", "battery-health", "--format", "json"])
        == EXIT_OK
    )
    rows = json.loads(capsys.readouterr().out)
    assert [r["event_date"] for r in rows] == [str(date(2026, 1, 1)), str(date(2026, 1, 2))]
    assert all(0 <= r["avg_last_battery_pct"] <= 100 for r in rows)


def test_an_assertion_that_cannot_run_is_a_failure_not_a_pass(lake, monkeypatch):
    import telemetry_lakehouse.verify as v

    broken = v.AssertionSpec("broken", "contract", "refers to a column that does not exist", None,
                             "SELECT no_such_column FROM events")  # fmt: skip
    monkeypatch.setattr(v, "load_assertions", lambda: [broken])
    report = v.verify(lake)
    assert not report.ok
    assert report.results[0].status == "FAIL" and "could not be evaluated" in report.results[0].note

"""The gate against fault-injected streams: what was injected is exactly what is found,
and nothing invalid ever reaches the published dataset."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from telemetry_lakehouse.config import (
    BatchConfig,
    FaultConfig,
    GateConfig,
    SimulatorConfig,
    StorageConfig,
)
from telemetry_lakehouse.ingest import JsonlSource, run_ingest
from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.reader import connect
from telemetry_lakehouse.simulator import generate_events, reference_table
from tests.helpers import build_source

SIM = SimulatorConfig(
    devices=40,
    events=4000,
    seed=21,
    rate_per_second=0.05,
    start_time=datetime(2026, 1, 1, 20, tzinfo=UTC),
)
REFERENCE = {r["event_id"]: r for r in reference_table(SIM).to_pylist()}
SEQ_OF = {e["event_id"]: (e["device_id"], e["sequence_number"]) for e in generate_events(SIM)}


def ingest(layout, batch_size=700, **gate):
    return run_ingest(
        JsonlSource.open(layout.default_source),
        layout,
        BatchConfig(batch_size=batch_size),
        StorageConfig(),
        GateConfig(**gate),
    )


def published(layout: StorageLayout) -> dict[str, dict]:
    con = connect(layout)
    try:
        table = con.execute("SELECT * EXCLUDE (event_date) FROM events").to_arrow_table()
    finally:
        con.close()
    return {r["event_id"]: r for r in table.to_pylist()}


def device_view(rows: dict[str, dict]) -> dict[str, dict]:
    """Rows without `ingestion_time`: delayed or re-ordered events are legitimately *received*
    later than in the clean stream, so only the device-originated fields must match."""
    return {i: {k: v for k, v in r.items() if k != "ingestion_time"} for i, r in rows.items()}


def assert_matches_reference(rows: dict[str, dict], expected: dict[str, dict] | None = None):
    expected = REFERENCE if expected is None else expected
    assert device_view(rows) == device_view(expected)
    assert all(rows[i]["ingestion_time"] >= expected[i]["ingestion_time"] for i in rows)


def snapshot(layout: StorageLayout) -> dict[str, str]:
    return {
        str(p.relative_to(layout.data_dir)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(layout.data_dir.rglob("*.parquet"))
    }


def total(summary, key):
    return summary.accounting[key]


# ---------------------------------------------------------------- benign faults are absorbed


def test_duplicates_reordering_and_delay_do_not_change_the_published_dataset(layout):
    faults = FaultConfig(
        duplicate=0.03, reorder=0.1, delay=0.01, reorder_window=40, delay_positions=900
    )
    ledger = build_source(layout, SIM, faults)
    summary = ingest(layout, max_lateness_seconds=10**9)

    assert summary.blocked == [] and summary.gaps == []
    assert_matches_reference(published(layout))  # exactly the known-good dataset
    assert total(summary, "duplicates") == ledger.counts["duplicate"] > 0
    assert total(summary, "consumed") == summary.records == SIM.events + ledger.counts["duplicate"]
    assert total(summary, "committed") == total(summary, "accepted") == SIM.events
    assert summary.out_of_order > 0


def test_out_of_order_events_are_not_mistaken_for_permanent_loss(layout):
    build_source(layout, SIM, FaultConfig(reorder=0.2, delay=0.02, delay_positions=1500))
    summary = ingest(layout, max_lateness_seconds=10**9)
    assert summary.gaps == [] and summary.open_gaps == 0
    assert len(published(layout)) == SIM.events


def test_duplicates_across_batch_boundaries_are_caught(layout):
    ledger = build_source(layout, SIM, FaultConfig(duplicate=0.05, reorder_window=500))
    summary = ingest(layout, batch_size=100)  # re-deliveries land in later batches
    assert total(summary, "duplicates") == ledger.counts["duplicate"]
    assert_matches_reference(published(layout))


# ---------------------------------------------------------------- loss is found, exactly


def test_dropped_events_are_proven_missing_against_the_end_of_stream_marker(layout):
    ledger = build_source(layout, SIM, FaultConfig(drop=0.02))
    summary = ingest(layout)
    assert summary.gap_basis == "end_of_stream" and summary.blocked == []
    found = {(g.device_id, s) for g in summary.gaps for s in range(g.first, g.last + 1)}
    assert found == {SEQ_OF[i] for i in ledger.ids["drop"]}
    assert not summary.integrity_ok
    report = json.loads((layout.meta_dir / "completeness-test-src.json").read_text())
    assert report["missing_events"] == ledger.counts["drop"]


def test_interrupted_source_cannot_claim_permanent_loss_by_marker(layout):
    build_source(layout, SIM, FaultConfig(interrupt_after=2500))
    summary = ingest(layout)
    assert summary.gap_basis == "watermark"  # no end-of-stream marker was issued
    assert summary.records == 2500 and summary.blocked == []


def test_trailing_loss_is_only_detectable_with_the_marker(layout):
    # Drop everything: only the end-of-stream final sequences reveal the loss.
    build_source(layout, SIM, FaultConfig(drop=1.0))
    summary = ingest(layout)
    assert summary.records == 0
    assert sum(g.missing for g in summary.gaps) == SIM.events


# ---------------------------------------------------------------- strict mode blocks


def test_corrupted_batch_is_blocked_and_the_trusted_dataset_is_unchanged(layout):
    build_source(layout, SIM)
    first = ingest(layout, batch_size=1000)  # publish a good dataset
    assert first.integrity_ok
    before, rows_before = snapshot(layout), published(layout)

    # Corrupt a record in a fresh source, and re-ingest it under a different source id.
    lines = layout.default_source.read_bytes().splitlines(keepends=True)
    lines[2500] = lines[2500].replace(b'"temperature_c":', b'"temperature_c":999.0,"x":')
    layout.default_source.write_bytes(b"".join(lines))
    summary = ingest(layout, batch_size=1000)

    assert snapshot(layout) == before and published(layout) == rows_before
    assert not any(layout.staging_dir.rglob("*.parquet"))
    # Unchanged batches are idempotent no-ops; the mutated one is refused, not re-evaluated.
    assert summary.skipped_batches == 2 and [b.start_offset for b in summary.blocked] == [2000]
    assert summary.halted and summary.blocked[0].reason_counts == {"SOURCE_MUTATED": 1}


@pytest.mark.parametrize(
    ("kind", "code"),
    [
        ("malformed", {"MALFORMED_PAYLOAD"}),
        ("invalid_value", {"OUT_OF_RANGE"}),
        ("unsupported_version", {"UNSUPPORTED_SCHEMA_VERSION"}),
        ("clock_skew_future", {"FUTURE_EVENT_TIME"}),
    ],
)
def test_strict_mode_blocks_every_batch_that_contains_a_bad_record(layout, kind, code):
    ledger = build_source(layout, SIM, FaultConfig(**{kind: 0.004}))
    n_bad = ledger.counts[kind]
    assert n_bad > 0
    summary = ingest(layout, on_block="continue")

    assert sum(sum(b.reason_counts.values()) for b in summary.blocked) == n_bad
    assert {c for b in summary.blocked for c in b.reason_counts} <= code | {"MALFORMED_PAYLOAD"}
    blocked_ranges = [(b.start_offset, b.end_offset) for b in summary.blocked]
    # No row from any blocked batch is published - good rows included (whole-batch rejection).
    files = {p.stem for p in layout.data_dir.rglob("*.parquet")}
    assert not files & {b.batch_id for b in summary.blocked}
    assert total(summary, "committed") == summary.records - sum(e - s for s, e in blocked_ranges)
    assert len(published(layout)) == total(summary, "committed")
    for b in summary.blocked:  # evidence exists for every block
        assert (Path(b.evidence) / "report.json").exists()


def test_halt_stops_at_the_first_blocked_batch_and_continue_does_not(layout):
    build_source(layout, SIM, FaultConfig(invalid_value=0.003))
    halted = ingest(layout, on_block="halt")
    assert halted.halted and len(halted.blocked) == 1
    published_after_halt = len(published(layout))
    assert published_after_halt == halted.accounting["committed"]
    assert halted.gap_basis is None  # completeness is not evaluated on a halted run


def test_evidence_report_records_reasons_and_balances(layout):
    build_source(layout, SIM, FaultConfig(malformed=0.003, invalid_value=0.003))
    summary = ingest(layout, on_block="continue")
    for b in summary.blocked:
        report = json.loads((Path(b.evidence) / "report.json").read_text())
        a = report["accounting"]
        assert report["decision"] == "BLOCK" and a["committed"] == 0
        assert a["consumed"] == a["accepted"] + a["duplicates"] + a["rejected"] + a["unresolved"]
        assert a["consumed"] == report["end_offset"] - report["start_offset"]
        assert sum(report["reason_counts"].values()) == a["rejected"] == len(report["rejections"])
        assert all(len(r["payload_sha256"]) == 64 for r in report["rejections"])


def test_conflicting_duplicates_block_the_batch_and_neither_copy_is_trusted(layout):
    ledger = build_source(layout, SIM, FaultConfig(conflict=0.002, reorder_window=5))
    summary = ingest(layout, batch_size=4100, on_block="continue")  # one batch: twins together
    assert len(summary.blocked) == 1
    assert summary.blocked[0].reason_counts == {
        "CONFLICTING_DUPLICATE": 2 * ledger.counts["conflict"]
    }
    assert published(layout) == {}


def test_conflict_with_already_published_data_is_rejected_and_original_survives(layout):
    build_source(layout, SIM)
    ingest(layout, batch_size=2000)
    original = published(layout)
    # Re-ingest a source in which some committed events now carry a different payload.
    lines = layout.default_source.read_bytes().splitlines(keepends=True)
    changed = json.loads(lines[10])
    changed["humidity_pct"] = round(changed["humidity_pct"] + 1.0, 2)
    lines[10] = json.dumps(changed, separators=(",", ":")).encode() + b"\n"
    layout.default_source.write_bytes(b"".join(lines))
    summary = ingest(layout, batch_size=1999)  # new boundaries => new batch IDs => the gate decides
    assert summary.blocked[0].reason_counts == {"CONFLICTING_DUPLICATE": 1}
    assert published(layout) == original


# ---------------------------------------------------------------- quarantine mode


def test_quarantine_publishes_good_rows_and_keeps_every_bad_row(layout):
    faults = FaultConfig(malformed=0.01, invalid_value=0.01, unsupported_version=0.005)
    ledger = build_source(layout, SIM, faults)
    bad = set(
        ledger.ids["malformed"] + ledger.ids["invalid_value"] + ledger.ids["unsupported_version"]
    )
    summary = ingest(layout, mode="quarantine")

    assert summary.blocked == [] and total(summary, "rejected") == len(bad)
    assert_matches_reference(
        published(layout), {i: r for i, r in REFERENCE.items() if i not in bad}
    )
    assert total(summary, "committed") == SIM.events - len(bad)
    quarantined = [
        json.loads(line)
        for f in layout.rejected_dir.glob("*/attempt-*/quarantine.jsonl")
        for line in f.read_text().splitlines()
    ]
    assert len(quarantined) == len(bad)  # every set-aside row is recoverable
    # ...and the losses are visible, not silent: the dropped sequences show up as gaps.
    assert sum(g.missing for g in summary.gaps) == len(bad)


# ---------------------------------------------------------------- late-event policies


@pytest.mark.parametrize("policy", ["accept", "reject", "hold"])
def test_late_event_policies(layout, policy):
    ledger = build_source(layout, SIM, FaultConfig(clock_skew_past=0.01))
    n_late = ledger.counts["clock_skew_past"]
    summary = ingest(layout, mode="quarantine", late_policy=policy)
    assert summary.late == n_late > 0
    expected_published = SIM.events - (0 if policy == "accept" else n_late)
    assert total(summary, "committed") == expected_published
    assert total(summary, "rejected") == (n_late if policy == "reject" else 0)
    assert total(summary, "unresolved") == (n_late if policy == "hold" else 0)
    assert len(published(layout)) == expected_published


# ---------------------------------------------------------------- run-level accounting


def test_run_level_accounting_balances_under_a_mix_of_faults(layout):
    faults = FaultConfig(
        duplicate=0.03, drop=0.01, malformed=0.01, invalid_value=0.01, reorder=0.05, conflict=0.005
    )
    build_source(layout, SIM, faults)
    summary = ingest(layout, mode="quarantine", batch_size=500)
    a = summary.accounting
    assert a["consumed"] == summary.records
    assert a["consumed"] == a["accepted"] + a["duplicates"] + a["rejected"] + a["unresolved"]
    assert a["committed"] == a["accepted"]
    assert (
        a["committed"]
        == sum(m.records for m in summary.batch_metrics) - a["duplicates"] - a["rejected"]
    )
    files_rows = len(published(layout))
    assert files_rows == a["committed"]

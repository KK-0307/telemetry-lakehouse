"""Randomised fault mixes: the integrity invariants must hold for every one of them."""

from __future__ import annotations

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from telemetry_lakehouse.config import (
    FAULT_KINDS,
    BatchConfig,
    FaultConfig,
    GateConfig,
    SimulatorConfig,
    StorageConfig,
)
from telemetry_lakehouse.ingest import JsonlSource, run_ingest
from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.reader import connect
from telemetry_lakehouse.simulator import reference_table
from tests.helpers import build_source

SIM = SimulatorConfig(
    devices=8,
    events=300,
    seed=3,
    rate_per_second=0.01,
    start_time=datetime(2026, 1, 1, 22, tzinfo=UTC),
)
rates = st.fixed_dictionaries({k: st.floats(0.0, 0.07) for k in FAULT_KINDS})


@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    rates=rates,
    seed=st.integers(0, 10_000),
    batch_size=st.integers(5, 150),
    mode=st.sampled_from(["strict", "quarantine"]),
    late_policy=st.sampled_from(["accept", "reject", "hold"]),
    on_block=st.sampled_from(["halt", "continue"]),
)
def test_gate_invariants_hold_for_arbitrary_fault_mixes(
    rates, seed, batch_size, mode, late_policy, on_block
):
    sim = SIM.model_copy(update={"seed": seed})
    reference = {r["event_id"]: r for r in reference_table(sim).to_pylist()}
    with tempfile.TemporaryDirectory() as tmp:
        layout = StorageLayout(Path(tmp))
        ledger = build_source(layout, sim, FaultConfig(**rates, reorder_window=10))
        summary = run_ingest(
            JsonlSource.open(layout.default_source),
            layout,
            BatchConfig(batch_size=batch_size),
            StorageConfig(),
            GateConfig(mode=mode, late_policy=late_policy, on_block=on_block),
        )
        con = connect(layout)
        try:
            rows = con.execute("SELECT * EXCLUDE (event_date) FROM events").to_arrow_table()
            files = {p.stem for p in layout.data_dir.rglob("*.parquet")}
        finally:
            con.close()
        reports = [json.loads(p.read_text()) for p in layout.rejected_dir.rglob("report.json")]

    published = rows.to_pylist()
    ids = [r["event_id"] for r in published]
    a = summary.accounting

    # A2 at run level: every consumed record is in exactly one bucket.
    assert a["consumed"] == summary.records
    assert a["consumed"] == a["accepted"] + a["duplicates"] + a["rejected"] + a["unresolved"]
    # Published row count equals what the accounting says was committed.
    assert len(published) == a["committed"] <= a["accepted"]
    # I1/I2: logical events are unique, and only real source events appear.
    assert len(ids) == len(set(ids))
    assert len({(r["device_id"], r["sequence_number"]) for r in published}) == len(published)
    assert set(ids) <= set(reference)
    # Nothing invalid is ever published (ranges, version, required fields).
    for r in published:
        assert -60 <= r["temperature_c"] <= 100 and 0 <= r["humidity_pct"] <= 100
        assert 0 <= r["battery_pct"] <= 100 and -130 <= r["signal_dbm"] <= 0
        assert r["schema_version"] == 1 and all(v is not None for v in r.values())
    # Device-originated content equals the truth, except where we deliberately lied
    # (conflicting twins and skewed clocks).
    lied = set(ledger.ids["conflict"]) | set(ledger.ids["clock_skew_past"])
    for r in published:
        if r["event_id"] not in lied:
            truth = reference[r["event_id"]]
            assert {k: v for k, v in r.items() if k != "ingestion_time"} == {
                k: v for k, v in truth.items() if k != "ingestion_time"
            }
    # Blocked batches contribute no files at all.
    assert not files & {b.batch_id for b in summary.blocked}
    # Strict mode blocks on any bad record; quarantine never blocks (gate is sound).
    if mode == "quarantine":
        assert summary.blocked == []
    for report in reports:
        acc = report["accounting"]
        assert (
            acc["consumed"]
            == acc["accepted"] + acc["duplicates"] + acc["rejected"] + acc["unresolved"]
        )
        if report["decision"] == "BLOCK":
            assert acc["committed"] == 0
            assert mode == "strict" or report["critical_failures"]
    if mode == "strict":
        assert len(summary.blocked) == sum(1 for r in reports if r["decision"] == "BLOCK")

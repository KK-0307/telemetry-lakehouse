"""Shared test builders (kept out of conftest so they can be imported explicitly)."""

from __future__ import annotations

import json
from typing import Any

from telemetry_lakehouse.batcher import Batch, SourceRecord, compute_batch_id
from telemetry_lakehouse.config import FaultConfig, SimulatorConfig
from telemetry_lakehouse.faults import FaultInjector, FaultLedger
from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.simulator import SOURCE_FORMAT, generate_events, sidecar_path
from tests.conftest import valid_wire_event


def wire(n: int, **overrides: Any) -> bytes:
    """Payload for event number `n` on device d1 (event_id e<n>, sequence n)."""
    fields = {"event_id": f"e{n}", "device_id": "d1", "sequence_number": n}
    fields.update(overrides)
    return json.dumps(valid_wire_event(**fields)).encode()


def make_batch(payloads: list[bytes], start: int = 0, source_id: str = "src") -> Batch:
    records = [SourceRecord(start + i, p) for i, p in enumerate(payloads)]
    end = start + len(payloads)
    return Batch(compute_batch_id(source_id, start, end), source_id, start, end, records, "size")


def build_source(
    layout: StorageLayout, sim: SimulatorConfig, faults: FaultConfig | None = None
) -> FaultLedger:
    """Write a faulty source log + sidecar like `write_source`, returning the ground-truth ledger
    (including which event IDs each fault touched)."""
    layout.ensure()
    injector = FaultInjector(sim, faults, track_ids=True)
    payloads = list(injector.stream(generate_events(sim)))
    layout.default_source.write_bytes(b"".join(p + b"\n" for p in payloads))
    ledger = injector.ledger
    sidecar_path(layout.default_source).write_text(
        json.dumps(
            {
                "format": SOURCE_FORMAT,
                "source_id": "test-src",
                "records": len(payloads),
                "complete": ledger.complete,
                "final_sequences": ledger.final_sequences if ledger.complete else None,
            }
        )
    )
    return ledger


def committed_rows(layout: StorageLayout) -> int:
    from telemetry_lakehouse.reader import connect

    con = connect(layout)
    try:
        return con.execute("SELECT count(*) FROM events").fetchone()[0]
    finally:
        con.close()


def publish_payloads(layout: StorageLayout, manifest, payloads: list[bytes], start: int = 0):
    """Drive one batch through gate + publisher by hand; returns (batch, result, outcome)."""
    from telemetry_lakehouse.config import GateConfig, StorageConfig
    from telemetry_lakehouse.manifest import BatchState
    from telemetry_lakehouse.publisher import Publisher
    from telemetry_lakehouse.validation import evaluate_batch

    batch = make_batch(payloads, start)
    manifest.begin_batch(batch)
    manifest.transition(batch.batch_id, BatchState.RECEIVED, BatchState.VALIDATING)
    result = evaluate_batch(batch, manifest.load_index(), GateConfig())
    outcome = Publisher(layout, manifest, StorageConfig()).publish(batch, result)
    return batch, result, outcome


INSERT_INDEX = (
    "INSERT INTO event_index (event_key, event_id, device_id, sequence_number, "
    "fingerprint, ingestion_us, batch_id) VALUES "
)

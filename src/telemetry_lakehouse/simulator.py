"""Deterministic telemetry device-stream simulator.

The stream is a pure function of `SimulatorConfig`: the same config always yields the same
events, event IDs and per-device sequence numbers. That makes the generator its own
*reference dataset* (see `reference_table`) against which the published dataset is compared.

`generate_events` is the clean, in-order stream. Fault injection (duplicates, gaps, reordering,
malformed payloads, ...) is layered over it in `faults.py`.

`ingestion_time` is stamped here, *once*, as part of the durable source record
(event_time + a seeded transport delay). The pipeline never reads the wall clock to produce
data, so replaying a source offset range always yields identical rows.
"""

from __future__ import annotations

import json
import math
import os
import random
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa

from telemetry_lakehouse.config import FaultConfig, SimulatorConfig
from telemetry_lakehouse.schemas import (
    BATTERY_PCT_RANGE,
    EVENT_SCHEMA,
    HUMIDITY_PCT_RANGE,
    SCHEMA_VERSION,
    SIGNAL_DBM_RANGE,
    TEMPERATURE_C_RANGE,
    DeviceStatus,
)

EVENT_ID_NAMESPACE = uuid.UUID("6f1c2a52-7d3e-4c1b-9a56-0c1f5d2b8e47")
SOURCE_FORMAT = "jsonl-v1"
LOW_BATTERY_THRESHOLD = 15.0

RawEvent = dict[str, Any]


def device_id_for(index: int, prefix: str = "dev") -> str:
    return f"{prefix}-{index:07d}"


def event_id_for(seed: int, device_id: str, sequence_number: int) -> str:
    """Stable logical event ID: a pure function of (stream seed, device, sequence)."""
    return str(uuid.uuid5(EVENT_ID_NAMESPACE, f"{seed}/{device_id}/{sequence_number}"))


def _clamp(value: float, bounds: tuple[float, float]) -> float:
    return min(max(value, bounds[0]), bounds[1])


def generate_events(cfg: SimulatorConfig) -> Iterator[RawEvent]:
    """Yield `cfg.events` events as dicts (datetime values, not yet serialised)."""
    rng = random.Random(cfg.seed)
    n = cfg.devices
    ids = [device_id_for(i, cfg.device_prefix) for i in range(n)]
    sequences = [0] * n
    base_temp = [rng.uniform(5.0, 35.0) for _ in range(n)]
    base_signal = [rng.uniform(-90.0, -50.0) for _ in range(n)]
    battery = [rng.uniform(30.0, 100.0) for _ in range(n)]

    step_us = max(1, round(1_000_000 / cfg.rate_per_second))
    start = cfg.start_time
    for i in range(cfg.events):
        d = rng.randrange(n)
        sequences[d] += 1
        event_time = start + timedelta(microseconds=i * step_us)
        ingestion_time = event_time + timedelta(microseconds=rng.randrange(5_000, 500_000))

        hour = event_time.hour + event_time.minute / 60.0
        diurnal = math.sin(2.0 * math.pi * (hour - 6.0) / 24.0)
        battery[d] = max(0.0, battery[d] - rng.uniform(0.0, 0.01))

        roll = rng.random()
        if battery[d] < LOW_BATTERY_THRESHOLD:
            status = DeviceStatus.LOW_BATTERY
        elif roll < 0.002:
            status = DeviceStatus.MAINTENANCE
        elif roll < 0.012:
            status = DeviceStatus.DEGRADED
        else:
            status = DeviceStatus.OK

        yield {
            "event_id": event_id_for(cfg.seed, ids[d], sequences[d]),
            "device_id": ids[d],
            "sequence_number": sequences[d],
            "event_time": event_time,
            "ingestion_time": ingestion_time,
            "schema_version": SCHEMA_VERSION,
            "temperature_c": round(
                _clamp(base_temp[d] + 5.0 * diurnal + rng.gauss(0.0, 0.5), TEMPERATURE_C_RANGE), 2
            ),
            "humidity_pct": round(
                _clamp(55.0 - 15.0 * diurnal + rng.gauss(0.0, 3.0), HUMIDITY_PCT_RANGE), 2
            ),
            "battery_pct": round(_clamp(battery[d], BATTERY_PCT_RANGE), 2),
            "signal_dbm": round(_clamp(base_signal[d] + rng.gauss(0.0, 3.0), SIGNAL_DBM_RANGE)),
            "status": status.value,
        }


def paced(
    events: Iterator[RawEvent],
    cfg: SimulatorConfig,
    *,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[RawEvent]:
    """Apply the configured emission mode. `accelerated` is a pass-through."""
    if cfg.mode == "accelerated":
        yield from events
        return
    started = clock()
    for i, event in enumerate(events):
        ahead = (started + i / cfg.rate_per_second) - clock()
        if ahead > 0:
            sleep(ahead)
        yield event


def encode_event(event: RawEvent) -> bytes:
    """Serialise one event to a JSON line (timestamps as ISO-8601 with explicit offset)."""
    wire = dict(event)
    wire["event_time"] = event["event_time"].isoformat()
    wire["ingestion_time"] = event["ingestion_time"].isoformat()
    return json.dumps(wire, separators=(",", ":")).encode()


def reference_table(cfg: SimulatorConfig) -> pa.Table:
    """The known-good dataset for `cfg`, built directly with the storage schema.

    Independent of the validation/ingest path, so tests can compare it with what was
    published. Materialises the whole stream: use only for small configs.
    """
    return pa.Table.from_pylist(list(generate_events(cfg)), schema=EVENT_SCHEMA)


def sidecar_path(source_path: Path) -> Path:
    return source_path.with_name(source_path.name + ".meta.json")


def write_source(
    cfg: SimulatorConfig,
    path: Path,
    *,
    source_id: str | None = None,
    faults: FaultConfig | None = None,
) -> dict:
    """Write the delivery stream as a JSONL source log plus a metadata sidecar.

    The sidecar carries the source's *completion marker*: ``complete`` and, when complete, each
    device's final sequence number. The integrity gate may claim a sequence gap is permanent
    only against such a marker (or a watermark); an interrupted source gets no marker.

    The log is written to a temp file and renamed, so a half-written source never has the
    final name. Memory use is bounded (lines are flushed in chunks).
    """
    from telemetry_lakehouse.faults import FaultInjector  # local import: faults imports us

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    injector = FaultInjector(cfg, faults)
    count = 0
    with tmp.open("wb") as fh:
        chunk: list[bytes] = []
        for payload in injector.stream(paced(generate_events(cfg), cfg)):
            chunk.append(payload)
            count += 1
            if len(chunk) >= 10_000:
                fh.write(b"\n".join(chunk) + b"\n")
                chunk.clear()
        if chunk:
            fh.write(b"\n".join(chunk) + b"\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)

    ledger = injector.ledger
    meta = {
        "format": SOURCE_FORMAT,
        "source_id": source_id or f"sim-seed{cfg.seed}",
        "records": count,
        "complete": ledger.complete,
        "final_sequences": ledger.final_sequences if ledger.complete else None,
        "simulator": json.loads(cfg.model_dump_json()),
        "faults": {
            "config": json.loads((faults or FaultConfig()).model_dump_json()),
            "injected": dict(sorted(ledger.counts.items())),
        },
    }
    sidecar_path(path).write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
    return meta

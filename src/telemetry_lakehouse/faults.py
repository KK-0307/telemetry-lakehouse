"""Seeded fault injection over the clean simulator stream.

`FaultInjector.stream` turns clean events into the *delivery stream* a real source could
produce: duplicates, conflicting re-deliveries, drops (missing sequence numbers), out-of-order
and delayed arrival, malformed payloads, invalid values, unsupported schema versions, device
clock skew, and source interruption.

Faults use their own RNG, so enabling them never perturbs the underlying clean events. Each
original event suffers at most one fault kind (one uniform draw against cumulative rates).
The `FaultLedger` records the ground truth - what was injected and for which event IDs - so
tests can check that the integrity gate found exactly what was injected.
"""

from __future__ import annotations

import heapq
import random
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import timedelta

from telemetry_lakehouse.config import FAULT_KINDS, FaultConfig, SimulatorConfig
from telemetry_lakehouse.schemas import (
    BATTERY_PCT_RANGE,
    HUMIDITY_PCT_RANGE,
    SIGNAL_DBM_RANGE,
    TEMPERATURE_C_RANGE,
)
from telemetry_lakehouse.simulator import RawEvent, encode_event


@dataclass
class FaultLedger:
    counts: Counter[str] = field(default_factory=Counter)
    ids: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))
    # Device-side truth: last sequence number each device really produced (drops included).
    final_sequences: dict[str, int] = field(default_factory=dict)
    delivered: int = 0
    # False if the source was interrupted: no end-of-stream marker may be claimed.
    complete: bool = True

    def note(self, kind: str, event_id: str, track_ids: bool) -> None:
        self.counts[kind] += 1
        if track_ids:
            self.ids[kind].append(event_id)


def _with(event: RawEvent, **changes: object) -> RawEvent:
    out = dict(event)
    out.update(changes)
    return out


class FaultInjector:
    def __init__(
        self,
        sim: SimulatorConfig,
        faults: FaultConfig | None = None,
        *,
        track_ids: bool = False,
    ) -> None:
        self.sim = sim
        self.faults = faults or FaultConfig()
        self.track_ids = track_ids
        self.ledger = FaultLedger()
        self._step_us = max(1, round(1_000_000 / sim.rate_per_second))

    def stream(self, events: Iterable[RawEvent]) -> Iterator[bytes]:
        """Yield delivered payloads in arrival order, updating `self.ledger` as it goes."""
        if not self.faults.active:
            yield from self._clean(events)
            return
        yield from self._faulty(events)

    def _clean(self, events: Iterable[RawEvent]) -> Iterator[bytes]:
        finals = self.ledger.final_sequences
        for e in events:
            finals[e["device_id"]] = e["sequence_number"]
            self.ledger.delivered += 1
            yield encode_event(e)

    def _faulty(self, events: Iterable[RawEvent]) -> Iterator[bytes]:
        f = self.faults
        ledger = self.ledger
        rng = random.Random(f"{self.sim.seed}/faults")
        rates = [(k, getattr(f, k)) for k in FAULT_KINDS if getattr(f, k) > 0]
        heap: list[tuple[int, int, bytes]] = []
        tiebreak = 0

        def push(release_at: int, payload: bytes) -> None:
            nonlocal tiebreak
            heapq.heappush(heap, (release_at, tiebreak, payload))
            tiebreak += 1

        def later(event: RawEvent, positions: int) -> RawEvent:
            delta = timedelta(microseconds=positions * self._step_us)
            return _with(event, ingestion_time=event["ingestion_time"] + delta)

        def drain(upto: int) -> Iterator[bytes]:
            while heap and heap[0][0] <= upto:
                if f.interrupt_after is not None and ledger.delivered >= f.interrupt_after:
                    return
                ledger.delivered += 1
                yield heapq.heappop(heap)[2]

        def interrupted() -> bool:
            return f.interrupt_after is not None and ledger.delivered >= f.interrupt_after

        for i, event in enumerate(events):
            ledger.final_sequences[event["device_id"]] = event["sequence_number"]
            eid = event["event_id"]
            draw = rng.random()
            kind = None
            acc = 0.0
            for name, rate in rates:
                acc += rate
                if draw < acc:
                    kind = name
                    break
            if kind is not None:
                ledger.note(kind, eid, self.track_ids)

            if kind == "drop":
                pass
            elif kind == "malformed":
                good = encode_event(event)
                variant = rng.randrange(3)
                bad = (good[: len(good) // 2], b"\x00\xff\xfenot-json", b"[1,2,3]")[variant]
                push(i, bad)
            elif kind == "invalid_value":
                field_name, bad_value = rng.choice(
                    [
                        ("temperature_c", TEMPERATURE_C_RANGE[1] + 150.0),
                        ("humidity_pct", HUMIDITY_PCT_RANGE[0] - 5.0),
                        ("battery_pct", BATTERY_PCT_RANGE[1] + 40.0),
                        ("signal_dbm", SIGNAL_DBM_RANGE[1] + 20),
                    ]
                )
                push(i, encode_event(_with(event, **{field_name: bad_value})))
            elif kind == "unsupported_version":
                push(i, encode_event(_with(event, schema_version=99)))
            elif kind == "clock_skew_future":
                hours = timedelta(hours=rng.randint(3, 72))
                push(i, encode_event(_with(event, event_time=event["event_time"] + hours)))
            elif kind == "clock_skew_past":
                hours = timedelta(hours=rng.randint(3, 72))
                push(i, encode_event(_with(event, event_time=event["event_time"] - hours)))
            elif kind == "duplicate":
                push(i, encode_event(event))
                gap = rng.randint(0, f.reorder_window)
                push(i + gap, encode_event(later(event, gap)))
            elif kind == "conflict":
                push(i, encode_event(event))
                gap = rng.randint(0, f.reorder_window)
                t = event["temperature_c"]
                changed = round(t + 1.0 if t + 1.0 <= TEMPERATURE_C_RANGE[1] else t - 1.0, 2)
                twin = _with(later(event, gap), temperature_c=changed)
                push(i + gap, encode_event(twin))
            elif kind == "reorder":
                gap = rng.randint(1, f.reorder_window)
                push(i + gap, encode_event(later(event, gap)))
            elif kind == "delay":
                gap = f.delay_positions
                push(i + gap, encode_event(later(event, gap)))
            else:
                push(i, encode_event(event))

            yield from drain(i)
            if interrupted():
                break

        if interrupted():
            ledger.complete = False
            return
        yield from drain(1 << 62)
        if f.interrupt_after is not None:
            # Interruption configured beyond the stream length: stream still ended normally.
            ledger.complete = True

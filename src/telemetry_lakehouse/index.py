"""In-memory view of what is already committed: identities and per-device sequence state.

The source of truth is the SQLite ``event_index`` (written in the same transaction as the manifest
commit, whose PRIMARY KEY makes logical-event uniqueness race-free). This cache is warmed from it at
startup (`Manifest.load_index`) and updated only *after* a commit succeeds. Start-up is O(committed
events); a lookup-on-demand variant is a Milestone 6 option if profiling justifies it.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from telemetry_lakehouse.schemas import TelemetryEvent

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_US = timedelta(microseconds=1)


def to_micros(dt: datetime) -> int:
    return (dt - _EPOCH) // _US


def payload_fingerprint(
    device_id: str,
    sequence_number: int,
    event_time_us: int,
    schema_version: int,
    temperature_c: float,
    humidity_pct: float,
    battery_pct: float,
    signal_dbm: int,
    status: str,
) -> bytes:
    """Identity of what the *device* said. `ingestion_time` is deliberately excluded: it is the
    platform's receipt stamp, so a legitimate re-delivery of the same event differs only there."""
    text = (
        f"{device_id}|{sequence_number}|{event_time_us}|{schema_version}|"
        f"{temperature_c!r}|{humidity_pct!r}|{battery_pct!r}|{signal_dbm}|{status}"
    )
    return hashlib.blake2b(text.encode(), digest_size=12).digest()


def event_fingerprint(e: TelemetryEvent) -> bytes:
    return payload_fingerprint(
        e.device_id,
        e.sequence_number,
        to_micros(e.event_time),
        e.schema_version,
        e.temperature_c,
        e.humidity_pct,
        e.battery_pct,
        e.signal_dbm,
        e.status.value,
    )


@dataclass(frozen=True, slots=True)
class Gap:
    """A run of missing sequence numbers ``[first, last]`` for one device."""

    device_id: str
    first: int
    last: int
    basis: str  # "end_of_stream" (exact) or "watermark" (grace period elapsed)

    @property
    def missing(self) -> int:
        return self.last - self.first + 1


@dataclass(slots=True)
class _Device:
    contiguous: int = 0  # every sequence 1..contiguous is present (or proven lost)
    max_seen: int = 0
    pending: dict[int, int] = field(default_factory=dict)  # seq above a gap -> ingestion_us


class SequenceTracker:
    """Per-device sequence state that tolerates out-of-order arrival.

    A gap is merely *open* while later sequence numbers are present but earlier ones are not;
    it is only reported as lost by `finalize`, against a watermark or an end-of-stream marker.
    """

    def __init__(self) -> None:
        self._devices: dict[str, _Device] = {}
        self.watermark_us = 0  # max ingestion_time among admitted events

    def max_seen(self, device_id: str) -> int:
        d = self._devices.get(device_id)
        return d.max_seen if d else 0

    def observe(self, device_id: str, seq: int, ingestion_us: int) -> None:
        d = self._devices.get(device_id)
        if d is None:
            d = self._devices[device_id] = _Device()
        self.watermark_us = max(self.watermark_us, ingestion_us)
        d.max_seen = max(d.max_seen, seq)
        if seq == d.contiguous + 1:
            d.contiguous = seq
            while d.contiguous + 1 in d.pending:
                d.contiguous += 1
                del d.pending[d.contiguous]
        elif seq > d.contiguous:
            d.pending[seq] = ingestion_us

    def open_gap_count(self) -> int:
        """Number of missing sequence numbers below each device's highest seen sequence."""
        return sum(d.max_seen - d.contiguous - len(d.pending) for d in self._devices.values())

    def finalize(
        self,
        grace_us: int,
        final_sequences: dict[str, int] | None = None,
    ) -> list[Gap]:
        """Return gaps proven permanent.

        * With ``final_sequences`` (an end-of-stream marker listing each device's last sequence),
          every missing number up to that final sequence is proven lost, including trailing loss.
        * Otherwise a gap is proven lost only if the event just above it arrived at least
          ``grace_us`` before the stream watermark - i.e. a late arrival had its chance.
        """
        gaps: list[Gap] = []
        end_of_stream = final_sequences is not None
        for device_id, d in sorted(self._devices.items()):
            lower = d.contiguous
            for upper in sorted(d.pending):
                if upper - lower > 1 and (
                    end_of_stream or self.watermark_us - d.pending[upper] >= grace_us
                ):
                    gaps.append(
                        Gap(
                            device_id,
                            lower + 1,
                            upper - 1,
                            "end_of_stream" if end_of_stream else "watermark",
                        )
                    )
                lower = upper
        if final_sequences is not None:
            for device_id, final in sorted(final_sequences.items()):
                top = self._devices[device_id].max_seen if device_id in self._devices else 0
                if final > top:
                    gaps.append(Gap(device_id, top + 1, final, "end_of_stream"))
        return gaps


@dataclass(frozen=True, slots=True)
class IndexedEvent:
    device_id: str
    sequence_number: int
    fingerprint: bytes


class CommittedIndex:
    """Identities of every published event, plus sequence state."""

    def __init__(self) -> None:
        self._by_id: dict[str, IndexedEvent] = {}
        self._seq_owner: dict[tuple[str, int], str] = {}
        self.sequences = SequenceTracker()

    def __len__(self) -> int:
        return len(self._by_id)

    def lookup(self, event_id: str) -> IndexedEvent | None:
        return self._by_id.get(event_id)

    def seq_owner(self, device_id: str, seq: int) -> str | None:
        return self._seq_owner.get((device_id, seq))

    def add(
        self,
        event_id: str,
        device_id: str,
        seq: int,
        fingerprint: bytes,
        ingestion_us: int,
    ) -> None:
        if event_id in self._by_id:
            raise ValueError(f"event_id {event_id!r} already indexed")
        self._by_id[event_id] = IndexedEvent(device_id, seq, fingerprint)
        self._seq_owner[(device_id, seq)] = event_id
        self.sequences.observe(device_id, seq, ingestion_us)

    def add_events(
        self,
        events: Iterable[TelemetryEvent],
        fingerprints: Iterable[bytes] | None = None,
        ingestion_us: Iterable[int] | None = None,
    ) -> None:
        """Index events; `fingerprints` / `ingestion_us` may be passed to avoid recomputing them."""
        events = list(events)
        fps = (
            list(fingerprints)
            if fingerprints is not None
            else [event_fingerprint(e) for e in events]
        )
        ing = (
            list(ingestion_us)
            if ingestion_us is not None
            else [to_micros(e.ingestion_time) for e in events]
        )
        for e, fp, i in zip(events, fps, ing, strict=True):
            self.add(e.event_id, e.device_id, e.sequence_number, fp, i)

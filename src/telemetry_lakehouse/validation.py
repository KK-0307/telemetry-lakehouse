"""The integrity gate: decides whether a batch may become visible to trusted readers.

For one `Batch` the gate classifies **every source record into exactly one bucket**:

    accepted   valid, new, consistent - eligible for publication
    duplicate  identical re-delivery of an event already seen (in this batch or committed)
    rejected   contract violation, conflicting identity, clock skew, or late under `reject`
    unresolved valid but held back (late under `hold`) - neither published nor discarded

and checks the accounting invariants (see docs/correctness.md):

    A1  consumed == end_offset - start_offset == len(batch)
    A2  consumed == accepted + duplicate + rejected + unresolved

The gate is pure with respect to storage: it reads the `CommittedIndex` but never mutates it,
and writes nothing. Publication and evidence persistence are the caller's job, which keeps
the decision logic unit-testable.

Whole-batch rejection vs row-level quarantine
---------------------------------------------
``strict`` (default): any rejected or unresolved record blocks the *entire* batch - the batch is
the unit of trust. ``quarantine``: bad records are set aside with evidence and the rest of the
batch publishes - the row is the unit of trust, but the published data is then knowingly
incomplete. An accounting violation (A1/A2) blocks in both modes: it means the gate itself
cannot be trusted for this batch.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Literal

from pydantic import ValidationError

from telemetry_lakehouse.batcher import Batch
from telemetry_lakehouse.config import GateConfig
from telemetry_lakehouse.index import CommittedIndex, payload_fingerprint, to_micros
from telemetry_lakehouse.schemas import TelemetryEvent


class ReasonCode(StrEnum):
    MALFORMED_PAYLOAD = "MALFORMED_PAYLOAD"
    MISSING_FIELD = "MISSING_FIELD"
    UNKNOWN_FIELD = "UNKNOWN_FIELD"
    INVALID_TYPE = "INVALID_TYPE"
    INVALID_VALUE = "INVALID_VALUE"
    OUT_OF_RANGE = "OUT_OF_RANGE"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    UNSUPPORTED_SCHEMA_VERSION = "UNSUPPORTED_SCHEMA_VERSION"
    FUTURE_EVENT_TIME = "FUTURE_EVENT_TIME"
    LATE_EVENT = "LATE_EVENT"
    CONFLICTING_DUPLICATE = "CONFLICTING_DUPLICATE"
    SEQUENCE_CONFLICT = "SEQUENCE_CONFLICT"


_RANGE_ERRORS = {
    "greater_than_equal",
    "less_than_equal",
    "greater_than",
    "less_than",
    "finite_number",
}
_TIMESTAMP_ERRORS = {
    "timezone_aware",
    "datetime_type",
    "datetime_parsing",
    "datetime_from_date_parsing",
}
_TYPE_ERRORS = {
    "int_type", "int_parsing", "int_from_float", "float_type", "float_parsing", "string_type",
    "bool_type", "enum",
}  # fmt: skip


def classify_validation_error(exc: ValidationError) -> tuple[ReasonCode, str]:
    errors = exc.errors()
    first = errors[0]
    kind, loc = first["type"], tuple(first["loc"])
    detail = "; ".join(
        f"{'.'.join(map(str, e['loc'])) or '<root>'}: {e['msg']}" for e in errors[:5]
    )
    if kind in {"json_invalid", "model_type", "dict_type"}:
        code = ReasonCode.MALFORMED_PAYLOAD
    elif loc == ("schema_version",) and kind == "value_error":
        code = ReasonCode.UNSUPPORTED_SCHEMA_VERSION
    elif kind == "missing":
        code = ReasonCode.MISSING_FIELD
    elif kind == "extra_forbidden":
        code = ReasonCode.UNKNOWN_FIELD
    elif kind in _RANGE_ERRORS:
        code = ReasonCode.OUT_OF_RANGE
    elif kind in _TIMESTAMP_ERRORS:
        code = ReasonCode.INVALID_TIMESTAMP
    elif kind in _TYPE_ERRORS:
        code = ReasonCode.INVALID_TYPE
    else:
        code = ReasonCode.INVALID_VALUE
    return code, detail


@dataclass(frozen=True, slots=True)
class Rejection:
    offset: int
    code: ReasonCode
    detail: str
    payload: bytes = field(repr=False)
    # For identity conflicts: offsets / committed event this record conflicts with.
    related: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class Accounting:
    consumed: int
    accepted: int
    duplicates: int
    rejected: int
    unresolved: int
    committed: int = 0  # filled in after publication

    def violations(self, start_offset: int, end_offset: int, batch_len: int) -> list[str]:
        out = []
        if not self.consumed == end_offset - start_offset == batch_len:
            out.append(
                f"A1: consumed={self.consumed}, offsets span {end_offset - start_offset}, "
                f"batch has {batch_len}"
            )
        total = self.accepted + self.duplicates + self.rejected + self.unresolved
        if self.consumed != total:
            out.append(
                f"A2: consumed={self.consumed} != accepted+duplicates+rejected+unresolved={total}"
            )
        return out

    def with_committed(self, committed: int) -> Accounting:
        return Accounting(**{**asdict(self), "committed": committed})


Decision = Literal["ADMIT", "BLOCK"]


@dataclass(slots=True)
class GateResult:
    batch_id: str
    decision: Decision
    mode: str
    accounting: Accounting
    accepted: list[TelemetryEvent]
    accepted_fingerprints: list[bytes]
    accepted_ingestion_us: list[int]
    rejections: list[Rejection]
    unresolved: list[tuple[int, TelemetryEvent, bytes]]  # (offset, event, raw payload)
    critical_failures: list[str]
    duplicates_in_batch: int = 0
    duplicates_committed: int = 0
    out_of_order: int = 0
    late: int = 0

    @property
    def reason_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for r in self.rejections:
            counts[r.code.value] = counts.get(r.code.value, 0) + 1
        if self.unresolved:
            counts[ReasonCode.LATE_EVENT.value + ":HELD"] = len(self.unresolved)
        return dict(sorted(counts.items()))


def payload_digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


@dataclass(slots=True)
class _Candidate:
    offset: int
    event: TelemetryEvent
    payload: bytes
    fingerprint: bytes
    event_us: int
    ingest_us: int


def evaluate_batch(batch: Batch, index: CommittedIndex, config: GateConfig) -> GateResult:
    rejections: list[Rejection] = []
    unresolved: list[tuple[int, TelemetryEvent, bytes]] = []
    duplicates_in_batch = duplicates_committed = 0

    candidates: dict[str, _Candidate] = {}  # event_id -> surviving candidate (arrival order)
    seq_owner: dict[tuple[str, int], str] = {}
    conflicted_ids: dict[str, int] = {}  # event_id -> offset of first conflicting record
    max_future_us = int(config.max_future_skew_seconds * 1_000_000)
    max_late_us = int(config.max_lateness_seconds * 1_000_000)

    for record in batch.records:
        # 1. contract
        try:
            event = TelemetryEvent.model_validate_json(record.payload)
        except ValidationError as exc:
            code, detail = classify_validation_error(exc)
            rejections.append(Rejection(record.offset, code, detail, record.payload))
            continue
        event_us, ingest_us = to_micros(event.event_time), to_micros(event.ingestion_time)
        if event_us - ingest_us > max_future_us:
            rejections.append(
                Rejection(
                    record.offset,
                    ReasonCode.FUTURE_EVENT_TIME,
                    f"event_time is {(event_us - ingest_us) / 1e6:.0f}s ahead of ingestion_time",
                    record.payload,
                )
            )
            continue
        fp = payload_fingerprint(
            event.device_id, event.sequence_number, event_us, event.schema_version,
            event.temperature_c, event.humidity_pct, event.battery_pct, event.signal_dbm,
            event.status.value,
        )  # fmt: skip
        eid = event.event_id

        # 2. identity against committed data
        committed = index.lookup(eid)
        if committed is not None:
            if committed.fingerprint == fp:
                duplicates_committed += 1
            else:
                rejections.append(
                    Rejection(
                        record.offset,
                        ReasonCode.CONFLICTING_DUPLICATE,
                        f"event_id {eid} is committed with a different payload",
                        record.payload,
                    )
                )
            continue
        owner = index.seq_owner(event.device_id, event.sequence_number)
        if owner is not None:  # owner != eid here, else step 2 would have matched
            rejections.append(
                Rejection(
                    record.offset,
                    ReasonCode.SEQUENCE_CONFLICT,
                    f"({event.device_id}, {event.sequence_number}) is committed as event {owner}",
                    record.payload,
                )
            )
            continue

        # 3. identity within the batch
        if eid in conflicted_ids:
            rejections.append(
                Rejection(
                    record.offset,
                    ReasonCode.CONFLICTING_DUPLICATE,
                    f"event_id {eid} already conflicted at offset {conflicted_ids[eid]}",
                    record.payload,
                    (conflicted_ids[eid],),
                )
            )
            continue
        prior = candidates.get(eid)
        if prior is not None:
            if prior.fingerprint == fp:
                duplicates_in_batch += 1
                continue
            # Same event_id, different payload: we cannot know which is true, so neither is.
            del candidates[eid]
            del seq_owner[(prior.event.device_id, prior.event.sequence_number)]
            conflicted_ids[eid] = prior.offset
            detail = f"event_id {eid} delivered with different payloads"
            rejections.append(
                Rejection(
                    prior.offset,
                    ReasonCode.CONFLICTING_DUPLICATE,
                    detail,
                    prior.payload,
                    (record.offset,),
                )
            )
            rejections.append(
                Rejection(
                    record.offset,
                    ReasonCode.CONFLICTING_DUPLICATE,
                    detail,
                    record.payload,
                    (prior.offset,),
                )
            )
            continue
        key = (event.device_id, event.sequence_number)
        other_id = seq_owner.get(key)
        if other_id is not None:
            other = candidates.pop(other_id)
            del seq_owner[key]
            detail = f"({key[0]}, {key[1]}) claimed by events {other_id} and {eid}"
            rejections.append(
                Rejection(
                    other.offset,
                    ReasonCode.SEQUENCE_CONFLICT,
                    detail,
                    other.payload,
                    (record.offset,),
                )
            )
            rejections.append(
                Rejection(
                    record.offset,
                    ReasonCode.SEQUENCE_CONFLICT,
                    detail,
                    record.payload,
                    (other.offset,),
                )
            )
            continue
        candidates[eid] = _Candidate(record.offset, event, record.payload, fp, event_us, ingest_us)
        seq_owner[key] = eid

    # 4. lateness policy and sequence statistics over the surviving candidates
    accepted: list[TelemetryEvent] = []
    accepted_fps: list[bytes] = []
    accepted_ing: list[int] = []
    late = out_of_order = 0
    local_max: dict[str, int] = {}
    for c in sorted(candidates.values(), key=lambda c: c.offset):
        e = c.event
        lateness_us = c.ingest_us - c.event_us
        if lateness_us > max_late_us:
            late += 1
            if config.late_policy == "reject":
                rejections.append(
                    Rejection(
                        c.offset,
                        ReasonCode.LATE_EVENT,
                        f"arrived {lateness_us / 1e6:.0f}s after event_time "
                        f"(max {config.max_lateness_seconds:.0f}s)",
                        c.payload,
                    )
                )
                continue
            if config.late_policy == "hold":
                unresolved.append((c.offset, e, c.payload))
                continue
        high = max(local_max.get(e.device_id, 0), index.sequences.max_seen(e.device_id))
        if e.sequence_number < high:
            out_of_order += 1
        local_max[e.device_id] = max(high, e.sequence_number)
        accepted.append(e)
        accepted_fps.append(c.fingerprint)
        accepted_ing.append(c.ingest_us)

    rejections.sort(key=lambda r: r.offset)
    accounting = Accounting(
        consumed=len(batch),
        accepted=len(accepted),
        duplicates=duplicates_in_batch + duplicates_committed,
        rejected=len(rejections),
        unresolved=len(unresolved),
    )
    critical = accounting.violations(batch.start_offset, batch.end_offset, len(batch))
    blocked = bool(critical) or (
        config.mode == "strict" and (accounting.rejected > 0 or accounting.unresolved > 0)
    )
    return GateResult(
        batch_id=batch.batch_id,
        decision="BLOCK" if blocked else "ADMIT",
        mode=config.mode,
        accounting=accounting,
        accepted=[] if blocked else accepted,
        accepted_fingerprints=[] if blocked else accepted_fps,
        accepted_ingestion_us=[] if blocked else accepted_ing,
        rejections=rejections,
        unresolved=unresolved,
        critical_failures=critical,
        duplicates_in_batch=duplicates_in_batch,
        duplicates_committed=duplicates_committed,
        out_of_order=out_of_order,
        late=late,
    )

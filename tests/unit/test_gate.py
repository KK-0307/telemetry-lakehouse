from __future__ import annotations

import json

import pytest

from telemetry_lakehouse.config import GateConfig
from telemetry_lakehouse.index import CommittedIndex, event_fingerprint, to_micros
from telemetry_lakehouse.schemas import TelemetryEvent
from telemetry_lakehouse.validation import Accounting, ReasonCode, evaluate_batch
from tests.conftest import valid_wire_event
from tests.helpers import make_batch, wire

STRICT = GateConfig()
QUARANTINE = GateConfig(mode="quarantine")


def evaluate(payloads, config=STRICT, index=None, start=0):
    return evaluate_batch(make_batch(payloads, start), index or CommittedIndex(), config)


def committed(index: CommittedIndex, payload: bytes) -> TelemetryEvent:
    e = TelemetryEvent.model_validate_json(payload)
    index.add_events([e])
    return e


def test_clean_batch_is_admitted_and_fully_accounted():
    r = evaluate([wire(i) for i in range(1, 6)])
    assert r.decision == "ADMIT"
    assert r.accounting == Accounting(
        consumed=5, accepted=5, duplicates=0, rejected=0, unresolved=0
    )
    assert [e.event_id for e in r.accepted] == ["e1", "e2", "e3", "e4", "e5"]


def broken(**overrides):
    return json.dumps(valid_wire_event(**overrides)).encode()


def without(field):
    wire_event = valid_wire_event()
    del wire_event[field]
    return json.dumps(wire_event).encode()


@pytest.mark.parametrize(
    ("payload", "code"),
    [
        (b"{not json", ReasonCode.MALFORMED_PAYLOAD),
        (b"\x00\xff\xfe", ReasonCode.MALFORMED_PAYLOAD),
        (b"", ReasonCode.MALFORMED_PAYLOAD),
        (b"[1,2,3]", ReasonCode.MALFORMED_PAYLOAD),
        (without("temperature_c"), ReasonCode.MISSING_FIELD),
        (broken(firmware="1.0"), ReasonCode.UNKNOWN_FIELD),
        (broken(temperature_c=500.0), ReasonCode.OUT_OF_RANGE),
        (broken(signal_dbm=5), ReasonCode.OUT_OF_RANGE),
        (broken(schema_version=2), ReasonCode.UNSUPPORTED_SCHEMA_VERSION),
        (broken(event_time="2026-01-01T12:00:00"), ReasonCode.INVALID_TIMESTAMP),
        (broken(temperature_c="21.5"), ReasonCode.INVALID_TYPE),
        (broken(status="EXPLODING"), ReasonCode.INVALID_TYPE),
        (broken(event_time="2026-01-01T13:00:00+00:00"), ReasonCode.FUTURE_EVENT_TIME),
    ],
)
def test_every_kind_of_bad_record_is_rejected_with_its_reason(payload, code):
    r = evaluate([wire(1), payload, wire(3)])
    assert r.decision == "BLOCK"  # strict: one bad record blocks the whole batch
    assert [(x.offset, x.code) for x in r.rejections] == [(1, code)]
    assert r.accepted == []  # nothing from a blocked batch is publishable
    assert r.accounting.accepted == 2 and r.accounting.rejected == 1


def test_quarantine_mode_admits_good_rows_and_sets_bad_ones_aside():
    r = evaluate([wire(1), b"garbage", wire(3)], QUARANTINE)
    assert r.decision == "ADMIT"
    assert [e.event_id for e in r.accepted] == ["e1", "e3"]
    assert [x.offset for x in r.rejections] == [1]


def test_identical_redelivery_in_batch_is_a_benign_duplicate():
    dup = wire(1, ingestion_time="2026-01-01T12:05:00+00:00")  # only the receipt stamp differs
    r = evaluate([wire(1), wire(2), dup])
    assert r.decision == "ADMIT"
    assert r.accounting.duplicates == 1 and r.duplicates_in_batch == 1
    assert [e.event_id for e in r.accepted] == ["e1", "e2"]


def test_same_event_id_with_different_payload_rejects_both_copies():
    r = evaluate([wire(1), wire(2), wire(1, temperature_c=30.0), wire(1, temperature_c=31.0)])
    assert r.decision == "BLOCK"
    assert sorted(x.offset for x in r.rejections) == [0, 2, 3]
    assert {x.code for x in r.rejections} == {ReasonCode.CONFLICTING_DUPLICATE}
    assert r.accounting.rejected == 3 and r.accounting.accepted == 1


def test_sequence_claimed_by_two_events_rejects_both():
    r = evaluate([wire(1), wire(2, event_id="other", sequence_number=1)], QUARANTINE)
    assert {x.code for x in r.rejections} == {ReasonCode.SEQUENCE_CONFLICT}
    assert r.accepted == []
    assert r.accounting.rejected == 2


def test_duplicate_of_committed_event_is_benign():
    index = CommittedIndex()
    committed(index, wire(1))
    r = evaluate([wire(1, ingestion_time="2026-01-01T13:00:00+00:00"), wire(2)], index=index)
    assert r.decision == "ADMIT"
    assert (r.duplicates_committed, r.accounting.duplicates) == (1, 1)
    assert [e.event_id for e in r.accepted] == ["e2"]


def test_conflict_with_committed_event_is_rejected():
    index = CommittedIndex()
    committed(index, wire(1))
    r = evaluate([wire(1, temperature_c=99.0), wire(2)], index=index)
    assert r.decision == "BLOCK"
    assert r.rejections[0].code == ReasonCode.CONFLICTING_DUPLICATE
    assert "committed" in r.rejections[0].detail


def test_sequence_taken_by_committed_event_is_rejected():
    index = CommittedIndex()
    committed(index, wire(1))
    r = evaluate([wire(7, sequence_number=1)], index=index)
    assert r.rejections[0].code == ReasonCode.SEQUENCE_CONFLICT


def test_out_of_order_arrival_is_accepted_and_counted_not_blocked():
    r = evaluate([wire(1), wire(3), wire(2), wire(5), wire(4)])
    assert r.decision == "ADMIT"
    assert r.out_of_order == 2  # seq 2 after 3; seq 4 after 5
    assert r.accounting.accepted == 5


def test_out_of_order_is_judged_against_committed_state_too():
    index = CommittedIndex()
    committed(index, wire(5))
    r = evaluate([wire(3)], index=index)
    assert r.out_of_order == 1 and r.decision == "ADMIT"


LATE = {"event_time": "2026-01-01T08:00:00+00:00"}  # 4h before ingestion_time (12:00:01)


def test_late_events_are_counted_and_accepted_by_default():
    r = evaluate([wire(1), wire(2, **LATE)])
    assert (r.late, r.decision, r.accounting.accepted) == (1, "ADMIT", 2)


def test_late_policy_reject():
    r = evaluate([wire(1), wire(2, **LATE)], GateConfig(late_policy="reject", mode="quarantine"))
    assert [x.code for x in r.rejections] == [ReasonCode.LATE_EVENT]
    assert r.accounting.accepted == 1


def test_late_policy_hold_leaves_records_unresolved_and_strict_blocks_on_them():
    cfg = GateConfig(late_policy="hold")
    r = evaluate([wire(1), wire(2, **LATE)], cfg)
    assert r.decision == "BLOCK"
    assert (r.accounting.accepted, r.accounting.unresolved, r.accounting.rejected) == (1, 1, 0)
    q = evaluate([wire(1), wire(2, **LATE)], GateConfig(late_policy="hold", mode="quarantine"))
    assert q.decision == "ADMIT" and [e.event_id for e in q.accepted] == ["e1"]


def test_mixed_batch_accounting_identity():
    index = CommittedIndex()
    committed(index, wire(1))
    payloads = [
        wire(1),  # duplicate (committed)
        wire(2),  # accepted
        wire(2),  # duplicate (batch)
        b"junk",  # rejected
        wire(3, temperature_c=300.0),  # rejected
        wire(4, **LATE),  # unresolved under hold
        wire(5),  # accepted
        wire(6),
        wire(6, humidity_pct=1.0),  # conflict -> 2 rejected (first copy + this)
    ]
    r = evaluate(payloads, GateConfig(mode="quarantine", late_policy="hold"), index)
    a = r.accounting
    assert a.consumed == len(payloads) == 9
    assert (a.accepted, a.duplicates, a.rejected, a.unresolved) == (2, 2, 4, 1)
    assert a.consumed == a.accepted + a.duplicates + a.rejected + a.unresolved
    assert a.violations(0, 9, 9) == []


def test_accounting_violations_are_detected():
    bad = Accounting(consumed=10, accepted=5, duplicates=1, rejected=1, unresolved=1)
    assert any(v.startswith("A2") for v in bad.violations(0, 10, 10))
    ok = Accounting(consumed=8, accepted=5, duplicates=1, rejected=1, unresolved=1)
    assert ok.violations(0, 8, 8) == []
    assert any(v.startswith("A1") for v in ok.violations(0, 9, 8))  # offsets disagree with count


def test_a_broken_gate_blocks_even_in_quarantine_mode(monkeypatch):
    import telemetry_lakehouse.validation as v

    monkeypatch.setattr(v.Accounting, "violations", lambda self, *a: ["A2: simulated"])
    r = evaluate([wire(1)], QUARANTINE)
    assert r.decision == "BLOCK" and r.critical_failures == ["A2: simulated"]
    assert r.accepted == []


def test_gate_does_not_mutate_the_index():
    index = CommittedIndex()
    evaluate([wire(1), wire(2)], index=index)
    assert len(index) == 0 and index.sequences.max_seen("d1") == 0


def test_fingerprint_ignores_ingestion_time_but_not_device_fields():
    a = TelemetryEvent.model_validate_json(wire(1))
    b = TelemetryEvent.model_validate_json(wire(1, ingestion_time="2026-01-02T00:00:00+00:00"))
    c = TelemetryEvent.model_validate_json(wire(1, battery_pct=1.0))
    assert event_fingerprint(a) == event_fingerprint(b) != event_fingerprint(c)
    assert to_micros(a.event_time) == 1767268800 * 1_000_000

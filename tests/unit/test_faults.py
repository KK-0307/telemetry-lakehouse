from __future__ import annotations

import json
from collections import Counter

import pytest
from pydantic import ValidationError

from telemetry_lakehouse.config import FaultConfig, SimulatorConfig
from telemetry_lakehouse.faults import FaultInjector
from telemetry_lakehouse.simulator import encode_event, generate_events

SIM = SimulatorConfig(devices=30, events=3000, seed=5, rate_per_second=1.0)


def deliver(faults: FaultConfig | None, sim: SimulatorConfig = SIM, track_ids: bool = True):
    inj = FaultInjector(sim, faults, track_ids=track_ids)
    return list(inj.stream(generate_events(sim))), inj.ledger


def test_no_faults_is_byte_identical_to_the_clean_stream():
    payloads, ledger = deliver(None)
    assert payloads == [encode_event(e) for e in generate_events(SIM)]
    assert ledger.complete and ledger.counts == Counter() and ledger.delivered == SIM.events


def test_fault_streams_are_deterministic():
    f = FaultConfig(duplicate=0.05, drop=0.02, reorder=0.1, malformed=0.02)
    assert deliver(f)[0] == deliver(f)[0]
    other = SIM.model_copy(update={"seed": 6})
    assert deliver(f)[0] != deliver(f, other)[0]


def test_untouched_events_are_exactly_the_clean_events():
    f = FaultConfig(malformed=0.1, invalid_value=0.05)
    payloads, ledger = deliver(f)
    touched = set(ledger.ids["malformed"]) | set(ledger.ids["invalid_value"])
    clean = {e["event_id"]: encode_event(e) for e in generate_events(SIM)}
    delivered_ok = {}
    for p in payloads:
        try:
            delivered_ok[json.loads(p)["event_id"]] = p
        except (ValueError, TypeError, KeyError):
            continue  # malformed payloads are not parseable events
    for event_id, payload in clean.items():
        if event_id not in touched:
            assert delivered_ok[event_id] == payload


def test_rates_are_roughly_honoured_and_counted():
    f = FaultConfig(duplicate=0.05, drop=0.03)
    payloads, ledger = deliver(f)
    assert 90 < ledger.counts["duplicate"] < 210  # expected 150
    assert 50 < ledger.counts["drop"] < 140  # expected 90
    assert len(payloads) == SIM.events - ledger.counts["drop"] + ledger.counts["duplicate"]


def test_drop_removes_the_event_but_device_truth_keeps_its_sequence():
    payloads, ledger = deliver(FaultConfig(drop=0.1))
    ids = {json.loads(p)["event_id"] for p in payloads}
    assert not ids & set(ledger.ids["drop"])
    clean_finals = {}
    for e in generate_events(SIM):
        clean_finals[e["device_id"]] = e["sequence_number"]
    assert ledger.final_sequences == clean_finals


def test_duplicates_are_identical_except_for_ingestion_time():
    payloads, ledger = deliver(FaultConfig(duplicate=0.1))
    by_id: dict[str, list[dict]] = {}
    for p in payloads:
        d = json.loads(p)
        by_id.setdefault(d["event_id"], []).append(d)
    dup_ids = set(ledger.ids["duplicate"])
    assert dup_ids
    for event_id in dup_ids:
        first, second = by_id[event_id]
        assert {k: v for k, v in first.items() if k != "ingestion_time"} == {
            k: v for k, v in second.items() if k != "ingestion_time"
        }
        assert second["ingestion_time"] >= first["ingestion_time"]
    assert all(len(v) == 1 for k, v in by_id.items() if k not in dup_ids)


def test_conflicts_reuse_the_event_id_with_a_different_payload():
    payloads, ledger = deliver(FaultConfig(conflict=0.05))
    seen: dict[str, set[float]] = {}
    for p in payloads:
        d = json.loads(p)
        seen.setdefault(d["event_id"], set()).add(d["temperature_c"])
    assert ledger.ids["conflict"]
    assert all(len(seen[i]) == 2 for i in ledger.ids["conflict"])


def test_reorder_and_delay_change_arrival_order_but_not_content():
    f = FaultConfig(reorder=0.2, delay=0.02, reorder_window=20, delay_positions=500)
    payloads, ledger = deliver(f)
    clean = [encode_event(e) for e in generate_events(SIM)]
    assert len(payloads) == len(clean)
    assert payloads != clean
    seqs: dict[str, list[int]] = {}
    for p in payloads:
        d = json.loads(p)
        seqs.setdefault(d["device_id"], []).append(d["sequence_number"])
    assert any(s != sorted(s) for s in seqs.values())  # genuinely out of order
    assert {json.loads(p)["event_id"] for p in payloads} == {
        e["event_id"] for e in generate_events(SIM)
    }
    assert ledger.counts["reorder"] > 0 and ledger.counts["delay"] > 0


@pytest.mark.parametrize(
    "kind", ["malformed", "invalid_value", "unsupported_version", "clock_skew_future"]
)
def test_bad_payload_faults_are_counted_one_to_one(kind):
    payloads, ledger = deliver(FaultConfig(**{kind: 0.05}))
    assert len(payloads) == SIM.events
    assert ledger.counts[kind] == len(ledger.ids[kind]) > 0


def test_interruption_truncates_the_stream_and_withholds_the_completion_marker():
    payloads, ledger = deliver(FaultConfig(interrupt_after=1000))
    assert len(payloads) == 1000 and ledger.complete is False
    # interruption configured past the end of the stream is not an interruption
    payloads, ledger = deliver(FaultConfig(interrupt_after=10_000))
    assert len(payloads) == SIM.events and ledger.complete is True


def test_fault_rates_must_sum_to_at_most_one():
    with pytest.raises(ValidationError):
        FaultConfig(drop=0.6, duplicate=0.6)
    with pytest.raises(ValidationError):
        FaultConfig(drop=-0.1)
    assert FaultConfig().active is False and FaultConfig(drop=0.1).active is True

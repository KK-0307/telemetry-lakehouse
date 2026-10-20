from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from telemetry_lakehouse.config import SimulatorConfig
from telemetry_lakehouse.schemas import TelemetryEvent
from telemetry_lakehouse.simulator import (
    encode_event,
    generate_events,
    paced,
    reference_table,
    write_source,
)

CFG = SimulatorConfig(devices=25, events=1500, seed=7, rate_per_second=0.5)


def test_same_seed_gives_identical_stream():
    assert list(generate_events(CFG)) == list(generate_events(CFG))


def test_different_seed_gives_different_stream():
    other = CFG.model_copy(update={"seed": 8})
    assert list(generate_events(CFG)) != list(generate_events(other))


def test_event_ids_are_globally_unique_and_stable():
    events = list(generate_events(CFG))
    ids = [e["event_id"] for e in events]
    assert len(set(ids)) == len(ids) == CFG.events
    assert ids == [e["event_id"] for e in generate_events(CFG)]


def test_per_device_sequences_are_gapless_and_in_order():
    seen: dict[str, list[int]] = defaultdict(list)
    for e in generate_events(CFG):
        seen[e["device_id"]].append(e["sequence_number"])
    for seqs in seen.values():
        assert seqs == list(range(1, len(seqs) + 1))


def test_every_generated_event_satisfies_the_contract():
    for e in generate_events(CFG):
        TelemetryEvent.model_validate_json(encode_event(e))


def test_event_time_follows_configured_rate_and_start():
    events = list(generate_events(CFG))
    assert events[0]["event_time"] == CFG.start_time
    assert (events[1]["event_time"] - events[0]["event_time"]).total_seconds() == 2.0


def test_ingestion_time_is_after_event_time():
    assert all(e["ingestion_time"] > e["event_time"] for e in generate_events(CFG))


def test_zero_events_is_an_empty_stream():
    assert list(generate_events(CFG.model_copy(update={"events": 0}))) == []


def test_config_validation_rejects_bad_values():
    with pytest.raises(ValidationError):
        SimulatorConfig(devices=0)
    with pytest.raises(ValidationError):
        SimulatorConfig(rate_per_second=-1)
    with pytest.raises(ValidationError):
        SimulatorConfig(start_time=datetime(2026, 1, 1))  # naive
    with pytest.raises(ValidationError):
        SimulatorConfig(bogus=1)


def test_write_source_is_byte_reproducible(tmp_path):
    a, b = tmp_path / "a.jsonl", tmp_path / "b.jsonl"
    meta = write_source(CFG, a)
    write_source(CFG, b)
    assert a.read_bytes() == b.read_bytes()
    assert meta["records"] == CFG.events
    assert len(a.read_bytes().splitlines()) == CFG.events
    assert json.loads(a.read_bytes().splitlines()[0])["sequence_number"] == 1
    assert not list(tmp_path.glob(".*.tmp"))


def test_reference_table_matches_stream():
    table = reference_table(CFG)
    assert table.num_rows == CFG.events
    assert table["event_time"][0].as_py() == datetime(2026, 1, 1, tzinfo=UTC)


def test_realtime_mode_paces_to_the_configured_rate():
    cfg = CFG.model_copy(update={"mode": "realtime", "rate_per_second": 10.0, "events": 5})
    now = [0.0]
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    out = list(paced(generate_events(cfg), cfg, clock=lambda: now[0], sleep=sleep))
    assert len(out) == 5
    assert now[0] == pytest.approx(0.4)  # 5 events at 10/s: last is released at t=0.4s


def test_accelerated_mode_never_sleeps():
    def boom(_: float) -> None:
        raise AssertionError("slept")

    assert len(list(paced(generate_events(CFG), CFG, sleep=boom))) == CFG.events


def test_device_prefix_namespaces_device_ids_without_changing_the_stream_shape():
    a = list(generate_events(CFG))
    b = list(generate_events(CFG.model_copy(update={"device_prefix": "edge"})))
    assert all(e["device_id"].startswith("dev-") for e in a)
    assert all(e["device_id"].startswith("edge-") for e in b)
    # same seed => same measurements and sequences; only the identifiers differ
    assert [e["temperature_c"] for e in a] == [e["temperature_c"] for e in b]
    assert [e["sequence_number"] for e in a] == [e["sequence_number"] for e in b]
    assert {e["event_id"] for e in a}.isdisjoint({e["event_id"] for e in b})
    with pytest.raises(ValidationError):
        SimulatorConfig(device_prefix="bad prefix!")

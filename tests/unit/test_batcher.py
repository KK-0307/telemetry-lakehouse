from __future__ import annotations

import pytest

from telemetry_lakehouse.batcher import (
    MicroBatcher,
    OffsetContinuityError,
    SourceRecord,
    compute_batch_id,
)


def records(start: int, stop: int) -> list[SourceRecord]:
    return [SourceRecord(i, f"r{i}".encode()) for i in range(start, stop)]


def test_size_based_flush_and_final_partial_batch():
    batches = list(MicroBatcher("s", 4).batches(records(0, 10)))
    assert [(b.start_offset, b.end_offset, b.reason) for b in batches] == [
        (0, 4, "size"),
        (4, 8, "size"),
        (8, 10, "end"),
    ]
    assert [len(b) for b in batches] == [4, 4, 2]


def test_batch_id_is_deterministic_and_interval_scoped():
    first = [b.batch_id for b in MicroBatcher("s", 4).batches(records(0, 10))]
    again = [b.batch_id for b in MicroBatcher("s", 4).batches(records(0, 10))]
    assert first == again
    assert len(set(first)) == 3
    assert compute_batch_id("s", 0, 4) == first[0]
    assert compute_batch_id("other-source", 0, 4) != first[0]


def test_same_interval_gets_same_id_regardless_of_batcher_instance():
    resumed = list(MicroBatcher("s", 4).batches(records(4, 8)))
    assert resumed[0].batch_id == compute_batch_id("s", 4, 8)


def test_empty_source_yields_no_batches():
    assert list(MicroBatcher("s", 4).batches([])) == []


def test_offset_gap_is_an_error():
    stream = [*records(0, 3), *records(5, 7)]
    with pytest.raises(OffsetContinuityError):
        list(MicroBatcher("s", 100).batches(stream))


def test_time_based_flush_with_idle_tick():
    now = [0.0]
    batcher = MicroBatcher("s", 100, max_age_seconds=5.0, clock=lambda: now[0])

    def stream():
        yield from records(0, 3)
        now[0] = 6.0  # the quiet period exceeds max age
        yield None  # idle tick lets the batcher flush without a new record
        yield from records(3, 5)

    out = list(batcher.batches(stream()))
    assert [(b.start_offset, b.end_offset, b.reason) for b in out] == [
        (0, 3, "time"),
        (3, 5, "end"),
    ]


def test_time_based_flush_not_triggered_before_max_age():
    now = [0.0]
    batcher = MicroBatcher("s", 100, max_age_seconds=5.0, clock=lambda: now[0])
    out = list(batcher.batches(iter([*records(0, 3), None])))
    assert [(b.reason, len(b)) for b in out] == [("end", 3)]


def test_buffer_never_exceeds_max_records():
    buffered_high_water = 0
    consumed = 0

    def stream():
        nonlocal consumed
        for r in records(0, 1000):
            consumed += 1
            yield r

    for batch in MicroBatcher("s", 16).batches(stream()):
        buffered_high_water = max(buffered_high_water, len(batch))
        assert consumed - batch.start_offset <= 16  # the batcher never reads ahead of a batch
    assert buffered_high_water == 16


def test_invalid_parameters():
    with pytest.raises(ValueError):
        MicroBatcher("s", 0)
    with pytest.raises(ValueError):
        MicroBatcher("s", 1, max_age_seconds=0)

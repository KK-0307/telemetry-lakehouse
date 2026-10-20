from __future__ import annotations

import pytest

from telemetry_lakehouse.index import CommittedIndex, Gap, SequenceTracker

H = 3_600_000_000  # one hour in microseconds


def tracker(*arrivals: tuple[str, int, int]) -> SequenceTracker:
    t = SequenceTracker()
    for device, seq, ingestion_us in arrivals:
        t.observe(device, seq, ingestion_us)
    return t


def test_in_order_stream_has_no_gaps():
    t = tracker(*[("d", s, s) for s in range(1, 6)])
    assert t.finalize(0) == [] and t.open_gap_count() == 0


def test_out_of_order_arrival_closes_a_temporary_gap():
    t = tracker(("d", 1, 1), ("d", 3, 2))
    assert t.open_gap_count() == 1  # open, but NOT lost
    t.observe("d", 2, 3)
    assert t.open_gap_count() == 0
    assert t.finalize(0, {"d": 3}) == []


def test_watermark_proves_a_gap_only_after_the_grace_period():
    t = tracker(("d", 1, 0), ("d", 3, 1 * H))
    assert t.finalize(2 * H) == []  # later event is only 0h old at the watermark
    t.observe("other", 1, 5 * H)  # watermark advances 4h past seq 3's arrival
    assert t.finalize(2 * H) == [Gap("d", 2, 2, "watermark")]


def test_zero_grace_flags_every_open_gap():
    t = tracker(("d", 1, 0), ("d", 4, 1), ("d", 6, 2))
    assert t.finalize(0) == [Gap("d", 2, 3, "watermark"), Gap("d", 5, 5, "watermark")]


def test_end_of_stream_marker_proves_all_gaps_including_trailing_loss():
    t = tracker(("d", 1, 0), ("d", 3, 1), ("e", 1, 2))
    gaps = t.finalize(10 * H, {"d": 5, "e": 1, "never_seen": 2})
    assert gaps == [
        Gap("d", 2, 2, "end_of_stream"),
        Gap("d", 4, 5, "end_of_stream"),
        Gap("never_seen", 1, 2, "end_of_stream"),
    ]


def test_complete_stream_with_matching_marker_has_no_gaps():
    t = tracker(("d", 2, 0), ("d", 1, 1), ("d", 3, 2))
    assert t.finalize(0, {"d": 3}) == []


def test_missing_counts():
    assert Gap("d", 4, 9, "x").missing == 6


def test_committed_index_rejects_double_add_and_tracks_owners():
    idx = CommittedIndex()
    idx.add("e1", "d", 1, b"fp", 5)
    assert idx.lookup("e1").fingerprint == b"fp"
    assert idx.seq_owner("d", 1) == "e1" and idx.seq_owner("d", 2) is None
    with pytest.raises(ValueError):
        idx.add("e1", "d", 2, b"fp", 6)
    assert len(idx) == 1

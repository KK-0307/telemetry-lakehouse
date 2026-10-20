from __future__ import annotations

import sqlite3

import pytest

from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.manifest import (
    TRANSITIONS,
    BatchState,
    CommitConflictError,
    IllegalTransition,
    InFlightBatchError,
    Manifest,
    SourceMutatedError,
    event_key,
)
from tests.helpers import INSERT_INDEX, make_batch, publish_payloads, wire

S = BatchState


@pytest.fixture
def manifest(layout: StorageLayout):
    with Manifest.open(layout) as m:
        yield m


def payloads(lo: int, hi: int) -> list[bytes]:
    return [wire(i) for i in range(lo, hi)]


def test_database_is_wal_durable_and_strict(manifest: Manifest):
    con = sqlite3.connect(manifest.path)
    assert con.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"batches", "batch_files", "event_index", "meta"} <= tables
    assert manifest._con.execute("PRAGMA synchronous").fetchone()[0] == 2  # FULL


def test_new_batch_is_received_with_its_boundaries(manifest: Manifest):
    batch = make_batch(payloads(1, 4))
    begin = manifest.begin_batch(batch)
    assert begin.outcome == "new"
    row = manifest.get_batch(batch.batch_id)
    assert (row.state, row.start_offset, row.end_offset, row.attempt) == (S.RECEIVED, 0, 3, 1)


def test_in_flight_batch_blocks_a_second_begin(manifest: Manifest):
    batch = make_batch(payloads(1, 4))
    manifest.begin_batch(batch)
    with pytest.raises(InFlightBatchError):
        manifest.begin_batch(batch)


def test_committed_batch_is_an_idempotent_noop_but_detects_a_mutated_source(
    manifest: Manifest, layout: StorageLayout
):
    batch, _, _ = publish_payloads(layout, manifest, payloads(1, 4))
    again = manifest.begin_batch(make_batch(payloads(1, 4)))
    assert again.outcome == "already_committed" and again.row.state is S.COMMITTED
    tampered = payloads(1, 4)
    tampered[1] = wire(2, temperature_c=40.0)
    with pytest.raises(SourceMutatedError):
        manifest.begin_batch(make_batch(tampered))
    assert manifest.get_batch(batch.batch_id).state is S.COMMITTED


def test_rejected_batch_can_be_resubmitted_as_a_new_attempt(manifest: Manifest):
    batch = make_batch(payloads(1, 4))
    manifest.begin_batch(batch)
    manifest.transition(batch.batch_id, S.RECEIVED, S.VALIDATING)
    manifest.reject(batch.batch_id, S.VALIDATING, None, {"X": 1}, [], "ev")
    assert manifest.get_batch(batch.batch_id).state is S.REJECTED
    assert manifest.begin_batch(batch).outcome == "retry"
    row = manifest.get_batch(batch.batch_id)
    assert (row.state, row.attempt, row.evidence_path) == (S.RECEIVED, 2, None)


ALL_PAIRS = [(a, b) for a in S for b in S if b not in TRANSITIONS[a]]


@pytest.mark.parametrize(("src", "dst"), ALL_PAIRS)
def test_illegal_transitions_are_refused(manifest: Manifest, src, dst):
    batch = make_batch(payloads(1, 2))
    manifest.begin_batch(batch)
    if src is S.COMMITTED:  # must satisfy the schema's own CHECKs to exist at all
        manifest._con.execute(
            "UPDATE batches SET state='COMMITTED', accepted=0, committed_rows=0, commit_seq=1"
        )
    else:
        manifest._con.execute("UPDATE batches SET state=?", (src.value,))
    with pytest.raises(IllegalTransition):
        manifest.transition(batch.batch_id, src, dst)


def test_compare_and_set_refuses_a_stale_source_state(manifest: Manifest):
    batch = make_batch(payloads(1, 2))
    manifest.begin_batch(batch)  # RECEIVED
    with pytest.raises(IllegalTransition):
        manifest.transition(batch.batch_id, S.STAGED, S.VERIFIED)
    assert manifest.get_batch(batch.batch_id).state is S.RECEIVED


def test_committed_is_terminal():
    assert TRANSITIONS[S.COMMITTED] == frozenset()


def test_schema_enforces_a3_a_committed_batch_publishes_what_was_accepted(manifest: Manifest):
    batch = make_batch(payloads(1, 4))
    manifest.begin_batch(batch)
    with pytest.raises(sqlite3.IntegrityError):  # COMMITTED without a commit_seq / committed_rows
        manifest._con.execute("UPDATE batches SET state='COMMITTED'")
    with pytest.raises(sqlite3.IntegrityError):  # committed_rows != accepted
        manifest._con.execute(
            "UPDATE batches SET state='COMMITTED', accepted=3, committed_rows=2, commit_seq=1"
        )
    with pytest.raises(sqlite3.IntegrityError):  # a non-committed batch cannot claim committed rows
        manifest._con.execute("UPDATE batches SET committed_rows=3")


def test_event_index_enforces_unique_identities_in_the_database(
    manifest: Manifest, layout: StorageLayout
):
    batch, _, _ = publish_payloads(layout, manifest, payloads(1, 4))
    con = manifest._con
    row = ("other", "d1", 99, b"fp", 1, batch.batch_id)
    with pytest.raises(sqlite3.IntegrityError):  # event_id (PRIMARY KEY)
        con.execute(
            INSERT_INDEX + "(?, 'e1', 'd1', 99, x'00', 1, ?)",
            (event_key("e1"), batch.batch_id),
        )
    with pytest.raises(sqlite3.IntegrityError):  # (device_id, sequence_number)
        con.execute(
            INSERT_INDEX + "(?, 'new-id', 'd1', 1, x'00', 1, ?)",
            (event_key("new-id"), batch.batch_id),
        )
    assert row  # silence unused
    assert con.execute("SELECT count(*) FROM event_index").fetchone()[0] == 3


def test_commit_is_all_or_nothing(manifest: Manifest, layout: StorageLayout):
    from telemetry_lakehouse.config import GateConfig, StorageConfig
    from telemetry_lakehouse.manifest import logical_digest
    from telemetry_lakehouse.parquet_writer import write_batch
    from telemetry_lakehouse.publisher import place_staged, verify_staged
    from telemetry_lakehouse.schemas import table_from_events
    from telemetry_lakehouse.validation import evaluate_batch

    batch = make_batch(payloads(1, 6))
    manifest.begin_batch(batch)
    manifest.transition(batch.batch_id, S.RECEIVED, S.VALIDATING)
    result = evaluate_batch(batch, manifest.load_index(), GateConfig())
    staged = write_batch(
        table_from_events(result.accepted), batch.batch_id, layout, StorageConfig()
    )
    manifest.record_staged(
        batch.batch_id,
        result.accounting,
        staged,
        logical_digest(result.accepted_fingerprints),
        {},
        None,
    )
    verify_staged(staged, result.accepted)
    manifest.transition(batch.batch_id, S.STAGED, S.VERIFIED)
    place_staged(staged, layout)
    entries = [
        (e.event_id, e.device_id, e.sequence_number, fp, 1)
        for e, fp in zip(result.accepted, result.accepted_fingerprints, strict=True)
    ]
    entries[-1] = entries[0]  # a duplicate identity in the same transaction
    with pytest.raises(CommitConflictError):
        manifest.commit(batch.batch_id, entries)
    # Nothing leaked: the state flip, the commit_seq and the earlier inserts were all rolled back.
    assert manifest.get_batch(batch.batch_id).state is S.VERIFIED
    assert manifest._con.execute("SELECT count(*) FROM event_index").fetchone()[0] == 0
    assert manifest.committed_files() == []


def test_only_committed_batches_have_visible_files(manifest: Manifest, layout: StorageLayout):
    publish_payloads(layout, manifest, payloads(1, 4))
    # A second batch staged but never committed:
    from telemetry_lakehouse import failpoints

    with failpoints.armed("after_place"), pytest.raises(failpoints.SimulatedCrash):
        publish_payloads(layout, manifest, payloads(4, 7), start=3)
    assert len(manifest.batches()) == 2
    assert {f.batch_id for f in manifest.committed_files()} == {make_batch(payloads(1, 4)).batch_id}
    assert len(manifest.staged_files(make_batch(payloads(4, 7), 3).batch_id)) == 1


def test_committed_frontier_follows_the_unbroken_committed_chain(
    manifest: Manifest, layout: StorageLayout
):
    assert manifest.committed_frontier("src") == 0
    publish_payloads(layout, manifest, payloads(1, 4), start=0)
    publish_payloads(layout, manifest, payloads(4, 7), start=3)
    assert manifest.committed_frontier("src") == 6
    publish_payloads(layout, manifest, payloads(10, 13), start=9)  # hole at [6, 9)
    assert manifest.committed_frontier("src") == 6


def test_load_index_rebuilds_identities_and_sequence_state(
    manifest: Manifest, layout: StorageLayout
):
    publish_payloads(layout, manifest, [wire(1), wire(2), wire(4)])
    index = manifest.load_index()
    assert len(index) == 3 and index.lookup("e2") is not None
    assert index.sequences.max_seen("d1") == 4 and index.sequences.open_gap_count() == 1

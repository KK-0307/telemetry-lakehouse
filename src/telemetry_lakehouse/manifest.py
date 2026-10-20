"""Durable batch metadata and the visibility boundary: a SQLite manifest.

The manifest answers one question authoritatively: *which Parquet files are committed?* A file is
trusted if and only if a batch row in state ``COMMITTED`` references it. Moving a batch to
COMMITTED is a single SQLite transaction that also writes the batch's identities into
``event_index`` - so either everything about the batch becomes visible, or nothing does.

Batch life cycle (every transition is a compare-and-set on the current state)::

    RECEIVED -> VALIDATING -> STAGED -> VERIFIED -> COMMITTED
                    |            |          |
                    +------------+----------+--> REJECTED   (gate block / failed verification)
    any in-flight state ------------------------> INTERRUPTED  (set by recovery, Milestone 4)
    REJECTED | INTERRUPTED ------------------------> RECEIVED   (new attempt)

Durability: WAL journal with ``synchronous=FULL``; the publisher is the single writer
(``BEGIN IMMEDIATE``). Data invariants live in the schema (CHECK/UNIQUE/PRIMARY KEY), not only in
Python: e.g. a COMMITTED batch must have ``committed_rows == accepted`` (invariant A3) and
``event_index`` rejects a second row for the same ``event_id`` or ``(device_id, sequence_number)`` (I1, I2).
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path

from telemetry_lakehouse import failpoints
from telemetry_lakehouse.batcher import Batch
from telemetry_lakehouse.index import CommittedIndex
from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.parquet_writer import StagedFile
from telemetry_lakehouse.validation import Accounting

MANIFEST_SCHEMA_VERSION = 2  # v2: compact event_key index (no migration from v1; pre-1.0)

SQLITE_CACHE_MB = int(os.environ.get("TELEMETRY_SQLITE_CACHE_MB", "128"))
SQLITE_WAL_AUTOCHECKPOINT_PAGES = int(os.environ.get("TELEMETRY_SQLITE_WAL_PAGES", "20000"))


class BatchState(StrEnum):
    RECEIVED = "RECEIVED"
    VALIDATING = "VALIDATING"
    STAGED = "STAGED"
    VERIFIED = "VERIFIED"
    COMMITTED = "COMMITTED"
    REJECTED = "REJECTED"
    INTERRUPTED = "INTERRUPTED"


S = BatchState
TRANSITIONS: dict[BatchState, frozenset[BatchState]] = {
    S.RECEIVED: frozenset({S.VALIDATING, S.INTERRUPTED}),
    S.VALIDATING: frozenset({S.STAGED, S.REJECTED, S.INTERRUPTED}),
    S.STAGED: frozenset({S.VERIFIED, S.REJECTED, S.INTERRUPTED}),
    S.VERIFIED: frozenset({S.COMMITTED, S.REJECTED, S.INTERRUPTED}),
    S.COMMITTED: frozenset(),  # terminal
    S.REJECTED: frozenset({S.RECEIVED}),  # resubmission starts a new attempt
    S.INTERRUPTED: frozenset({S.RECEIVED}),  # recovery starts a new attempt
}
IN_FLIGHT = frozenset({S.RECEIVED, S.VALIDATING, S.STAGED, S.VERIFIED, S.INTERRUPTED})


class ManifestError(Exception):
    pass


class IllegalTransition(ManifestError):
    pass


class InFlightBatchError(ManifestError):
    """A previous attempt at this batch never finished; `telemetry recover` must resolve it."""

    def __init__(self, batch_id: str, state: BatchState) -> None:
        super().__init__(
            f"batch {batch_id} is {state} from an earlier, unfinished attempt; run `telemetry recover`"
        )
        self.batch_id, self.state = batch_id, state


class SourceMutatedError(ManifestError):
    """Same source interval (same batch ID) now holds different bytes than when it was committed."""


class CommitConflictError(ManifestError):
    """The commit transaction violated an identity constraint; nothing was committed."""


_DDL = f"""
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;

CREATE TABLE IF NOT EXISTS batches (
    batch_id        TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL,
    start_offset    INTEGER NOT NULL,
    end_offset      INTEGER NOT NULL,
    state           TEXT NOT NULL CHECK (state IN ({", ".join(f"'{s.value}'" for s in BatchState)})),
    attempt         INTEGER NOT NULL CHECK (attempt >= 1),
    flush_reason    TEXT NOT NULL,
    input_digest    TEXT NOT NULL,
    consumed        INTEGER, accepted INTEGER, duplicates INTEGER, rejected INTEGER, unresolved INTEGER,
    committed_rows  INTEGER,
    reason_counts   TEXT,
    critical_failures TEXT,
    evidence_path   TEXT,
    output_digest   TEXT,
    logical_digest  TEXT,
    commit_seq      INTEGER UNIQUE,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE (source_id, start_offset, end_offset),
    CHECK (end_offset > start_offset),
    -- A3: a committed batch publishes exactly what the gate accepted; others publish nothing.
    CHECK (state <> 'COMMITTED' OR (commit_seq IS NOT NULL AND committed_rows = accepted)),
    CHECK (state = 'COMMITTED' OR (commit_seq IS NULL AND committed_rows IS NULL))
) STRICT;

CREATE TABLE IF NOT EXISTS batch_files (
    batch_id          TEXT NOT NULL REFERENCES batches(batch_id),
    relative_path     TEXT NOT NULL UNIQUE,
    event_date        TEXT NOT NULL,
    rows              INTEGER NOT NULL CHECK (rows > 0),
    size_bytes        INTEGER NOT NULL,
    sha256            TEXT NOT NULL,
    min_event_time_us INTEGER NOT NULL,
    max_event_time_us INTEGER NOT NULL,
    PRIMARY KEY (batch_id, relative_path)
) STRICT;

CREATE TABLE IF NOT EXISTS attempt_log (
    id          INTEGER PRIMARY KEY,
    batch_id    TEXT NOT NULL REFERENCES batches(batch_id),
    attempt     INTEGER NOT NULL,
    outcome     TEXT NOT NULL,
    detail      TEXT NOT NULL,
    recorded_at TEXT NOT NULL
) STRICT;

-- One row per published event. Uniqueness of the logical event (I1) is enforced on `event_key`, a
-- 16-byte hash of event_id: a compact unique index stays small enough to live in the page cache
-- (a UNIQUE TEXT key made commits 3-4x slower at 1M events; see docs/benchmarks.md). A hash
-- collision between two different IDs would surface as a CommitConflictError, never as a silent merge.
CREATE TABLE IF NOT EXISTS event_index (
    id              INTEGER PRIMARY KEY,
    event_key       BLOB NOT NULL UNIQUE,
    event_id        TEXT NOT NULL,
    device_id       TEXT NOT NULL,
    sequence_number INTEGER NOT NULL,
    fingerprint     BLOB NOT NULL,
    ingestion_us    INTEGER NOT NULL,
    batch_id        TEXT NOT NULL REFERENCES batches(batch_id),
    UNIQUE (device_id, sequence_number)
) STRICT;

-- Rows of one batch share a key and have increasing rowids, so maintaining this index is an append.
CREATE INDEX IF NOT EXISTS event_index_batch ON event_index (batch_id);
"""


def event_key(event_id: str) -> bytes:
    return hashlib.blake2b(event_id.encode(), digest_size=16).digest()


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def input_digest(batch: Batch) -> str:
    """Digest of the exact source bytes of a batch. Offsets are contiguous (the batcher enforces it),
    so the interval plus the newline-joined payloads identify the content."""
    h = hashlib.blake2b(digest_size=16)
    h.update(f"{batch.start_offset}:{batch.end_offset}:".encode())
    h.update(b"\n".join(r.payload for r in batch.records))
    return h.hexdigest()


def logical_digest(fingerprints: Sequence[bytes]) -> str:
    """Order-independent digest of the logical content (device-originated fields) of a batch."""
    h = hashlib.sha256()
    for fp in sorted(fingerprints):
        h.update(fp)
    return h.hexdigest()


@dataclass(frozen=True, slots=True)
class BatchRow:
    batch_id: str
    source_id: str
    start_offset: int
    end_offset: int
    state: BatchState
    attempt: int
    input_digest: str
    accepted: int | None
    committed_rows: int | None
    commit_seq: int | None
    evidence_path: str | None
    output_digest: str | None
    logical_digest: str | None


@dataclass(frozen=True, slots=True)
class CommittedFile:
    batch_id: str
    relative_path: str
    rows: int
    size_bytes: int
    sha256: str
    commit_seq: int


@dataclass(frozen=True, slots=True)
class Begin:
    outcome: str  # "new" | "retry" | "already_committed"
    row: BatchRow


_ROW_COLS = (
    "batch_id, source_id, start_offset, end_offset, state, attempt, input_digest, accepted, "
    "committed_rows, commit_seq, evidence_path, output_digest, logical_digest"
)


def _row(r: tuple) -> BatchRow:
    return BatchRow(*r[:4], BatchState(r[4]), *r[5:])


class Manifest:
    def __init__(self, path: Path, *, readonly: bool = False) -> None:
        self.path = path
        self._con = sqlite3.connect(path, isolation_level=None, timeout=10.0)
        self._con.execute("PRAGMA foreign_keys = ON")
        if readonly:
            self._con.execute("PRAGMA query_only = ON")
        else:
            self._con.execute("PRAGMA journal_mode = WAL")
            self._con.execute("PRAGMA synchronous = FULL")
            self._con.execute(f"PRAGMA cache_size = {-SQLITE_CACHE_MB * 1024}")
            self._con.execute(f"PRAGMA wal_autocheckpoint = {SQLITE_WAL_AUTOCHECKPOINT_PAGES}")
            self._init_schema()

    # ---- construction -------------------------------------------------------------------
    @classmethod
    def open(cls, layout: StorageLayout) -> Manifest:
        layout.meta_dir.mkdir(parents=True, exist_ok=True)
        return cls(layout.meta_dir / "manifest.sqlite")

    @classmethod
    def open_readonly(cls, layout: StorageLayout) -> Manifest | None:
        path = layout.meta_dir / "manifest.sqlite"
        return cls(path, readonly=True) if path.exists() else None

    def _init_schema(self) -> None:
        with self._txn():
            for statement in _DDL.split(";\n\n"):
                if statement.strip():
                    self._con.execute(statement)
            row = self._con.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
            if row is None:
                self._con.execute(
                    "INSERT INTO meta VALUES ('schema_version', ?)", (str(MANIFEST_SCHEMA_VERSION),)
                )
            elif int(row[0]) != MANIFEST_SCHEMA_VERSION:
                raise ManifestError(
                    f"manifest schema {row[0]} != supported {MANIFEST_SCHEMA_VERSION}"
                )

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> Manifest:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @contextmanager
    def _txn(self) -> Iterator[sqlite3.Connection]:
        """One write transaction. Rolls back on *any* exit by exception, including simulated crashes."""
        self._con.execute("BEGIN IMMEDIATE")
        try:
            yield self._con
        except BaseException:
            self._con.execute("ROLLBACK")
            raise
        else:
            self._con.execute("COMMIT")

    # ---- reads --------------------------------------------------------------------------
    def get_batch(self, batch_id: str) -> BatchRow | None:
        r = self._con.execute(
            f"SELECT {_ROW_COLS} FROM batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        return _row(r) if r else None

    def batches(self, state: BatchState | None = None) -> list[BatchRow]:
        sql = f"SELECT {_ROW_COLS} FROM batches"
        args: tuple = ()
        if state is not None:
            sql, args = sql + " WHERE state=?", (state.value,)
        return [_row(r) for r in self._con.execute(sql + " ORDER BY source_id, start_offset", args)]

    def in_flight(self) -> list[BatchRow]:
        return [b for b in self.batches() if b.state in IN_FLIGHT]

    def state_counts(self) -> dict[str, int]:
        return dict(
            self._con.execute("SELECT state, count(*) FROM batches GROUP BY state ORDER BY state")
        )

    def committed_files(self) -> list[CommittedFile]:
        """The visible dataset: files of COMMITTED batches only, from one consistent snapshot."""
        rows = self._con.execute(
            "SELECT f.batch_id, f.relative_path, f.rows, f.size_bytes, f.sha256, b.commit_seq "
            "FROM batch_files f JOIN batches b USING (batch_id) "
            "WHERE b.state = 'COMMITTED' ORDER BY b.commit_seq, f.relative_path"
        ).fetchall()
        return [CommittedFile(*r) for r in rows]

    def staged_files(self, batch_id: str) -> list[CommittedFile]:
        rows = self._con.execute(
            "SELECT batch_id, relative_path, rows, size_bytes, sha256, 0 FROM batch_files "
            "WHERE batch_id=? ORDER BY relative_path",
            (batch_id,),
        ).fetchall()
        return [CommittedFile(*r) for r in rows]

    def committed_frontier(self, source_id: str) -> int:
        """First source offset not covered by an unbroken chain of COMMITTED batches from 0."""
        frontier = 0
        for start, end in self._con.execute(
            "SELECT start_offset, end_offset FROM batches WHERE source_id=? AND state='COMMITTED' "
            "ORDER BY start_offset, end_offset",
            (source_id,),
        ):
            if start > frontier:
                break
            frontier = max(frontier, end)
        return frontier

    def load_index(self) -> CommittedIndex:
        index = CommittedIndex()
        for eid, dev, seq, fp, ing in self._con.execute(
            "SELECT event_id, device_id, sequence_number, fingerprint, ingestion_us FROM event_index"
        ):
            index.add(eid, dev, seq, bytes(fp), ing)
        return index

    # ---- writes -------------------------------------------------------------------------
    def begin_batch(self, batch: Batch) -> Begin:
        """Register a batch before any work on it (durably fixes its boundaries)."""
        digest = input_digest(batch)
        with self._txn() as con:
            existing = self.get_batch(batch.batch_id)
            now = _now()
            if existing is None:
                con.execute(
                    "INSERT INTO batches (batch_id, source_id, start_offset, end_offset, state, "
                    "attempt, flush_reason, input_digest, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, 'RECEIVED', 1, ?, ?, ?, ?)",
                    (batch.batch_id, batch.source_id, batch.start_offset, batch.end_offset,
                     batch.reason, digest, now, now),
                )  # fmt: skip
                return Begin("new", self.get_batch(batch.batch_id))  # type: ignore[arg-type]
            if existing.state is BatchState.COMMITTED:
                if existing.input_digest != digest:
                    raise SourceMutatedError(
                        f"batch {batch.batch_id} [{batch.start_offset}, {batch.end_offset}) of "
                        f"{batch.source_id} is committed, but the source bytes at those offsets changed"
                    )
                return Begin("already_committed", existing)
            if existing.state in (BatchState.REJECTED, BatchState.INTERRUPTED):  # new attempt
                con.execute(
                    "UPDATE batches SET state='RECEIVED', attempt=attempt+1, input_digest=?, "
                    "flush_reason=?, consumed=NULL, accepted=NULL, duplicates=NULL, rejected=NULL, "
                    "unresolved=NULL, reason_counts=NULL, critical_failures=NULL, evidence_path=NULL, "
                    "output_digest=NULL, logical_digest=NULL, updated_at=? WHERE batch_id=?",
                    (digest, batch.reason, now, batch.batch_id),
                )
                return Begin("retry", self.get_batch(batch.batch_id))  # type: ignore[arg-type]
            raise InFlightBatchError(batch.batch_id, existing.state)

    def _cas(
        self,
        con: sqlite3.Connection,
        batch_id: str,
        src: BatchState,
        dst: BatchState,
        sets: str = "",
        args: tuple = (),
    ) -> None:
        if dst not in TRANSITIONS[src]:
            raise IllegalTransition(f"{src} -> {dst} is not allowed")
        cur = con.execute(
            f"UPDATE batches SET state=?, updated_at=?{', ' + sets if sets else ''} "
            "WHERE batch_id=? AND state=?",
            (dst.value, _now(), *args, batch_id, src.value),
        )  # fmt: skip
        if cur.rowcount != 1:
            raise IllegalTransition(f"batch {batch_id} is not in state {src}; cannot move to {dst}")

    def mark_interrupted(self, batch_id: str, detail: dict) -> BatchRow:
        """Record that an in-flight attempt died. Keeps durable evidence in `attempt_log`."""
        with self._txn() as con:
            row = self.get_batch(batch_id)
            if row is None:
                raise ManifestError(f"unknown batch {batch_id}")
            if row.state is BatchState.INTERRUPTED:
                return row
            self._cas(con, batch_id, row.state, BatchState.INTERRUPTED)
            con.execute(
                "INSERT INTO attempt_log (batch_id, attempt, outcome, detail, recorded_at) "
                "VALUES (?, ?, 'INTERRUPTED', ?, ?)",
                (
                    batch_id,
                    row.attempt,
                    json.dumps({"state_at_crash": row.state.value, **detail}),
                    _now(),
                ),
            )
        return self.get_batch(batch_id)  # type: ignore[return-value]

    def attempt_log(self, batch_id: str | None = None) -> list[tuple[str, int, str, dict]]:
        sql = "SELECT batch_id, attempt, outcome, detail FROM attempt_log"
        args: tuple = ()
        if batch_id:
            sql, args = sql + " WHERE batch_id=?", (batch_id,)
        return [
            (b, a, o, json.loads(d)) for b, a, o, d in self._con.execute(sql + " ORDER BY id", args)
        ]

    def indexed_events(self, batch_id: str) -> dict[str, bytes]:
        """event_id -> payload fingerprint for everything a committed batch published."""
        return {
            eid: bytes(fp)
            for eid, fp in self._con.execute(
                "SELECT event_id, fingerprint FROM event_index WHERE batch_id=?", (batch_id,)
            )
        }

    def check_consistency(self) -> list[str]:
        """Internal cross-table checks (the SQL verifier of Milestone 5 re-checks against Parquet)."""
        problems = []
        indexed = dict(
            self._con.execute("SELECT batch_id, count(*) FROM event_index GROUP BY batch_id")
        )
        filed = dict(
            self._con.execute("SELECT batch_id, sum(rows) FROM batch_files GROUP BY batch_id")
        )
        committed = {
            b: rows
            for b, rows in self._con.execute(
                "SELECT batch_id, committed_rows FROM batches WHERE state = 'COMMITTED'"
            )
        }
        for bid, rows in committed.items():
            n_idx, n_files = indexed.get(bid, 0), filed.get(bid, 0)
            if n_idx != rows or n_files != rows:
                problems.append(
                    f"{bid}: committed_rows={rows}, event_index rows={n_idx}, file rows={n_files}"
                )
        stray = sum(n for b, n in indexed.items() if b not in committed)
        if stray:
            problems.append(f"{stray} event_index rows belong to non-committed batches")
        return problems

    def transition(self, batch_id: str, src: BatchState, dst: BatchState) -> None:
        with self._txn() as con:
            self._cas(con, batch_id, src, dst)

    def record_staged(
        self,
        batch_id: str,
        accounting: Accounting,
        files: Sequence[StagedFile],
        logical: str,
        reason_counts: dict[str, int],
        evidence_path: str | None,
    ) -> None:
        """VALIDATING -> STAGED, persisting the exact staged files and their fingerprints."""
        output = hashlib.sha256(
            "".join(f"{f.relative_path}:{f.sha256}\n" for f in files).encode()
        ).hexdigest()
        with self._txn() as con:
            self._cas(
                con, batch_id, S.VALIDATING, S.STAGED,
                "consumed=?, accepted=?, duplicates=?, rejected=?, unresolved=?, reason_counts=?, "
                "evidence_path=?, output_digest=?, logical_digest=?",
                (accounting.consumed, accounting.accepted, accounting.duplicates, accounting.rejected,
                 accounting.unresolved, json.dumps(reason_counts), evidence_path, output, logical),
            )  # fmt: skip
            con.execute("DELETE FROM batch_files WHERE batch_id=?", (batch_id,))
            con.executemany(
                "INSERT INTO batch_files VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (batch_id, f.relative_path, f.event_date.isoformat(), f.rows, f.size_bytes,
                     f.sha256, _us(f.min_event_time), _us(f.max_event_time))
                    for f in files
                ],
            )  # fmt: skip

    def reject(
        self,
        batch_id: str,
        src: BatchState,
        accounting: Accounting | None,
        reason_counts: dict[str, int],
        critical_failures: Sequence[str],
        evidence_path: str | None,
    ) -> None:
        """Terminal for this attempt. A rejected batch has no committed output, ever."""
        a = accounting
        with self._txn() as con:
            self._cas(
                con, batch_id, src, S.REJECTED,
                "consumed=COALESCE(?, consumed), accepted=COALESCE(?, accepted), "
                "duplicates=COALESCE(?, duplicates), rejected=COALESCE(?, rejected), "
                "unresolved=COALESCE(?, unresolved), reason_counts=?, critical_failures=?, evidence_path=?",
                (a.consumed if a else None, a.accepted if a else None, a.duplicates if a else None,
                 a.rejected if a else None, a.unresolved if a else None, json.dumps(reason_counts),
                 json.dumps(list(critical_failures)), evidence_path),
            )  # fmt: skip

    def commit(
        self,
        batch_id: str,
        events: Sequence[tuple[str, str, int, bytes, int]],
    ) -> int:
        """VERIFIED -> COMMITTED in ONE transaction; returns the commit sequence number.

        `events`: (event_id, device_id, sequence_number, fingerprint, ingestion_us) per accepted
        event. Visibility of the batch, its files and its identities all flip at this COMMIT.
        """
        try:
            with self._txn() as con:
                seq = con.execute(
                    "SELECT COALESCE(MAX(commit_seq), 0) + 1 FROM batches"
                ).fetchone()[0]
                self._cas(
                    con, batch_id, S.VERIFIED, S.COMMITTED,
                    "committed_rows=?, commit_seq=?", (len(events), seq),
                )  # fmt: skip
                con.executemany(
                    "INSERT INTO event_index (event_key, event_id, device_id, sequence_number, "
                    "fingerprint, ingestion_us, batch_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [(event_key(e), e, d, s, fp, ing, batch_id) for e, d, s, fp, ing in events],
                )
                failpoints.hit("in_commit")  # window 4: statements issued, COMMIT not yet durable
        except sqlite3.IntegrityError as exc:
            raise CommitConflictError(
                f"commit of {batch_id} violated an identity constraint: {exc}"
            ) from exc
        return seq


def _us(dt: datetime) -> int:
    return (dt - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)

"""Publication protocol: stage -> verify -> place -> commit.

    1. STAGE    write Parquet under staging/<batch_id>/, fsynced, SHA-256 per file;
                record the exact files in the manifest            (VALIDATING -> STAGED)
    2. VERIFY   re-read the staged bytes: hash, schema, row counts, partition, event IDs
                (STAGED -> VERIFIED)
    3. PLACE    hard-link each file into data/ (no-clobber), fsync directories.
                The files are now on disk but still *unreferenced* - invisible to
                manifest-aware readers.
    4. COMMIT   one SQLite transaction flips the batch to COMMITTED and inserts its event
                identities (VERIFIED -> COMMITTED). This is the visibility boundary.
    5. CLEANUP  remove the staging links (best effort; leftovers are harmless).

Atomicity scope: step 4 makes the batch visible all-or-nothing to readers that ask the manifest
(`reader.visible_files`). A reader that globs ``data/`` can see placed files between steps 3 and 4,
or orphans after a crash - that is why every query path in this project goes through the manifest.
Crash handling for each step is specified in docs/recovery.md; Milestone 3 guarantees *safety*
(nothing partial is ever visible), Milestone 4 adds automatic *recovery*.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import time
from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow.parquet as pq

from telemetry_lakehouse import failpoints
from telemetry_lakehouse.batcher import Batch
from telemetry_lakehouse.config import StorageConfig
from telemetry_lakehouse.fsutil import ImmutableFileExistsError, fsync_dir
from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.manifest import (
    BatchState,
    CommitConflictError,
    Manifest,
    logical_digest,
)
from telemetry_lakehouse.parquet_writer import StagedFile, event_dates, write_batch
from telemetry_lakehouse.schemas import EVENT_SCHEMA, TelemetryEvent, table_from_events
from telemetry_lakehouse.validation import GateResult


class VerificationError(Exception):
    """Staged files do not match what the gate accepted; nothing was published."""


def verify_staged(files: Sequence[StagedFile], accepted: Sequence[TelemetryEvent]) -> None:
    """Independently re-read the staged files and compare them with what the gate accepted."""
    expected_ids = {e.event_id for e in accepted}
    seen_ids: set[str] = set()
    rows = 0
    for f in files:
        data = f.path.read_bytes()
        if hashlib.sha256(data).hexdigest() != f.sha256 or len(data) != f.size_bytes:
            raise VerificationError(f"{f.relative_path}: bytes differ from the recorded SHA-256")
        meta = pq.read_metadata(f.path)
        if meta.num_rows != f.rows:
            raise VerificationError(f"{f.relative_path}: {meta.num_rows} rows, expected {f.rows}")
        if not pq.read_schema(f.path).equals(EVENT_SCHEMA):
            raise VerificationError(f"{f.relative_path}: schema differs from EVENT_SCHEMA")
        table = pq.read_table(f.path)
        if any(d.as_py() != f.event_date for d in event_dates(table).unique()):
            raise VerificationError(f"{f.relative_path}: rows outside partition {f.event_date}")
        ids = table["event_id"].to_pylist()
        if len(set(ids)) != len(ids) or seen_ids & set(ids):
            raise VerificationError(f"{f.relative_path}: duplicate event_id in staged output")
        seen_ids.update(ids)
        rows += table.num_rows
    if rows != len(accepted) or seen_ids != expected_ids:
        raise VerificationError(
            f"staged {rows} rows / {len(seen_ids)} ids, gate accepted {len(accepted)}"
        )


def place_staged(files: Sequence[StagedFile], layout: StorageLayout) -> None:
    """Link staged files into ``data/`` without ever overwriting (all-or-error pre-check)."""
    targets = [layout.data_path(f.relative_path) for f in files]
    existing = [t for t in targets if t.exists()]
    if existing:
        raise ImmutableFileExistsError(f"refusing to overwrite existing data file {existing[0]}")
    for f, target in zip(files, targets, strict=True):
        target.parent.mkdir(parents=True, exist_ok=True)
        os.link(f.path, target)  # atomic; fails if the target exists
        fsync_dir(target.parent)


def cleanup_staged(files: Sequence[StagedFile]) -> None:
    for f in files:
        with contextlib.suppress(FileNotFoundError):
            f.path.unlink()
        with contextlib.suppress(OSError):  # directory still holds other files
            f.path.parent.rmdir()
            f.path.parent.parent.rmdir()


@dataclass(frozen=True, slots=True)
class PublishOutcome:
    commit_seq: int
    files: int
    bytes: int
    committed_rows: int
    stage_s: float
    verify_s: float
    place_s: float
    commit_s: float


class Publisher:
    def __init__(self, layout: StorageLayout, manifest: Manifest, storage: StorageConfig) -> None:
        self.layout = layout
        self.manifest = manifest
        self.storage = storage

    def publish(
        self, batch: Batch, result: GateResult, evidence_path: str | None = None
    ) -> PublishOutcome:
        """Run the protocol for an ADMITted batch whose manifest row is VALIDATING."""
        m = self.manifest
        failpoints.hit("before_stage")  # window 1
        t0 = time.perf_counter()
        staged: list[StagedFile] = []
        if result.accepted:
            table = table_from_events(result.accepted)
            staged = write_batch(table, batch.batch_id, self.layout, self.storage)  # window 2
        m.record_staged(
            batch.batch_id,
            result.accounting,
            staged,
            logical_digest(result.accepted_fingerprints),
            result.reason_counts,
            evidence_path,
        )
        failpoints.hit("after_stage")  # window 3
        t1 = time.perf_counter()
        try:
            verify_staged(staged, result.accepted)
        except VerificationError as exc:
            m.reject(
                batch.batch_id,
                BatchState.STAGED,
                None,
                {},
                [f"STAGING_VERIFICATION_FAILED: {exc}"],
                evidence_path,
            )
            raise
        m.transition(batch.batch_id, BatchState.STAGED, BatchState.VERIFIED)
        t2 = time.perf_counter()
        place_staged(staged, self.layout)
        failpoints.hit("after_place")  # window 3 (files in data/, still unreferenced)
        t3 = time.perf_counter()
        entries = [
            (e.event_id, e.device_id, e.sequence_number, fp, ing)
            for e, fp, ing in zip(
                result.accepted,
                result.accepted_fingerprints,
                result.accepted_ingestion_us,
                strict=True,
            )
        ]
        try:
            seq = m.commit(batch.batch_id, entries)  # windows 4 (inside) and the visibility flip
        except CommitConflictError as exc:
            for f in staged:  # nothing references our placed files: remove them
                with contextlib.suppress(FileNotFoundError):
                    self.layout.data_path(f.relative_path).unlink()
            m.reject(
                batch.batch_id,
                BatchState.VERIFIED,
                None,
                {},
                [f"EVENT_INDEX_CONFLICT: {exc}"],
                None,
            )
            raise
        failpoints.hit("after_commit")  # window 5
        t4 = time.perf_counter()
        cleanup_staged(staged)
        return PublishOutcome(
            commit_seq=seq,
            files=len(staged),
            bytes=sum(f.size_bytes for f in staged),
            committed_rows=len(entries),
            stage_s=t1 - t0,
            verify_s=t2 - t1,
            place_s=t3 - t2,
            commit_s=t4 - t3,
        )

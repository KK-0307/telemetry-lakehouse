"""Crash recovery, orphan cleanup, deterministic replay, and committed-file verification.

Recovery is a deterministic function of durable state; it never trusts a file merely because it
exists. The rule for an unfinished batch is:

    RESUME   iff every file the manifest recorded for it has an intact copy (staging/ or data/,
             matching the recorded size and SHA-256) AND the logical digest recomputed from those
             files equals the digest recorded when the gate approved the batch.
             -> finish placement and run the commit transaction.
    REPLAY   otherwise: discard partial work (kept as evidence), then re-run the batch over its
             *recorded* source interval through the normal pipeline.

Order of a recovery run (each step is re-runnable; a crash inside recovery is safe):

    1  resume or interrupt every in-flight batch
    2  sweep orphans: unreferenced files in data/, remnants in staging/ -> `rejected/` (evidence)
    3  replay INTERRUPTED batches from the source (if available)
    4  verify committed files against the manifest; optionally repair by deterministic replay
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from telemetry_lakehouse import failpoints, log
from telemetry_lakehouse.batcher import Batch, compute_batch_id
from telemetry_lakehouse.config import GateConfig, StorageConfig
from telemetry_lakehouse.evidence import next_attempt_dir
from telemetry_lakehouse.fsutil import fsync_dir
from telemetry_lakehouse.index import event_fingerprint, payload_fingerprint
from telemetry_lakehouse.ingest import IngestSummary, JsonlSource, Pipeline
from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.manifest import (
    IN_FLIGHT,
    BatchRow,
    BatchState,
    CommitConflictError,
    CommittedFile,
    Manifest,
    logical_digest,
)
from telemetry_lakehouse.parquet_writer import StagedFile, write_batch
from telemetry_lakehouse.schemas import TelemetryEvent, table_from_events

_COLUMNS = [
    "event_id", "device_id", "sequence_number", "event_time", "ingestion_time", "schema_version",
    "temperature_c", "humidity_pct", "battery_pct", "signal_dbm", "status",
]  # fmt: skip


class ReplayError(Exception):
    pass


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _file_ok(path: Path, f: CommittedFile) -> bool:
    try:
        return path.stat().st_size == f.size_bytes and sha256_file(path) == f.sha256
    except FileNotFoundError:
        return False


def _entries_from_files(paths: list[Path]) -> tuple[list[tuple], list[bytes]]:
    """Commit entries + fingerprints derived from the (verified) Parquet content itself."""
    entries: list[tuple] = []
    fps: list[bytes] = []
    for path in paths:
        for batch in pq.ParquetFile(path).iter_batches(columns=_COLUMNS):
            col = {n: batch.column(n).to_pylist() for n in _COLUMNS if not n.endswith("_time")}
            et = pc.cast(batch.column("event_time"), pa.int64()).to_pylist()
            it = pc.cast(batch.column("ingestion_time"), pa.int64()).to_pylist()
            for i in range(batch.num_rows):
                fp = payload_fingerprint(
                    col["device_id"][i], col["sequence_number"][i], et[i], col["schema_version"][i],
                    col["temperature_c"][i], col["humidity_pct"][i], col["battery_pct"][i],
                    col["signal_dbm"][i], col["status"][i],
                )  # fmt: skip
                fps.append(fp)
                entries.append(
                    (col["event_id"][i], col["device_id"][i], col["sequence_number"][i], fp, it[i])
                )
    return entries, fps


# ---------------------------------------------------------------- evidence moves
def _unique(path: Path) -> Path:
    if not path.exists():
        return path
    n = 1
    while path.with_name(f"{path.name}.{n}").exists():
        n += 1
    return path.with_name(f"{path.name}.{n}")


def _quarantine_file(layout: StorageLayout, path: Path, relative: str) -> Path:
    """Move an untrusted file out of `data/` into the evidence area (never deleted)."""
    target = _unique(layout.rejected_dir / "orphans" / relative)
    target.parent.mkdir(parents=True, exist_ok=True)
    os.rename(path, target)
    fsync_dir(path.parent)
    return target


def _preserve_staging(layout: StorageLayout, batch_id: str, note: dict) -> Path | None:
    """Move a batch's staging directory into its next attempt's evidence directory."""
    src = layout.staging_dir / batch_id
    if not src.exists():
        return None
    attempt_dir = next_attempt_dir(layout, batch_id)
    attempt_dir.mkdir(parents=True, exist_ok=True)
    target = attempt_dir / "staging-remnants"
    os.rename(src, target)
    (attempt_dir / "interrupted.json").write_text(json.dumps(note, indent=2) + "\n")
    return target


# ---------------------------------------------------------------- file verification
@dataclass(frozen=True, slots=True)
class FileProblem:
    batch_id: str
    relative_path: str
    kind: str  # MISSING | SIZE_MISMATCH | SHA_MISMATCH | UNREADABLE | ROWS_MISMATCH
    detail: str = ""


def check_committed_files(
    layout: StorageLayout, manifest: Manifest, *, deep: bool = True
) -> list[FileProblem]:
    """Compare every committed file with what the manifest recorded."""
    problems = []
    for f in manifest.committed_files():
        path = layout.data_path(f.relative_path)
        if not path.exists():
            problems.append(FileProblem(f.batch_id, f.relative_path, "MISSING"))
            continue
        size = path.stat().st_size
        if size != f.size_bytes:
            detail = f"{size} bytes on disk, {f.size_bytes} recorded"
            problems.append(FileProblem(f.batch_id, f.relative_path, "SIZE_MISMATCH", detail))
            continue
        if not deep:
            continue
        if sha256_file(path) != f.sha256:
            problems.append(FileProblem(f.batch_id, f.relative_path, "SHA_MISMATCH"))
            continue
        try:
            rows = pq.read_metadata(path).num_rows
        except Exception as exc:
            problems.append(FileProblem(f.batch_id, f.relative_path, "UNREADABLE", str(exc)))
            continue
        if rows != f.rows:
            detail = f"{rows} rows, {f.rows} recorded"
            problems.append(FileProblem(f.batch_id, f.relative_path, "ROWS_MISMATCH", detail))
    return problems


# ---------------------------------------------------------------- deterministic replay
@dataclass(slots=True)
class Rebuilt:
    files: dict[str, StagedFile]
    logical: str


@contextmanager
def rebuild_batch(
    layout: StorageLayout,
    manifest: Manifest,
    row: BatchRow,
    source: JsonlSource,
    storage: StorageConfig,
) -> Iterator[Rebuilt]:
    """Re-derive a COMMITTED batch's Parquet from the source in a scratch directory.

    The records of ``[start, end)`` are re-validated, restricted to the events the manifest says
    this batch published (matching fingerprint), and written exactly as at commit time.
    """
    if source.source_id != row.source_id:
        raise ReplayError(f"source is {source.source_id!r}, batch belongs to {row.source_id!r}")
    wanted = manifest.indexed_events(row.batch_id)
    events: dict[str, TelemetryEvent] = {}
    for rec in source.read_range(row.start_offset, row.end_offset):
        try:
            e = TelemetryEvent.model_validate_json(rec.payload)
        except ValueError:
            continue
        fp = wanted.get(e.event_id)
        if fp is not None and e.event_id not in events and event_fingerprint(e) == fp:
            events[e.event_id] = e
    if set(events) != set(wanted):
        raise ReplayError(
            f"source no longer reproduces the batch: {len(wanted) - len(events)} of "
            f"{len(wanted)} published events are missing or changed"
        )
    ordered = list(events.values())
    with tempfile.TemporaryDirectory(prefix=".replay-", dir=layout.root) as tmp:
        scratch = StorageLayout(Path(tmp))
        staged = (
            write_batch(table_from_events(ordered), row.batch_id, scratch, storage)
            if ordered
            else []
        )
        yield Rebuilt(
            {f.relative_path: f for f in staged},
            logical_digest([event_fingerprint(e) for e in ordered]),
        )


@dataclass(frozen=True, slots=True)
class ReplayResult:
    batch_id: str
    offsets: tuple[int, int]
    rows: int
    ok: bool
    logical_match: bool
    bytes_match: bool
    detail: str
    seconds: float


def replay_batch(
    layout: StorageLayout,
    manifest: Manifest,
    row: BatchRow,
    source: JsonlSource,
    storage: StorageConfig,
) -> ReplayResult:
    """Rebuild one committed batch from the source and compare it with the manifest."""
    t0 = time.perf_counter()
    recorded = {f.relative_path: f for f in manifest.staged_files(row.batch_id)}
    try:
        with rebuild_batch(layout, manifest, row, source, storage) as rebuilt:
            logical = rebuilt.logical == row.logical_digest
            same_names = set(rebuilt.files) == set(recorded)
            bytes_ok = same_names and all(
                rebuilt.files[p].sha256 == recorded[p].sha256 for p in recorded
            )
    except ReplayError as exc:
        return ReplayResult(
            row.batch_id, (row.start_offset, row.end_offset), row.committed_rows or 0,
            False, False, False, str(exc), time.perf_counter() - t0,
        )  # fmt: skip
    if logical and bytes_ok:
        detail = "identical"
    elif logical:
        detail = "same logical content, different bytes (library/version drift?)"
    else:
        detail = "content differs"
    return ReplayResult(
        row.batch_id, (row.start_offset, row.end_offset), row.committed_rows or 0,
        logical and bytes_ok, logical, bytes_ok, detail, time.perf_counter() - t0,
    )  # fmt: skip


def replay_committed(
    layout: StorageLayout,
    manifest: Manifest,
    source: JsonlSource,
    storage: StorageConfig,
    batch_id: str | None = None,
) -> list[ReplayResult]:
    rows = [
        b
        for b in manifest.batches(BatchState.COMMITTED)
        if b.source_id == source.source_id and (batch_id is None or b.batch_id == batch_id)
    ]
    return [replay_batch(layout, manifest, b, source, storage) for b in rows]


# ---------------------------------------------------------------- recovery
@dataclass(slots=True)
class RecoveryReport:
    actions: list[dict] = field(default_factory=list)
    orphans_quarantined: list[str] = field(default_factory=list)
    staging_cleaned: list[str] = field(default_factory=list)
    problems: list[FileProblem] = field(default_factory=list)  # committed-file problems found
    repaired: list[str] = field(default_factory=list)
    unresolved: list[str] = field(default_factory=list)
    consistency_problems: list[str] = field(default_factory=list)
    replay: IngestSummary | None = None
    seconds: float = 0.0

    @property
    def ok(self) -> bool:
        remaining = [p for p in self.problems if p.relative_path not in self.repaired]
        blocked = bool(self.replay and self.replay.blocked)
        return not (remaining or self.unresolved or self.consistency_problems or blocked)

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "seconds": round(self.seconds, 3),
            "actions": self.actions,
            "orphans_quarantined": self.orphans_quarantined,
            "staging_cleaned": self.staging_cleaned,
            "committed_file_problems": [
                {
                    "batch_id": p.batch_id,
                    "path": p.relative_path,
                    "kind": p.kind,
                    "detail": p.detail,
                }
                for p in self.problems
            ],
            "repaired": self.repaired,
            "unresolved": self.unresolved,
            "consistency_problems": self.consistency_problems,
            "replayed_batches": self.replay.batches if self.replay else 0,
            "replay_blocked": [b.batch_id for b in self.replay.blocked] if self.replay else [],
        }


def _resume(layout: StorageLayout, manifest: Manifest, row: BatchRow) -> str | None:
    """Finish a STAGED/VERIFIED batch iff all its recorded files are intact. Else return None."""
    if row.state not in (BatchState.STAGED, BatchState.VERIFIED) or row.logical_digest is None:
        return None
    files = manifest.staged_files(row.batch_id)
    plan: list[tuple[CommittedFile, Path, Path, bool]] = []
    damaged_placed: list[tuple[Path, str]] = []
    for f in files:
        staged = layout.staging_dir / row.batch_id / f.relative_path
        placed = layout.data_path(f.relative_path)
        placed_ok = placed.exists() and _file_ok(placed, f)
        staged_ok = staged.exists() and _file_ok(staged, f)
        if placed.exists() and not placed_ok:
            damaged_placed.append((placed, f.relative_path))
        if not (placed_ok or staged_ok):
            return None
        plan.append((f, staged, placed, placed_ok))
    entries, fps = _entries_from_files([placed if ok else staged for _, staged, placed, ok in plan])
    if len(entries) != row.accepted or logical_digest(fps) != row.logical_digest:
        return None

    for path, rel in damaged_placed:  # unreferenced and wrong: evidence, not data
        _quarantine_file(layout, path, rel)
    if row.state is BatchState.STAGED:
        manifest.transition(row.batch_id, BatchState.STAGED, BatchState.VERIFIED)
    failpoints.hit("recover_before_commit")
    for _, staged, placed, placed_ok in plan:
        if not placed_ok:
            placed.parent.mkdir(parents=True, exist_ok=True)
            os.link(staged, placed)
            fsync_dir(placed.parent)
    try:
        manifest.commit(row.batch_id, entries)
    except CommitConflictError as exc:
        manifest.reject(
            row.batch_id, BatchState.VERIFIED, None, {}, [f"EVENT_INDEX_CONFLICT: {exc}"], None
        )
        for _, _, placed, _ in plan:
            if placed.exists():
                _quarantine_file(layout, placed, placed.relative_to(layout.data_dir).as_posix())
        return "rejected_on_resume"
    return "resumed_and_committed"


def _sweep_orphans(layout: StorageLayout, manifest: Manifest, report: RecoveryReport) -> None:
    committed = {f.relative_path: f for f in manifest.committed_files()}
    for path in sorted(layout.data_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(layout.data_dir).as_posix()
        if path.name.startswith(".") and path.name.endswith(".tmp"):
            path.unlink()  # torn temp file: no evidentiary value
        elif rel not in committed:
            target = _quarantine_file(layout, path, rel)
            report.orphans_quarantined.append(rel)
            log.emit("orphan_quarantined", path=rel, to=str(target))
    for d in sorted(layout.staging_dir.iterdir()) if layout.staging_dir.exists() else []:
        row = manifest.get_batch(d.name)
        if row is not None and row.state is BatchState.COMMITTED:
            # Window 5 leftovers: harmless links to files that are verified in data/. Remove them.
            ok = all(
                _file_ok(layout.data_path(f.relative_path), f)
                for f in manifest.staged_files(d.name)
            )
            if ok:
                _remove_tree(d)
                report.staging_cleaned.append(d.name)
                continue
        if row is not None and row.state in IN_FLIGHT - {BatchState.INTERRUPTED}:
            continue  # still being worked on (cannot happen after step 1; defensive)
        note = {"batch_id": d.name, "reason": "unreferenced staging remnants"}
        if _preserve_staging(layout, d.name, note) is None:
            continue
        report.staging_cleaned.append(d.name)


def _remove_tree(path: Path) -> None:
    for p in sorted(path.rglob("*"), reverse=True):
        p.unlink() if p.is_file() or p.is_symlink() else p.rmdir()
    path.rmdir()


def recover(
    layout: StorageLayout,
    manifest: Manifest,
    *,
    source: JsonlSource | None = None,
    storage: StorageConfig | None = None,
    gate: GateConfig | None = None,
    verify_files: bool = True,
    repair: bool = False,
) -> RecoveryReport:
    storage = storage or StorageConfig()
    gate = gate or GateConfig()
    layout.ensure()
    report = RecoveryReport()
    t0 = time.perf_counter()

    # 1. resume or interrupt every in-flight batch
    for row in manifest.in_flight():
        if row.state is BatchState.INTERRUPTED:
            continue
        outcome = _resume(layout, manifest, row)
        if outcome is not None:
            report.actions.append(
                {"batch_id": row.batch_id, "from": row.state.value, "action": outcome}
            )
            log.emit("batch_recovered", batch_id=row.batch_id, action=outcome)
            continue
        staged = [f.relative_path for f in manifest.staged_files(row.batch_id)]
        manifest.mark_interrupted(row.batch_id, {"recorded_files": staged})
        failpoints.hit("recover_after_interrupt")
        report.actions.append(
            {"batch_id": row.batch_id, "from": row.state.value, "action": "interrupted_for_replay"}
        )

    # 2. orphan sweep (must precede replay: stale files would block no-clobber placement)
    _sweep_orphans(layout, manifest, report)

    # 3. replay INTERRUPTED batches over their recorded intervals
    pending = manifest.batches(BatchState.INTERRUPTED)
    if pending:
        report.replay = IngestSummary(source_id=source.source_id if source else "")
        pipeline = Pipeline(layout, manifest, storage, gate, manifest.load_index())
        for row in pending:
            if source is None or source.source_id != row.source_id:
                report.unresolved.append(
                    f"{row.batch_id}: needs source {row.source_id!r} to replay "
                    f"[{row.start_offset}, {row.end_offset})"
                )
                continue
            assert compute_batch_id(row.source_id, row.start_offset, row.end_offset) == row.batch_id
            records = source.read_range(row.start_offset, row.end_offset)
            batch = Batch(
                row.batch_id, row.source_id, row.start_offset, row.end_offset, records, "recovery"
            )
            halted = pipeline.process(batch, report.replay)
            report.actions.append(
                {"batch_id": row.batch_id, "from": "INTERRUPTED", "action": "replayed"}
            )
            if halted:
                break

    # 4. verify committed files; optionally repair by deterministic replay
    report.consistency_problems = manifest.check_consistency()
    if verify_files:
        report.problems = check_committed_files(layout, manifest, deep=True)
        if repair and report.problems and source is not None:
            _repair(layout, manifest, source, storage, report)
    report.seconds = time.perf_counter() - t0
    return report


def _repair(
    layout: StorageLayout,
    manifest: Manifest,
    source: JsonlSource,
    storage: StorageConfig,
    report: RecoveryReport,
) -> None:
    """Restore damaged committed files iff replay reproduces them byte for byte."""
    by_batch: dict[str, list[FileProblem]] = {}
    for p in report.problems:
        by_batch.setdefault(p.batch_id, []).append(p)
    for batch_id, problems in by_batch.items():
        row = manifest.get_batch(batch_id)
        assert row is not None
        try:
            with rebuild_batch(layout, manifest, row, source, storage) as rebuilt:
                for p in problems:
                    fresh = rebuilt.files.get(p.relative_path)
                    recorded = next(
                        f
                        for f in manifest.staged_files(batch_id)
                        if f.relative_path == p.relative_path
                    )
                    if fresh is None or fresh.sha256 != recorded.sha256:
                        report.unresolved.append(
                            f"{p.relative_path}: replay does not reproduce the committed bytes"
                        )
                        continue
                    target = layout.data_path(p.relative_path)
                    if target.exists():
                        _quarantine_file(layout, target, p.relative_path)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    os.link(fresh.path, target)
                    fsync_dir(target.parent)
                    report.repaired.append(p.relative_path)
                    log.emit("committed_file_repaired", path=p.relative_path, kind=p.kind)
        except ReplayError as exc:
            report.unresolved.append(f"{batch_id}: {exc}")

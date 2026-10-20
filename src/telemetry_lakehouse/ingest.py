"""Ingestion: read a source incrementally, micro-batch it, gate it, publish it atomically.

    JsonlSource -> MicroBatcher -> Manifest.begin_batch -> evaluate_batch (gate)
                                                            |-- BLOCK --> evidence + REJECTED
                                                            '-- ADMIT --> Publisher: stage, verify,
                                                                          place, COMMIT

Order of side effects is what keeps untrusted data out: the batch is registered (boundaries made
durable) before any work; the gate runs before anything is written; a blocked batch writes only
evidence under ``rejected/``; the in-memory index is updated only after the manifest commit.

Re-submitting a batch that is already COMMITTED is a no-op (idempotent retry). An unfinished
earlier attempt is *not* retried here: `telemetry recover` (Milestone 4) resolves it, and until
then ingestion refuses to proceed past it.
"""

from __future__ import annotations

import json
import os
import resource
import sys
import time
from collections import Counter
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from itertools import islice
from pathlib import Path

from telemetry_lakehouse import log
from telemetry_lakehouse.batcher import Batch, MicroBatcher, SourceRecord
from telemetry_lakehouse.config import BatchConfig, GateConfig, StorageConfig
from telemetry_lakehouse.evidence import persist_evidence
from telemetry_lakehouse.index import CommittedIndex, Gap
from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.manifest import (
    BatchState,
    CommitConflictError,
    Manifest,
    SourceMutatedError,
)
from telemetry_lakehouse.publisher import Publisher, VerificationError
from telemetry_lakehouse.simulator import SOURCE_FORMAT, sidecar_path
from telemetry_lakehouse.validation import evaluate_batch

MAX_REPORTED_GAPS = 1000


class JsonlSource:
    """Offset-addressable JSONL log. Offset = 0-based record ordinal; reading is incremental.

    ``complete`` / ``final_sequences`` are the source's end-of-stream marker (from the sidecar):
    only when ``complete`` may the pipeline claim that missing sequence numbers are lost for good.
    """

    def __init__(
        self,
        path: Path,
        source_id: str,
        start_offset: int = 0,
        *,
        complete: bool = False,
        final_sequences: dict[str, int] | None = None,
    ) -> None:
        self.path = path
        self.source_id = source_id
        self.start_offset = start_offset
        self.complete = complete
        self.final_sequences = final_sequences

    @classmethod
    def open(cls, path: Path, source_id: str | None = None, start_offset: int = 0) -> JsonlSource:
        if not path.exists():
            raise FileNotFoundError(f"source log not found: {path} (run `telemetry simulate`)")
        meta_file = sidecar_path(path)
        complete, finals = False, None
        if meta_file.exists():
            meta = json.loads(meta_file.read_text())
            if meta.get("format") != SOURCE_FORMAT:
                raise ValueError(f"unsupported source format {meta.get('format')!r}")
            source_id = source_id or meta["source_id"]
            complete = bool(meta.get("complete", False))
            finals = meta.get("final_sequences") if complete else None
        return cls(
            path, source_id or path.stem, start_offset, complete=complete, final_sequences=finals
        )

    def read_range(self, start: int, end: int) -> list[SourceRecord]:
        """Records ``[start, end)`` - used to replay a batch over its recorded boundaries."""
        with self.path.open("rb") as fh:
            records = [
                SourceRecord(off, line.rstrip(b"\n"))
                for off, line in enumerate(islice(fh, start, end), start=start)
            ]
        if len(records) != end - start:
            raise ValueError(
                f"source holds {len(records)} of the {end - start} records in [{start}, {end})"
            )
        return records

    def __iter__(self) -> Iterator[SourceRecord]:
        with self.path.open("rb") as fh:
            lines = islice(fh, self.start_offset, None)
            for offset, line in enumerate(lines, start=self.start_offset):
                yield SourceRecord(offset, line.rstrip(b"\n"))


@dataclass(slots=True)
class BatchMetrics:
    batch_id: str
    start_offset: int
    end_offset: int
    records: int
    files: int
    bytes: int
    commit_seq: int
    gate_s: float
    stage_s: float
    verify_s: float
    place_s: float
    commit_s: float

    @property
    def total_s(self) -> float:
        return self.gate_s + self.stage_s + self.verify_s + self.place_s + self.commit_s


@dataclass(slots=True)
class BlockedBatch:
    batch_id: str
    start_offset: int
    end_offset: int
    reason_counts: dict[str, int]
    critical_failures: list[str]
    evidence: str


@dataclass(slots=True)
class IngestSummary:
    source_id: str
    records: int = 0
    batches: int = 0
    files: int = 0
    bytes_written: int = 0
    elapsed_s: float = 0.0
    peak_rss_mb: float = 0.0
    halted: bool = False
    skipped_batches: int = 0  # already COMMITTED: idempotent no-ops
    skipped_records: int = 0
    accounting: Counter[str] = field(default_factory=Counter)
    out_of_order: int = 0
    late: int = 0
    blocked: list[BlockedBatch] = field(default_factory=list)
    gaps: list[Gap] = field(default_factory=list)
    gap_basis: str | None = None  # None = completeness not evaluated (run halted)
    open_gaps: int = 0
    batch_metrics: list[BatchMetrics] = field(default_factory=list)

    @property
    def events_per_second(self) -> float:
        return self.records / self.elapsed_s if self.elapsed_s else 0.0

    @property
    def integrity_ok(self) -> bool:
        return not self.blocked and not self.gaps

    def render_text(self) -> str:
        """Human-readable run summary (the JSON form is `to_dict`)."""
        committed = self.accounting.get("committed", 0)
        lines = [
            f"source {self.source_id}: read {self.records:,} records in {self.batches} batches -> "
            f"{committed:,} rows committed in {self.files} file{'s' if self.files != 1 else ''} "
            f"({self.skipped_batches} batches already committed, {len(self.blocked)} blocked)"
            + (
                f" in {self.elapsed_s:.2f}s ({self.events_per_second:,.0f} events/s)"
                if self.elapsed_s
                else ""
            )
        ]
        a = self.accounting
        if a:
            lines.append(
                f"  accounting: consumed={a['consumed']:,} accepted={a['accepted']:,} "
                f"duplicates={a['duplicates']:,} rejected={a['rejected']:,} "
                f"unresolved={a['unresolved']:,} committed={committed:,}"
            )
        if self.blocked:
            lines.append(
                "  (a blocked batch is classified but publishes nothing: "
                "committed counts only admitted batches)"
            )
        for b in self.blocked:
            reasons = ", ".join(f"{k}={v}" for k, v in b.reason_counts.items()) or "; ".join(
                b.critical_failures
            )
            lines.append(
                f"  BLOCKED batch {b.batch_id} offsets [{b.start_offset:,}, {b.end_offset:,}): "
                f"{reasons}"
            )
            if b.evidence:
                lines.append(f"    evidence: {b.evidence}/report.json")
        if self.halted:
            lines.append("  run halted at the blocked batch: nothing after it was published")
        if self.gap_basis is not None:
            missing = sum(g.missing for g in self.gaps)
            lines.append(
                f"  sequences: {len(self.gaps)} gaps proven "
                f"({missing} events missing, basis: {self.gap_basis})"
            )
        return "\n".join(lines)

    def to_dict(self) -> dict:
        latencies = sorted(m.total_s for m in self.batch_metrics)
        out = {
            "source_id": self.source_id,
            "records": self.records,
            "batches": self.batches,
            "files": self.files,
            "bytes_written": self.bytes_written,
            "elapsed_s": round(self.elapsed_s, 3),
            "events_per_second": round(self.events_per_second, 1),
            "peak_rss_mb": round(self.peak_rss_mb, 1),
            "halted": self.halted,
            "skipped_committed_batches": self.skipped_batches,
            "skipped_committed_records": self.skipped_records,
            "accounting": dict(self.accounting),
            "out_of_order": self.out_of_order,
            "late": self.late,
            "blocked_batches": [asdict(b) for b in self.blocked],
            "sequence": {
                "basis": self.gap_basis,
                "proven_gaps": len(self.gaps),
                "missing_events": sum(g.missing for g in self.gaps),
                "open_gaps_unproven": self.open_gaps,
            },
        }
        if latencies:
            out["batch_latency_s"] = {
                "p50": round(latencies[len(latencies) // 2], 4),
                "max": round(latencies[-1], 4),
            }
        return out


def peak_rss_mb() -> float:
    """Peak RSS in MiB (ru_maxrss is bytes on macOS, KiB on Linux)."""
    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return raw / (1024 * 1024) if sys.platform == "darwin" else raw / 1024


def _write_completeness(layout: StorageLayout, summary: IngestSummary) -> None:
    report = {
        "source_id": summary.source_id,
        "basis": summary.gap_basis,
        "proven_gaps": len(summary.gaps),
        "missing_events": sum(g.missing for g in summary.gaps),
        "open_gaps_unproven": summary.open_gaps,
        "gaps": [asdict(g) for g in summary.gaps[:MAX_REPORTED_GAPS]],
        "gaps_truncated": max(0, len(summary.gaps) - MAX_REPORTED_GAPS),
    }
    path = layout.meta_dir / f"completeness-{summary.source_id}.json"
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(report, indent=2) + "\n")
    os.replace(tmp, path)


def run_ingest(
    source: JsonlSource,
    layout: StorageLayout,
    batch_config: BatchConfig,
    storage_config: StorageConfig,
    gate_config: GateConfig | None = None,
    *,
    manifest: Manifest | None = None,
) -> IngestSummary:
    gate = gate_config or GateConfig()
    layout.ensure()
    owns_manifest = manifest is None
    manifest = manifest or Manifest.open(layout)
    try:
        return _run(source, layout, batch_config, storage_config, gate, manifest)
    finally:
        if owns_manifest:
            manifest.close()


class Pipeline:
    """Everything that happens to one batch: register, gate, publish (or reject).

    Used by `run_ingest` for fresh batches and by recovery to replay an interrupted batch over its
    recorded source interval - the same code path, so a replay cannot behave differently.
    """

    def __init__(
        self,
        layout: StorageLayout,
        manifest: Manifest,
        storage_config: StorageConfig,
        gate: GateConfig,
        index: CommittedIndex,
    ) -> None:
        self.layout = layout
        self.manifest = manifest
        self.gate = gate
        self.index = index
        self.publisher = Publisher(layout, manifest, storage_config)

    def _block(
        self,
        summary: IngestSummary,
        batch: Batch,
        counts: dict[str, int],
        critical: list[str],
        evidence: str,
    ) -> bool:
        summary.blocked.append(
            BlockedBatch(
                batch.batch_id, batch.start_offset, batch.end_offset, counts, critical, evidence
            )
        )
        log.emit(
            "batch_blocked",
            30,
            batch_id=batch.batch_id,
            offsets=[batch.start_offset, batch.end_offset],
            reasons=counts,
            critical=critical,
            evidence=evidence,
        )
        if self.gate.on_block == "halt":
            summary.halted = True
            return True
        return False

    def process(self, batch: Batch, summary: IngestSummary) -> bool:
        """Run one batch to a durable outcome. Returns True if the run must halt."""
        manifest, layout, gate = self.manifest, self.layout, self.gate
        summary.records += len(batch)
        summary.batches += 1
        try:
            begin = manifest.begin_batch(batch)
        except SourceMutatedError as exc:
            return self._block(summary, batch, {"SOURCE_MUTATED": 1}, [str(exc)], "")
        if begin.outcome == "already_committed":
            summary.skipped_batches += 1
            summary.skipped_records += len(batch)
            return False

        t0 = time.perf_counter()
        manifest.transition(batch.batch_id, BatchState.RECEIVED, BatchState.VALIDATING)
        result = evaluate_batch(batch, self.index, gate)
        t1 = time.perf_counter()
        summary.out_of_order += result.out_of_order
        summary.late += result.late

        if result.decision == "BLOCK":
            evidence = persist_evidence(layout, batch, result, gate)
            manifest.reject(
                batch.batch_id,
                BatchState.VALIDATING,
                result.accounting,
                result.reason_counts,
                result.critical_failures,
                str(evidence),
            )
            _add_accounting(summary, result.accounting.with_committed(0))
            return self._block(
                summary, batch, result.reason_counts, result.critical_failures, str(evidence)
            )

        evidence_path = None
        if result.rejections or result.unresolved:  # quarantine mode: keep the evidence
            evidence_path = str(persist_evidence(layout, batch, result, gate))
        try:
            outcome = self.publisher.publish(batch, result, evidence_path)
        except (VerificationError, CommitConflictError) as exc:
            return self._block(
                summary, batch, {}, [f"{type(exc).__name__}: {exc}"], evidence_path or ""
            )
        self.index.add_events(
            result.accepted, result.accepted_fingerprints, result.accepted_ingestion_us
        )  # only after commit

        _add_accounting(summary, result.accounting.with_committed(outcome.committed_rows))
        m = BatchMetrics(
            batch.batch_id, batch.start_offset, batch.end_offset, len(batch), outcome.files,
            outcome.bytes, outcome.commit_seq, gate_s=t1 - t0, stage_s=outcome.stage_s,
            verify_s=outcome.verify_s, place_s=outcome.place_s, commit_s=outcome.commit_s,
        )  # fmt: skip
        summary.batch_metrics.append(m)
        summary.files += outcome.files
        summary.bytes_written += outcome.bytes
        log.emit(
            "batch_committed",
            batch_id=batch.batch_id,
            offsets=[batch.start_offset, batch.end_offset],
            reason=batch.reason,
            commit_seq=outcome.commit_seq,
            accounting=asdict(result.accounting.with_committed(outcome.committed_rows)),
            files=outcome.files,
            latency_s=round(m.total_s, 4),
        )
        return False


def _run(
    source: JsonlSource,
    layout: StorageLayout,
    batch_config: BatchConfig,
    storage_config: StorageConfig,
    gate: GateConfig,
    manifest: Manifest,
) -> IngestSummary:
    pipeline = Pipeline(layout, manifest, storage_config, gate, manifest.load_index())
    batcher = MicroBatcher(source.source_id, batch_config.batch_size, batch_config.max_age_seconds)
    summary = IngestSummary(source_id=source.source_id)
    started = time.perf_counter()

    for batch in batcher.batches(source):
        if pipeline.process(batch, summary):
            break

    if not summary.halted:
        final = source.final_sequences if source.complete else None
        summary.gaps = pipeline.index.sequences.finalize(
            int(gate.gap_grace_seconds * 1_000_000), final
        )
        summary.gap_basis = "end_of_stream" if final is not None else "watermark"
        seq = pipeline.index.sequences
        proven_inside = sum(g.missing for g in summary.gaps if g.last <= seq.max_seen(g.device_id))
        summary.open_gaps = seq.open_gap_count() - proven_inside
        _write_completeness(layout, summary)
        if summary.gaps:
            log.emit(
                "sequence_gaps_proven",
                30,
                basis=summary.gap_basis,
                gaps=len(summary.gaps),
                missing=sum(g.missing for g in summary.gaps),
            )
    summary.elapsed_s = time.perf_counter() - started
    summary.peak_rss_mb = peak_rss_mb()
    return summary


def _add_accounting(summary: IngestSummary, accounting) -> None:
    for key, value in asdict(accounting).items():
        summary.accounting[key] += value

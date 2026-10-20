"""Persist why a batch (or some of its records) did not become trusted data.

Evidence lives under ``rejected/<batch_id>/attempt-NNNN/`` - a storage area no reader queries.
Each attempt directory is write-once. Two artefacts:

``report.json``       decision, policy, accounting, per-reason counts, and the first
                      `MAX_EVIDENCE_ROWS` rejections (offset, code, detail, payload hash and a
                      bounded preview). Counts are always complete; the list may be truncated.
``quarantine.jsonl``  (quarantine mode only) every set-aside record, with its full payload, so
                      the rows are recoverable. In strict mode the whole batch is blocked and the
                      source log still holds every record at the recorded offsets, so no copy
                      is needed.
"""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from pathlib import Path

from telemetry_lakehouse.batcher import Batch
from telemetry_lakehouse.config import GateConfig
from telemetry_lakehouse.fsutil import write_new_file
from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.validation import GateResult, payload_digest

MAX_EVIDENCE_ROWS = 1000
PREVIEW_BYTES = 256


def next_attempt_dir(layout: StorageLayout, batch_id: str) -> Path:
    base = layout.rejected_dir / batch_id
    existing = sorted(p.name for p in base.glob("attempt-*")) if base.exists() else []
    n = int(existing[-1].split("-")[1]) + 1 if existing else 1
    return base / f"attempt-{n:04d}"


def _preview(payload: bytes) -> str:
    return payload[:PREVIEW_BYTES].decode("utf-8", errors="replace")


def persist_evidence(
    layout: StorageLayout, batch: Batch, result: GateResult, config: GateConfig
) -> Path:
    directory = next_attempt_dir(layout, batch.batch_id)
    rows = [
        {
            "offset": r.offset,
            "code": r.code.value,
            "detail": r.detail,
            "related_offsets": list(r.related),
            "payload_sha256": payload_digest(r.payload),
            "payload_preview": _preview(r.payload),
        }
        for r in result.rejections[:MAX_EVIDENCE_ROWS]
    ]
    held = [
        {"offset": off, "event_id": e.event_id, "payload_sha256": payload_digest(raw)}
        for off, e, raw in result.unresolved[:MAX_EVIDENCE_ROWS]
    ]
    report = {
        "batch_id": batch.batch_id,
        "source_id": batch.source_id,
        "start_offset": batch.start_offset,
        "end_offset": batch.end_offset,
        "decision": result.decision,
        "mode": result.mode,
        "policy": json.loads(config.model_dump_json()),
        "accounting": {
            "consumed": result.accounting.consumed,
            "accepted": result.accounting.accepted,
            "duplicates": result.accounting.duplicates,
            "rejected": result.accounting.rejected,
            "unresolved": result.accounting.unresolved,
            "committed": result.accounting.committed,
        },
        "duplicates": {
            "in_batch": result.duplicates_in_batch,
            "already_committed": result.duplicates_committed,
        },
        "sequence": {"out_of_order": result.out_of_order, "late": result.late},
        "critical_failures": result.critical_failures,
        "reason_counts": result.reason_counts,
        "rejections": rows,
        "rejections_truncated": max(0, len(result.rejections) - MAX_EVIDENCE_ROWS),
        "unresolved": held,
        "unresolved_truncated": max(0, len(result.unresolved) - MAX_EVIDENCE_ROWS),
        "recorded_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    # Write the bulky quarantine file first: a report's presence implies its companions exist.
    if result.mode == "quarantine" and result.decision == "ADMIT":
        lines = [
            json.dumps(
                {
                    "offset": r.offset,
                    "code": r.code.value,
                    "payload_b64": base64.b64encode(r.payload).decode(),
                }
            )
            for r in result.rejections
        ]
        lines += [
            json.dumps(
                {
                    "offset": off,
                    "code": "LATE_EVENT:HELD",
                    "payload_b64": base64.b64encode(raw).decode(),
                }
            )
            for off, _, raw in result.unresolved
        ]
        if lines:
            write_new_file(directory / "quarantine.jsonl", ("\n".join(lines) + "\n").encode())
    write_new_file(directory / "report.json", (json.dumps(report, indent=2) + "\n").encode())
    return directory

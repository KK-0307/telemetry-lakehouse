"""Bounded micro-batcher with deterministic batch identity.

A batch is the half-open source interval ``[start_offset, end_offset)`` of one source. Its
ID is a pure function of ``(source_id, start_offset, end_offset)``, so the same interval
always maps to the same batch - the key to idempotent retries.

Boundaries are deterministic under size-based flushing. Time-based flushing makes them
depend on wall-clock arrival; to retain replay determinism, the publication layer
(Milestone 3) persists each batch's boundaries *before* processing, and recovery replays the
recorded interval instead of re-batching.

Memory is bounded: at most ``max_records`` records are buffered at once.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Literal

FlushReason = Literal["size", "time", "end", "recovery"]


class OffsetContinuityError(ValueError):
    """Source offsets were not strictly consecutive, so a batch interval would lie."""


@dataclass(frozen=True, slots=True)
class SourceRecord:
    """One raw record and its position in the source."""

    offset: int
    payload: bytes


@dataclass(frozen=True, slots=True)
class Batch:
    batch_id: str
    source_id: str
    start_offset: int  # inclusive
    end_offset: int  # exclusive
    records: list[SourceRecord]
    reason: FlushReason

    def __len__(self) -> int:
        return len(self.records)


def compute_batch_id(source_id: str, start_offset: int, end_offset: int) -> str:
    digest = hashlib.blake2b(
        f"{source_id}|{start_offset}|{end_offset}".encode(), digest_size=8
    ).hexdigest()
    return f"b{digest}"


class MicroBatcher:
    def __init__(
        self,
        source_id: str,
        max_records: int,
        max_age_seconds: float | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_records < 1:
            raise ValueError("max_records must be >= 1")
        if max_age_seconds is not None and max_age_seconds <= 0:
            raise ValueError("max_age_seconds must be > 0")
        self._source_id = source_id
        self._max_records = max_records
        self._max_age = max_age_seconds
        self._clock = clock

    def batches(self, records: Iterable[SourceRecord | None]) -> Iterator[Batch]:
        """Group a record stream into batches.

        ``None`` items are idle ticks: they let a quiet source trigger a time-based flush.
        """
        buffer: list[SourceRecord] = []
        opened_at = 0.0
        for item in records:
            if item is not None:
                if buffer and item.offset != buffer[-1].offset + 1:
                    raise OffsetContinuityError(
                        f"expected offset {buffer[-1].offset + 1}, got {item.offset}"
                    )
                if not buffer:
                    opened_at = self._clock()
                buffer.append(item)
                if len(buffer) >= self._max_records:
                    yield self._flush(buffer, "size")
                    buffer = []
                    continue
            if buffer and self._max_age is not None and self._clock() - opened_at >= self._max_age:
                yield self._flush(buffer, "time")
                buffer = []
        if buffer:
            yield self._flush(buffer, "end")

    def _flush(self, buffer: list[SourceRecord], reason: FlushReason) -> Batch:
        start, end = buffer[0].offset, buffer[-1].offset + 1
        return Batch(
            batch_id=compute_batch_id(self._source_id, start, end),
            source_id=self._source_id,
            start_offset=start,
            end_offset=end,
            records=buffer,
            reason=reason,
        )

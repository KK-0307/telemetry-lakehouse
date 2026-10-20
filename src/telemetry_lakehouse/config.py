"""Validated configuration models. Invalid configuration fails fast with a clear error."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

Compression = Literal["zstd", "snappy", "gzip", "lz4", "none"]


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class SimulatorConfig(_Frozen):
    """Deterministic device-stream simulator. Same config => byte-identical stream."""

    devices: int = Field(1000, ge=1, le=1_000_000)
    events: int = Field(100_000, ge=0)
    seed: int = 42
    # Device IDs are `<prefix>-<7-digit index>`. device_id is one global namespace in the manifest
    # (unique (device_id, sequence_number)), so independent sources need distinct prefixes.
    device_prefix: str = Field("dev", pattern=r"^[A-Za-z0-9_.:]{1,24}$")
    start_time: AwareDatetime = datetime(2026, 1, 1, tzinfo=UTC)
    # Global event-time production rate (events per second of *simulated* time).
    rate_per_second: float = Field(1.0, gt=0, le=1_000_000)
    # "accelerated" ignores the wall clock; "realtime" paces emission to `rate_per_second`.
    mode: Literal["accelerated", "realtime"] = "accelerated"


class BatchConfig(_Frozen):
    """Micro-batch boundaries. `max_age_seconds` enables optional time-based flushing."""

    batch_size: int = Field(5000, ge=1, le=10_000_000)
    max_age_seconds: float | None = Field(None, gt=0)


class StorageConfig(_Frozen):
    compression: Compression = "zstd"
    row_group_size: int = Field(131_072, ge=1)


FAULT_KINDS: tuple[str, ...] = (
    "drop",
    "duplicate",
    "conflict",
    "reorder",
    "delay",
    "malformed",
    "invalid_value",
    "unsupported_version",
    "clock_skew_future",
    "clock_skew_past",
)


class FaultConfig(_Frozen):
    """Per-event fault probabilities. Each original event suffers at most one fault kind,
    so the rates must sum to <= 1. All faults are seeded and therefore reproducible."""

    drop: float = Field(0.0, ge=0, le=1)  # event never delivered -> missing sequence number
    duplicate: float = Field(0.0, ge=0, le=1)  # identical re-delivery (later ingestion_time)
    conflict: float = Field(0.0, ge=0, le=1)  # same event_id re-delivered with a changed payload
    reorder: float = Field(0.0, ge=0, le=1)  # arrives up to `reorder_window` events late
    delay: float = Field(0.0, ge=0, le=1)  # arrives `delay_positions` events late
    malformed: float = Field(0.0, ge=0, le=1)  # truncated / non-JSON / non-object payload
    invalid_value: float = Field(0.0, ge=0, le=1)  # measurement outside its valid range
    unsupported_version: float = Field(0.0, ge=0, le=1)  # schema_version the platform rejects
    clock_skew_future: float = Field(0.0, ge=0, le=1)  # device clock ahead of the platform's
    clock_skew_past: float = Field(0.0, ge=0, le=1)  # device clock behind (event looks very late)
    reorder_window: int = Field(50, ge=1)
    delay_positions: int = Field(5000, ge=1)
    # Simulated source interruption: the stream stops after this many delivered records and
    # the source is marked incomplete (no end-of-stream marker).
    interrupt_after: int | None = Field(None, ge=0)

    @model_validator(mode="after")
    def _rates_sum_to_at_most_one(self) -> FaultConfig:
        if sum(getattr(self, k) for k in FAULT_KINDS) > 1.0 + 1e-9:
            raise ValueError("fault rates must sum to <= 1")
        return self

    @property
    def active(self) -> bool:
        return self.interrupt_after is not None or any(getattr(self, k) > 0 for k in FAULT_KINDS)


class GateConfig(_Frozen):
    """Integrity gate / admission policy."""

    # strict: any rejected or unresolved record blocks the WHOLE batch.
    # quarantine: bad records are set aside (with evidence) and the rest of the batch publishes.
    mode: Literal["strict", "quarantine"] = "strict"
    # Events with ingestion_time - event_time above `max_lateness_seconds` are "late".
    # accept: publish and count; reject: row is rejected; hold: row is left unresolved.
    late_policy: Literal["accept", "reject", "hold"] = "accept"
    max_lateness_seconds: float = Field(3600.0, gt=0)
    # event_time further ahead of ingestion_time than this means the device clock is wrong.
    max_future_skew_seconds: float = Field(300.0, ge=0)
    # A sequence gap is proven missing by watermark once later events from the same device
    # arrived at least this long ago.
    gap_grace_seconds: float = Field(3600.0, ge=0)
    # What to do after a blocked batch: halt (default, nothing after it is published) or continue.
    on_block: Literal["halt", "continue"] = "halt"

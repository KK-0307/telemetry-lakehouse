"""On-disk storage areas.

<root>/
  source/     raw source logs (input to ingestion)
  staging/    candidate files for batches not yet published   (untrusted)
  data/       immutable Parquet files, Hive layout             (trusted only if referenced
                                                                by a committed manifest)
  rejected/   evidence for blocked/quarantined batches         (never queried as data)
  meta/       publication metadata (SQLite manifest, Milestone 3)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from telemetry_lakehouse.schemas import PARTITION_COLUMN


def partition_dir(event_date: date) -> str:
    return f"{PARTITION_COLUMN}={event_date.isoformat()}"


def parquet_relative_path(event_date: date, batch_id: str) -> str:
    """Batch-scoped, deterministic path of the file holding one batch's rows for one date."""
    return f"{partition_dir(event_date)}/{batch_id}.parquet"


@dataclass(frozen=True, slots=True)
class StorageLayout:
    root: Path

    @property
    def source_dir(self) -> Path:
        return self.root / "source"

    @property
    def staging_dir(self) -> Path:
        return self.root / "staging"

    @property
    def data_dir(self) -> Path:
        return self.root / "data"

    @property
    def rejected_dir(self) -> Path:
        return self.root / "rejected"

    @property
    def meta_dir(self) -> Path:
        return self.root / "meta"

    @property
    def default_source(self) -> Path:
        return self.source_dir / "events.jsonl"

    def ensure(self) -> None:
        for d in (
            self.source_dir,
            self.staging_dir,
            self.data_dir,
            self.rejected_dir,
            self.meta_dir,
        ):
            d.mkdir(parents=True, exist_ok=True)

    def staged_path(self, batch_id: str, event_date: date) -> Path:
        return self.staging_dir / batch_id / parquet_relative_path(event_date, batch_id)

    def data_path(self, relative_path: str) -> Path:
        return self.data_dir / relative_path

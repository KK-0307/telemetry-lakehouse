from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from telemetry_lakehouse import failpoints
from telemetry_lakehouse.layout import StorageLayout


@pytest.fixture
def layout(tmp_path: Path) -> StorageLayout:
    lay = StorageLayout(tmp_path / "lakehouse")
    lay.ensure()
    return lay


def valid_wire_event(**overrides: Any) -> dict[str, Any]:
    """A contract-conforming event in wire form (timestamps as strings)."""
    event: dict[str, Any] = {
        "event_id": "evt-0001",
        "device_id": "dev-0000001",
        "sequence_number": 1,
        "event_time": "2026-01-01T12:00:00+00:00",
        "ingestion_time": "2026-01-01T12:00:01+00:00",
        "schema_version": 1,
        "temperature_c": 21.5,
        "humidity_pct": 40.0,
        "battery_pct": 87.25,
        "signal_dbm": -60,
        "status": "OK",
    }
    event.update(overrides)
    return event


@pytest.fixture
def wire_event() -> Callable[..., dict[str, Any]]:
    return valid_wire_event


UTC_DT = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _disarm_failpoints():
    failpoints.disarm()
    yield
    failpoints.disarm()

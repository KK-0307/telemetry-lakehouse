"""DuckDB access to the published dataset.

All reads go through `visible_files`, the single seam that decides which Parquet files a trusted
reader may see: exactly the files referenced by COMMITTED batches in the manifest. Files that merely
exist under ``data/`` (placed-but-uncommitted, orphaned by a crash, or planted) are never read.
"""

from __future__ import annotations

from importlib import resources
from pathlib import Path

import duckdb

from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.manifest import Manifest

NAMED_QUERIES: dict[str, str] = {
    "hourly-device-health": "hourly_device_health.sql",
    "fleet-summary": "fleet_summary.sql",
    "battery-health": "battery_health.sql",
}

_EMPTY_EVENTS = """
    SELECT
        NULL::VARCHAR AS event_id, NULL::VARCHAR AS device_id, NULL::BIGINT AS sequence_number,
        NULL::TIMESTAMPTZ AS event_time, NULL::TIMESTAMPTZ AS ingestion_time,
        NULL::INTEGER AS schema_version, NULL::DOUBLE AS temperature_c,
        NULL::DOUBLE AS humidity_pct, NULL::DOUBLE AS battery_pct, NULL::INTEGER AS signal_dbm,
        NULL::VARCHAR AS status, NULL::DATE AS event_date
    WHERE false
"""


def visible_files(layout: StorageLayout) -> list[Path]:
    """Parquet files of committed batches, from one consistent manifest snapshot."""
    manifest = Manifest.open_readonly(layout)
    if manifest is None:
        return []
    try:
        return [layout.data_path(f.relative_path) for f in manifest.committed_files()]
    finally:
        manifest.close()


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect(layout: StorageLayout) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection exposing the dataset as view `events`."""
    con = duckdb.connect(":memory:")
    con.execute("SET TimeZone = 'UTC'")
    files = visible_files(layout)
    if files:
        listing = ", ".join(_sql_literal(str(f)) for f in files)
        con.execute(
            f"CREATE VIEW events AS SELECT * FROM read_parquet([{listing}], "
            "hive_partitioning = true, hive_types = {'event_date': DATE})"
        )
    else:
        con.execute(f"CREATE VIEW events AS {_EMPTY_EVENTS}")
    return con


def run_named_query(
    layout: StorageLayout, name: str, *, limit: int = 20
) -> tuple[list[str], list[tuple]]:
    if name not in NAMED_QUERIES:
        raise KeyError(f"unknown query {name!r}; available: {', '.join(sorted(NAMED_QUERIES))}")
    sql = (resources.files("telemetry_lakehouse") / "sql" / NAMED_QUERIES[name]).read_text()
    con = connect(layout)
    try:
        cursor = con.execute(sql, {"limit": limit})
        columns = [d[0] for d in cursor.description]
        return columns, cursor.fetchall()
    finally:
        con.close()

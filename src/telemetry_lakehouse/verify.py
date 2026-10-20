"""Independent verification of the published dataset with DuckDB SQL.

`verify` does **not** reuse the validation code or its reported counts. It reads only what a trusted
reader may read - the Parquet files referenced by committed manifest rows - plus the manifest tables
themselves, loads both into an in-memory DuckDB, and evaluates the assertions in
``sql/assertions/*.sql``. Each assertion is a query that returns *violating rows*; zero rows = PASS.

Statuses: PASS, FAIL, SKIP (the assertion's precondition is absent, e.g. no end-of-stream marker -
reported, never counted as a pass) and WARN (a *completeness* assertion failed but
``allow_incomplete`` downgrades it; the dataset is internally consistent but known to be incomplete).
Exit code of the CLI: 0 iff no FAIL.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.schemas import (
    BATTERY_PCT_RANGE,
    EVENT_COLUMNS,
    HUMIDITY_PCT_RANGE,
    SIGNAL_DBM_RANGE,
    SUPPORTED_SCHEMA_VERSIONS,
    TEMPERATURE_C_RANGE,
    DeviceStatus,
)

COMPLETENESS = "completeness"
EXAMPLE_ROWS = 5

# Expected DuckDB logical types of the contract (DESCRIBE output), independent of the Arrow schema.
EXPECTED_TYPES: dict[str, str] = {
    "event_id": "VARCHAR",
    "device_id": "VARCHAR",
    "sequence_number": "BIGINT",
    "event_time": "TIMESTAMP WITH TIME ZONE",
    "ingestion_time": "TIMESTAMP WITH TIME ZONE",
    "schema_version": "INTEGER",
    "temperature_c": "DOUBLE",
    "humidity_pct": "DOUBLE",
    "battery_pct": "DOUBLE",
    "signal_dbm": "INTEGER",
    "status": "VARCHAR",
}

# Expected Parquet physical types of the file columns (what a per-file reader sees).
EXPECTED_PHYSICAL: dict[str, str] = {
    "event_id": "BYTE_ARRAY",
    "device_id": "BYTE_ARRAY",
    "sequence_number": "INT64",
    "event_time": "INT64",
    "ingestion_time": "INT64",
    "schema_version": "INT32",
    "temperature_c": "DOUBLE",
    "humidity_pct": "DOUBLE",
    "battery_pct": "DOUBLE",
    "signal_dbm": "INT32",
    "status": "BYTE_ARRAY",
}


@dataclass(frozen=True, slots=True)
class VerifyConfig:
    grace_seconds: float = 3600.0
    future_skew_seconds: float = 300.0
    allow_incomplete: bool = False
    sidecars: tuple[Path, ...] = ()  # extra source sidecars carrying end-of-stream markers
    only: frozenset[str] | None = None


@dataclass(frozen=True, slots=True)
class AssertionSpec:
    name: str
    category: str
    description: str
    requires: str | None
    sql: str


@dataclass(slots=True)
class AssertionResult:
    name: str
    category: str
    description: str
    status: str  # PASS | FAIL | WARN | SKIP
    violations: int = 0
    examples: list[dict[str, Any]] = field(default_factory=list)
    note: str = ""
    seconds: float = 0.0


@dataclass(slots=True)
class VerifyReport:
    generated_at: str
    profile: dict[str, Any]
    results: list[AssertionResult]
    allow_incomplete: bool

    @property
    def failed(self) -> list[AssertionResult]:
        return [r for r in self.results if r.status == "FAIL"]

    @property
    def ok(self) -> bool:
        return not self.failed

    def to_dict(self) -> dict:
        counts: dict[str, int] = {}
        for r in self.results:
            counts[r.status] = counts.get(r.status, 0) + 1
        return {
            "result": "PASS" if self.ok else "FAIL",
            "generated_at": self.generated_at,
            "counts": counts,
            "profile": self.profile,
            "assertions": [
                {
                    "name": r.name,
                    "category": r.category,
                    "status": r.status,
                    "violations": r.violations,
                    "description": r.description,
                    "note": r.note,
                    "examples": r.examples,
                    "seconds": round(r.seconds, 4),
                }
                for r in self.results
            ],
        }

    def render(self) -> str:
        p = self.profile
        lines = [
            "Telemetry Lakehouse integrity report",
            f"dataset : {p['events']:,} events | {p['devices']:,} devices | {p['files']:,} files | "
            f"{p['batches_committed']:,} committed batches | dates {p['first_date']} .. {p['last_date']}",
            f"manifest: last commit_seq {p['last_commit_seq']} | batch states {p['batch_states']}",
            "",
        ]
        width = max(len(r.name) for r in self.results) if self.results else 10
        for r in self.results:
            tail = f"{r.violations} violation(s)" if r.status in ("FAIL", "WARN") else r.note
            if r.status == "PASS":
                tail = "0 violations"
            lines.append(f"{r.status:<4}  {r.category:<14} {r.name:<{width}}  {tail}")
            for ex in r.examples[:3] if r.status in ("FAIL", "WARN") else []:
                lines.append(f"        e.g. {json.dumps(ex, default=str)}")
        counts = self.to_dict()["counts"]
        summary = ", ".join(f"{n} {k.lower()}" for k, n in sorted(counts.items()))
        lines += [
            "",
            f"RESULT: {'PASS' if self.ok else 'FAIL'} - {summary} ({len(self.results)} assertions)",
        ]
        if self.allow_incomplete and counts.get("WARN"):
            lines.append(
                "note: completeness failures were downgraded to WARN by --allow-incomplete"
            )
        return "\n".join(lines)


# ---------------------------------------------------------------------------------------- loading
def load_assertions() -> list[AssertionSpec]:
    specs = []
    folder = resources.files("telemetry_lakehouse") / "sql" / "assertions"
    for entry in sorted(folder.iterdir(), key=lambda e: e.name):
        if not entry.name.endswith(".sql"):
            continue
        text = entry.read_text()
        header = dict(re.findall(r"^-- (\w+): (.*)$", text, flags=re.M))
        body = "\n".join(line for line in text.splitlines() if not line.startswith("-- "))
        specs.append(
            AssertionSpec(
                header["name"],
                header["category"],
                header["description"],
                header.get("requires"),
                body.strip(),
            )
        )
    return specs


def _arrow(columns: list[tuple[str, pa.DataType]], rows: list[tuple]) -> pa.Table:
    data = list(zip(*rows, strict=True)) if rows else [()] * len(columns)
    return pa.table(
        {
            name: pa.array(list(col), type=typ)
            for (name, typ), col in zip(columns, data, strict=True)
        }
    )


def _read_manifest(layout: StorageLayout) -> dict[str, pa.Table]:
    s, i = pa.string(), pa.int64()
    schemas = {
        "m_batches": [("batch_id", s), ("source_id", s), ("start_offset", i), ("end_offset", i),
                      ("state", s), ("consumed", i), ("accepted", i), ("duplicates", i),
                      ("rejected", i), ("unresolved", i), ("committed_rows", i), ("commit_seq", i)],
        "m_files": [("batch_id", s), ("relative_path", s), ("rows", i), ("size_bytes", i),
                    ("sha256", s), ("min_event_time_us", i), ("max_event_time_us", i)],
        "m_index": [("event_id", s), ("device_id", s), ("sequence_number", i), ("batch_id", s)],
    }  # fmt: skip
    queries = {
        "m_batches": "SELECT batch_id, source_id, start_offset, end_offset, state, consumed, accepted, "
        "duplicates, rejected, unresolved, committed_rows, commit_seq FROM batches",
        "m_files": "SELECT batch_id, relative_path, rows, size_bytes, sha256, min_event_time_us, "
        "max_event_time_us FROM batch_files",
        "m_index": "SELECT event_id, device_id, sequence_number, batch_id FROM event_index",
    }
    path = layout.meta_dir / "manifest.sqlite"
    if not path.exists():
        return {k: _arrow(v, []) for k, v in schemas.items()}
    con = sqlite3.connect(path, timeout=10.0)
    try:
        con.execute("PRAGMA query_only = ON")
        return {k: _arrow(schemas[k], con.execute(q).fetchall()) for k, q in queries.items()}
    finally:
        con.close()


def _markers(layout: StorageLayout, extra: tuple[Path, ...], source_ids: set[str]) -> list[tuple]:
    """(source_id, device_id, final_seq) rows from end-of-stream markers of the manifest's sources."""
    out: list[tuple] = []
    seen: set[str] = set()
    candidates = sorted(layout.source_dir.glob("*.meta.json")) if layout.source_dir.exists() else []
    for sidecar in [*candidates, *extra]:
        try:
            meta = json.loads(sidecar.read_text())
        except (OSError, ValueError):
            continue
        sid = meta.get("source_id")
        if (
            sid in source_ids
            and sid not in seen
            and meta.get("complete")
            and meta.get("final_sequences")
        ):
            seen.add(sid)
            out += [(sid, d, n) for d, n in meta["final_sequences"].items()]
    return out


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _insert_many(con: duckdb.DuckDBPyConnection, sql: str, rows: list[tuple]) -> None:
    if rows:  # DuckDB's executemany rejects an empty parameter list
        con.executemany(sql, rows)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


EMPTY_EVENTS = (
    "SELECT NULL::VARCHAR AS event_id, NULL::VARCHAR AS device_id, NULL::BIGINT AS sequence_number, "
    "NULL::TIMESTAMPTZ AS event_time, NULL::TIMESTAMPTZ AS ingestion_time, NULL::INTEGER AS schema_version, "
    "NULL::DOUBLE AS temperature_c, NULL::DOUBLE AS humidity_pct, NULL::DOUBLE AS battery_pct, "
    "NULL::INTEGER AS signal_dbm, NULL::VARCHAR AS status, NULL::DATE AS event_date, "
    "NULL::VARCHAR AS filename, NULL::VARCHAR AS batch_id WHERE false"
)


def _prepare(layout: StorageLayout, config: VerifyConfig) -> tuple[duckdb.DuckDBPyConnection, dict]:
    con = duckdb.connect(":memory:")
    con.execute("SET TimeZone = 'UTC'")
    for name, table in _read_manifest(layout).items():
        con.register(f"_{name}", table)
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM _{name}")
        con.unregister(f"_{name}")
    con.execute(
        "CREATE TABLE m_committed_files AS SELECT f.* FROM m_files f "
        "JOIN m_batches b USING (batch_id) WHERE b.state = 'COMMITTED'"
    )

    # Physical state of every committed file, checked from the file system and DuckDB (not the writer).
    checks: list[tuple] = []
    readable_paths: list[str] = []
    file_schemas: list[tuple] = []
    for rel, size, sha in con.execute(
        "SELECT relative_path, size_bytes, sha256 FROM m_committed_files ORDER BY relative_path"
    ).fetchall():
        path = layout.data_path(rel)
        present = path.is_file()
        readable = size_ok = sha_ok = False
        if present:
            size_ok = path.stat().st_size == size
            sha_ok = _sha256(path) == sha
            try:
                rows = con.execute(
                    "SELECT name, repetition_type, num_children, type FROM parquet_schema(?)",
                    [str(path)],
                ).fetchall()
                con.execute("SELECT num_rows FROM parquet_file_metadata(?)", [str(path)]).fetchone()
                readable = True
                readable_paths.append(str(path))
                file_schemas += [(str(path), n, r, c, t) for n, r, c, t in rows]
            except duckdb.Error:
                readable = False
        checks.append((rel, str(path), present, readable, size_ok, sha_ok))
    con.execute(
        "CREATE TABLE file_checks (relative_path VARCHAR, abs_path VARCHAR, present BOOLEAN, "
        "readable BOOLEAN, size_matches BOOLEAN, sha_matches BOOLEAN)"
    )
    _insert_many(con, "INSERT INTO file_checks VALUES (?, ?, ?, ?, ?, ?)", checks)
    con.execute(
        "CREATE TABLE file_schemas (filename VARCHAR, name VARCHAR, repetition_type VARCHAR, "
        "num_children BIGINT, physical_type VARCHAR)"
    )
    _insert_many(con, "INSERT INTO file_schemas VALUES (?, ?, ?, ?, ?)", file_schemas)

    on_disk = (
        sorted(
            p.relative_to(layout.data_dir).as_posix()
            for p in layout.data_dir.rglob("*")
            if p.is_file()
        )
        if layout.data_dir.exists()
        else []
    )
    con.execute("CREATE TABLE data_dir_files (relative_path VARCHAR)")
    _insert_many(con, "INSERT INTO data_dir_files VALUES (?)", [(p,) for p in on_disk])

    errors: list[str] = []
    if readable_paths:
        listing = ", ".join(_sql_literal(p) for p in readable_paths)
        try:
            con.execute(
                "CREATE VIEW events AS SELECT *, "
                "regexp_extract(filename, '([^/]+)[.]parquet$', 1) AS batch_id "
                f"FROM read_parquet([{listing}], hive_partitioning = true, filename = true, "
                "union_by_name = true, hive_types = {'event_date': DATE})"
            )
        except duckdb.Error as exc:
            errors.append(f"published files cannot be read as one dataset: {exc}")
            con.execute(f"CREATE VIEW events AS {EMPTY_EVENTS}")
    else:
        con.execute(f"CREATE VIEW events AS {EMPTY_EVENTS}")

    con.execute("CREATE TABLE expected_columns (column_name VARCHAR, physical_type VARCHAR)")
    _insert_many(con, "INSERT INTO expected_columns VALUES (?, ?)", list(EXPECTED_PHYSICAL.items()))
    con.execute("CREATE TABLE expected_types (column_name VARCHAR, column_type VARCHAR)")
    _insert_many(
        con,
        "INSERT INTO expected_types VALUES (?, ?)",
        [*EXPECTED_TYPES.items(), ("event_date", "DATE")],
    )
    cols = ", ".join([*EVENT_COLUMNS, "event_date"])
    con.execute(
        "CREATE TABLE events_types AS SELECT column_name, column_type "
        f"FROM (DESCRIBE SELECT {cols} FROM events)"
    )

    source_ids = {r[0] for r in con.execute("SELECT DISTINCT source_id FROM m_batches").fetchall()}
    markers = _markers(layout, config.sidecars, source_ids)
    con.execute(
        "CREATE TABLE final_sequences (source_id VARCHAR, device_id VARCHAR, final_seq BIGINT)"
    )
    _insert_many(con, "INSERT INTO final_sequences VALUES (?, ?, ?)", markers)

    params = {
        "temp_min": TEMPERATURE_C_RANGE[0], "temp_max": TEMPERATURE_C_RANGE[1],
        "humidity_min": HUMIDITY_PCT_RANGE[0], "humidity_max": HUMIDITY_PCT_RANGE[1],
        "battery_min": BATTERY_PCT_RANGE[0], "battery_max": BATTERY_PCT_RANGE[1],
        "signal_min": SIGNAL_DBM_RANGE[0], "signal_max": SIGNAL_DBM_RANGE[1],
        "statuses": [s.value for s in DeviceStatus],
        "versions": sorted(SUPPORTED_SCHEMA_VERSIONS),
        "future_skew_s": config.future_skew_seconds,
        "grace_s": config.grace_seconds,
    }  # fmt: skip
    return con, {"params": params, "markers": bool(markers), "errors": errors}


def _profile(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    events, devices, first, last = con.execute(
        "SELECT count(*), count(DISTINCT device_id), min(event_date), max(event_date) FROM events"
    ).fetchone()
    files = con.execute("SELECT count(*) FROM m_committed_files").fetchone()[0]
    states = dict(
        con.execute(
            "SELECT state, count(*) FROM m_batches GROUP BY state ORDER BY state"
        ).fetchall()
    )
    seq = con.execute("SELECT coalesce(max(commit_seq), 0) FROM m_batches").fetchone()[0]
    return {
        "events": events,
        "devices": devices,
        "files": files,
        "first_date": str(first) if first else "-",
        "last_date": str(last) if last else "-",
        "batches_committed": states.get("COMMITTED", 0),
        "batch_states": states,
        "last_commit_seq": seq,
    }


def _run_one(
    con: duckdb.DuckDBPyConnection, spec: AssertionSpec, ctx: dict, cfg: VerifyConfig
) -> AssertionResult:
    base = AssertionResult(spec.name, spec.category, spec.description, "PASS")
    if spec.requires == "final_sequences" and not ctx["markers"]:
        base.status, base.note = "SKIP", "no end-of-stream marker found for any source"
        return base
    wanted = set(re.findall(r"\$(\w+)", spec.sql))
    params = {k: ctx["params"][k] for k in wanted}
    t0 = time.perf_counter()
    try:
        (count,) = con.execute(f"SELECT count(*) FROM ({spec.sql}) AS v", params).fetchone()
        base.violations = count
        if count:
            cur = con.execute(f"SELECT * FROM ({spec.sql}) AS v LIMIT {EXAMPLE_ROWS}", params)
            names = [d[0] for d in cur.description]
            base.examples = [dict(zip(names, row, strict=True)) for row in cur.fetchall()]
            base.status = "FAIL"
            if spec.category == COMPLETENESS and cfg.allow_incomplete:
                base.status = "WARN"
    except duckdb.Error as exc:  # an assertion that cannot run is a failure, never a pass
        base.status, base.note = "FAIL", f"assertion could not be evaluated: {exc}"
        base.violations = 1
    base.seconds = time.perf_counter() - t0
    return base


def verify(layout: StorageLayout, config: VerifyConfig | None = None) -> VerifyReport:
    cfg = config or VerifyConfig()
    con, ctx = _prepare(layout, cfg)
    try:
        results: list[AssertionResult] = []
        for message in ctx["errors"]:
            results.append(
                AssertionResult("dataset_readable", "contract", "Published files form one readable dataset.",
                                "FAIL", 1, [], message)
            )  # fmt: skip
        for spec in load_assertions():
            if cfg.only is None or spec.name in cfg.only:
                results.append(_run_one(con, spec, ctx, cfg))
        return VerifyReport(
            generated_at=datetime.now(UTC).isoformat(timespec="seconds"),
            profile=_profile(con),
            results=results,
            allow_incomplete=cfg.allow_incomplete,
        )
    finally:
        con.close()


def write_report(layout: StorageLayout, report: VerifyReport) -> Path:
    layout.meta_dir.mkdir(parents=True, exist_ok=True)
    path = layout.meta_dir / "verify-report.json"
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(report.to_dict(), indent=2, default=str) + "\n")
    os.replace(tmp, path)
    return path

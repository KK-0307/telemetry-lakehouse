"""Reproducible benchmark harness.

Methodology (also written up in docs/benchmarks.md):

* The source log is generated once per scale from a fixed seed (generation time is reported on its own).
* Every ingest measurement runs in a **fresh subprocess** on a **fresh lakehouse directory**, so peak
  RSS is per run and nothing is warm from a previous run. Wall time is measured inside the worker around
  `run_ingest` only (interpreter start-up and imports excluded).
* Each configuration is repeated `repeats` times; reported are the median with min/max.
* Stage timings come from the pipeline's own per-batch metrics; `stage` = build Arrow table, sort, encode
  Parquet, fsync, hash, and record the files in the manifest.
* Nothing here is estimated: a figure that was not measured is absent, not guessed.
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pydantic

from telemetry_lakehouse.config import (
    BatchConfig,
    GateConfig,
    SimulatorConfig,
    StorageConfig,
)
from telemetry_lakehouse.layout import StorageLayout

QUERIES = ("count", "fleet-summary", "hourly-device-health", "battery-health", "point-lookup")


# ------------------------------------------------------------------------------------ helpers
def _sysctl(name: str) -> str | None:
    try:
        return subprocess.run(
            ["sysctl", "-n", name], capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def hardware_info() -> dict[str, Any]:
    cpu = _sysctl("machdep.cpu.brand_string")
    mem = _sysctl("hw.memsize")
    if cpu is None and Path("/proc/cpuinfo").exists():
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    if mem is None and Path("/proc/meminfo").exists():
        kb = int(Path("/proc/meminfo").read_text().split()[1])
        mem = str(kb * 1024)
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu": cpu or platform.processor() or "unknown",
        "logical_cores": os.cpu_count(),
        "ram_gib": round(int(mem) / 2**30, 1) if mem else None,
        "python": sys.version.split()[0],
        "pyarrow": pa.__version__,
        "duckdb": duckdb.__version__,
        "pydantic": pydantic.__version__,
        "sqlite": sqlite3.sqlite_version,
    }


def _stats(values: list[float]) -> dict[str, float]:
    return {
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
        "n": len(values),
    }


def _pct(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))] if ordered else 0.0


def dir_size(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


# ------------------------------------------------------------------------------------ worker
def _worker_ingest(spec: dict) -> dict:
    """Runs inside a fresh subprocess; prints one JSON document."""
    from telemetry_lakehouse import failpoints, log
    from telemetry_lakehouse.ingest import JsonlSource, peak_rss_mb, run_ingest

    log.configure("ERROR")
    failpoints.load_from_env()
    layout = StorageLayout(Path(spec["root"]))
    source = JsonlSource.open(Path(spec["source"]))
    summary = run_ingest(
        source,
        layout,
        BatchConfig(batch_size=spec["batch_size"]),
        StorageConfig(compression=spec["compression"]),
        GateConfig(),
    )
    m = summary.batch_metrics
    gate = [b.gate_s for b in m]
    total = [b.total_s for b in m]

    def tot(attr: str) -> float:
        return sum(getattr(b, attr) for b in m)

    stage_s, bytes_written = tot("stage_s"), summary.bytes_written
    return {
        "records": summary.records,
        "batches": summary.batches,
        "files": summary.files,
        "elapsed_s": summary.elapsed_s,
        "events_per_second": summary.events_per_second,
        "peak_rss_mb": peak_rss_mb(),
        "bytes_written": bytes_written,
        "stage_totals_s": {
            "gate": tot("gate_s"),
            "stage": stage_s,
            "verify": tot("verify_s"),
            "place": tot("place_s"),
            "commit": tot("commit_s"),
        },
        "gate_latency_ms": {
            "p50": _pct(gate, 0.5) * 1e3,
            "p95": _pct(gate, 0.95) * 1e3,
            "max": max(gate, default=0) * 1e3,
        },
        "publish_latency_ms": {
            "p50": _pct(total, 0.5) * 1e3,
            "p95": _pct(total, 0.95) * 1e3,
            "max": max(total, default=0) * 1e3,
        },
        "parquet_write_mb_per_s": (bytes_written / 1e6) / stage_s if stage_s else 0.0,
        "parquet_write_rows_per_s": summary.records / stage_s if stage_s else 0.0,
    }


def _run_worker(
    spec: dict, env_extra: dict[str, str] | None = None
) -> tuple[int, dict | None, str]:
    src_root = str(Path(__file__).resolve().parents[1])  # make workers independent of .pth files
    env = {**os.environ, **(env_extra or {})}
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [src_root, env.get("PYTHONPATH")]))
    proc = subprocess.run(
        [sys.executable, "-m", "telemetry_lakehouse.benchmark", "worker", json.dumps(spec)],
        capture_output=True,
        text=True,
        env=env,
    )
    result = None
    if proc.returncode == 0 and proc.stdout.strip():
        result = json.loads(proc.stdout.strip().splitlines()[-1])
    return proc.returncode, result, proc.stderr[-500:]


# ------------------------------------------------------------------------------------ measurements
def generate_source(events: int, devices: int, seed: int, path: Path) -> dict:
    from telemetry_lakehouse.simulator import write_source

    t0 = time.perf_counter()
    meta = write_source(SimulatorConfig(devices=devices, events=events, seed=seed), path)
    seconds = time.perf_counter() - t0
    return {
        "events": events,
        "seconds": seconds,
        "events_per_second": events / seconds if seconds else 0.0,
        "source_bytes": path.stat().st_size,
        "source_id": meta["source_id"],
    }


def bench_ingest(
    source: Path, workdir: Path, batch_size: int, compression: str, repeats: int
) -> dict:
    runs = []
    for i in range(repeats):
        root = workdir / f"ingest-{batch_size}-{compression}-{i}"
        shutil.rmtree(root, ignore_errors=True)
        code, result, err = _run_worker(
            {
                "root": str(root),
                "source": str(source),
                "batch_size": batch_size,
                "compression": compression,
            }
        )
        if result is None:
            raise RuntimeError(f"worker failed ({code}): {err}")
        result["storage_bytes"] = dir_size(StorageLayout(root).data_dir)
        runs.append(result)
        if i < repeats - 1:
            shutil.rmtree(root, ignore_errors=True)
        else:
            result["_root"] = str(root)
    keys = [
        "elapsed_s",
        "events_per_second",
        "peak_rss_mb",
        "parquet_write_mb_per_s",
        "storage_bytes",
    ]
    agg = {k: _stats([r[k] for r in runs]) for k in keys}
    last = runs[-1]
    return {
        "batch_size": batch_size,
        "compression": compression,
        "repeats": repeats,
        "records": last["records"],
        "batches": last["batches"],
        "files": last["files"],
        "summary": agg,
        "gate_latency_ms_p50": _stats([r["gate_latency_ms"]["p50"] for r in runs]),
        "gate_latency_ms_p95": _stats([r["gate_latency_ms"]["p95"] for r in runs]),
        "publish_latency_ms_p50": _stats([r["publish_latency_ms"]["p50"] for r in runs]),
        "publish_latency_ms_p95": _stats([r["publish_latency_ms"]["p95"] for r in runs]),
        "stage_share": {
            k: statistics.median(r["stage_totals_s"][k] / r["elapsed_s"] for r in runs)
            for k in ("gate", "stage", "verify", "place", "commit")
        },
        "parquet_write_rows_per_s": _stats([r["parquet_write_rows_per_s"] for r in runs]),
        "runs": [{k: v for k, v in r.items() if not k.startswith("_")} for r in runs],
        "_root": last.get("_root"),
    }


def bench_queries(root: Path, repeats: int = 5) -> dict:
    """DuckDB latency over the committed dataset: connection+view setup, then each query."""
    from telemetry_lakehouse.reader import connect, run_named_query

    layout = StorageLayout(root)
    setup = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        connect(layout).close()
        setup.append(time.perf_counter() - t0)

    def timed(fn) -> dict:
        samples = []
        for _ in range(repeats):
            t0 = time.perf_counter()
            fn()
            samples.append(time.perf_counter() - t0)
        return {**_stats(samples), "first": samples[0]}

    out: dict[str, Any] = {"connect_and_view_setup_s": _stats(setup)}

    def count() -> None:
        con = connect(layout)
        con.execute("SELECT count(*) FROM events").fetchone()
        con.close()

    out["count"] = timed(count)
    for name in ("fleet-summary", "hourly-device-health", "battery-health"):
        out[name] = timed(lambda n=name: run_named_query(layout, n, limit=20))

    con = connect(layout)
    (probe,) = con.execute("SELECT event_id FROM events USING SAMPLE 1 ROWS").fetchone()
    con.close()

    def lookup() -> None:
        c = connect(layout)
        c.execute(
            "SELECT event_id, device_id, sequence_number, temperature_c, status "
            "FROM events WHERE event_id = ?",
            [probe],
        ).fetchall()  # (TIMESTAMPTZ columns would need pytz to materialise in Python)
        c.close()

    out["point-lookup"] = timed(lookup)
    return out


def bench_recovery(source: Path, workdir: Path, batch_size: int, total_batches: int) -> dict:
    """Crash near the end of a run, then time `recover` for the resume path and the replay path."""
    from telemetry_lakehouse.ingest import JsonlSource
    from telemetry_lakehouse.manifest import Manifest
    from telemetry_lakehouse.recovery import recover

    out: dict[str, Any] = {"batch_size": batch_size, "batches_before_crash": total_batches - 1}
    for label, point in (("resume", "after_place"), ("replay", "before_stage")):
        root = workdir / f"recovery-{label}"
        shutil.rmtree(root, ignore_errors=True)
        code, _, _ = _run_worker(
            {
                "root": str(root),
                "source": str(source),
                "batch_size": batch_size,
                "compression": "zstd",
            },
            {"TELEMETRY_FAILPOINT": f"{point}:{total_batches}"},
        )
        if code != 137:
            raise RuntimeError(f"expected a simulated crash (137), got {code}")
        layout = StorageLayout(root)
        t0 = time.perf_counter()
        with Manifest.open(layout) as m:
            report = recover(layout, m, source=JsonlSource.open(source), verify_files=True)
        out[label] = {
            "seconds": time.perf_counter() - t0,
            "ok": report.ok,
            "actions": [a["action"] for a in report.actions],
        }
        shutil.rmtree(root, ignore_errors=True)
    return out


def bench_replay_and_verify(root: Path, source: Path) -> dict:
    from telemetry_lakehouse.ingest import JsonlSource
    from telemetry_lakehouse.manifest import Manifest
    from telemetry_lakehouse.recovery import check_committed_files, replay_committed
    from telemetry_lakehouse.verify import verify

    layout = StorageLayout(root)
    out: dict[str, Any] = {}
    t0 = time.perf_counter()
    with Manifest.open(layout) as m:
        results = replay_committed(layout, m, JsonlSource.open(source), StorageConfig())
    seconds = time.perf_counter() - t0
    rows = sum(r.rows for r in results)
    out["replay"] = {
        "seconds": seconds,
        "batches": len(results),
        "rows": rows,
        "rows_per_second": rows / seconds if seconds else 0.0,
        "all_identical": all(r.ok for r in results),
    }
    t0 = time.perf_counter()
    with Manifest.open(layout) as m:
        problems = check_committed_files(layout, m, deep=True)
    out["deep_file_check_s"] = time.perf_counter() - t0
    out["deep_file_check_problems"] = len(problems)
    t0 = time.perf_counter()
    report = verify(layout)
    out["verify"] = {
        "seconds": time.perf_counter() - t0,
        "result": "PASS" if report.ok else "FAIL",
        "assertions": len(report.results),
    }
    return out


# ------------------------------------------------------------------------------------ orchestration
@dataclass(slots=True)
class BenchConfig:
    events: int = 1_000_000
    devices: int = 1000
    seed: int = 42
    repeats: int = 3
    batch_sizes: tuple[int, ...] = ()
    compressions: tuple[str, ...] = ()
    sweep_events: int | None = None
    scales: tuple[int, ...] = ()
    recovery: bool = True
    label: str = "run"
    workdir: Path | None = None
    keep: bool = False


def run_suite(cfg: BenchConfig, progress=print) -> dict:
    work = cfg.workdir or Path(tempfile.mkdtemp(prefix="telemetry-bench-"))
    work.mkdir(parents=True, exist_ok=True)
    result: dict[str, Any] = {
        "label": cfg.label,
        "started_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "hardware": hardware_info(),
        "config": {
            "devices": cfg.devices,
            "seed": cfg.seed,
            "repeats": cfg.repeats,
            "simulator_rate_per_second": SimulatorConfig().rate_per_second,
            "default_batch_size": BatchConfig().batch_size,
            "default_compression": StorageConfig().compression,
        },
    }
    sources: dict[int, Path] = {}

    def source_for(events: int) -> Path:
        if events not in sources:
            path = work / f"source-{events}.jsonl"
            progress(f"[gen] {events:,} events ...")
            result.setdefault("generation", {})[str(events)] = generate_source(
                events, cfg.devices, cfg.seed, path
            )
            sources[events] = path
        return sources[events]

    try:
        scales = sorted({*cfg.scales, cfg.events})
        result["scales"] = {}
        for n in scales:
            progress(f"[scale] {n:,} events, batch 5000, zstd, {cfg.repeats} repeats")
            case = bench_ingest(source_for(n), work, BatchConfig().batch_size, "zstd", cfg.repeats)
            root = Path(case.pop("_root"))
            if n == cfg.events:
                progress("[queries / replay / verify]")
                case["queries"] = bench_queries(root)
                case["replay_verify"] = bench_replay_and_verify(root, source_for(n))
            result["scales"][str(n)] = case
            shutil.rmtree(
                work / f"ingest-{BatchConfig().batch_size}-zstd-{cfg.repeats - 1}",
                ignore_errors=True,
            )

        sweep_n = cfg.sweep_events or cfg.events
        if cfg.batch_sizes:
            result["batch_size_sweep"] = {"events": sweep_n, "cases": []}
            for bs in cfg.batch_sizes:
                progress(f"[batch-size] {bs:,} @ {sweep_n:,} events")
                case = bench_ingest(source_for(sweep_n), work, bs, "zstd", cfg.repeats)
                case.pop("_root", None)
                result["batch_size_sweep"]["cases"].append(case)
        if cfg.compressions:
            result["compression_sweep"] = {"events": sweep_n, "cases": []}
            for codec in cfg.compressions:
                progress(f"[compression] {codec} @ {sweep_n:,} events")
                case = bench_ingest(
                    source_for(sweep_n), work, BatchConfig().batch_size, codec, cfg.repeats
                )
                case.pop("_root", None)
                result["compression_sweep"]["cases"].append(case)
        if cfg.recovery:
            n = sweep_n if sweep_n <= cfg.events else cfg.events
            batch = BatchConfig().batch_size
            progress(f"[recovery] crash at last batch of {n:,} events")
            result["recovery"] = bench_recovery(source_for(n), work, batch, -(-n // batch))
    finally:
        if not cfg.keep:
            shutil.rmtree(work, ignore_errors=True)
    result["finished_at"] = datetime.now(UTC).isoformat(timespec="seconds")
    return result


# ------------------------------------------------------------------------------------ rendering
def _fmt_s(x: dict) -> str:
    return f"{x['median']:.2f} ({x['min']:.2f}-{x['max']:.2f})"


def render_markdown(res: dict) -> str:
    hw = res["hardware"]
    lines = [
        f"_Run `{res['label']}` - {res['started_at']} - {hw['cpu']}, {hw['logical_cores']} logical cores, "
        f"{hw['ram_gib']} GiB, Python {hw['python']}, pyarrow {hw['pyarrow']}, duckdb {hw['duckdb']}, "
        f"SQLite {hw['sqlite']}_",
        "",
        "### Scale (batch 5,000, ZSTD)",
        "",
        "| Events | Wall s | Events/s | Peak RSS MB | Parquet MB | Gate p50 / p95 ms | Publish p50 / p95 ms |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for n, c in res["scales"].items():
        s = c["summary"]
        lines.append(
            f"| {int(n):,} | {_fmt_s(s['elapsed_s'])} | {s['events_per_second']['median']:,.0f} | "
            f"{s['peak_rss_mb']['median']:.0f} | {s['storage_bytes']['median'] / 1e6:.1f} | "
            f"{c['gate_latency_ms_p50']['median']:.1f} / {c['gate_latency_ms_p95']['median']:.1f} | "
            f"{c['publish_latency_ms_p50']['median']:.1f} / {c['publish_latency_ms_p95']['median']:.1f} |"
        )
    lines += ["", "Share of wall time by stage (median over runs):", "",
              "| Events | gate | stage (build+encode+fsync+record) | verify | place | commit | other (read, batching, bookkeeping) |",
              "|---:|---:|---:|---:|---:|---:|---:|"]  # fmt: skip
    for n, c in res["scales"].items():
        sh = c["stage_share"]
        cells = [f"{sh[k] * 100:.0f}%" for k in ("gate", "stage", "verify", "place", "commit")]
        cells.append(f"{(1 - sum(sh.values())) * 100:.0f}%")
        lines.append(f"| {int(n):,} | " + " | ".join(cells) + " |")
    for key, title, col in (
        ("batch_size_sweep", "Batch-size sweep (ZSTD)", "batch_size"),
        ("compression_sweep", "Compression sweep (batch 5,000)", "compression"),
    ):
        if key in res:
            lines += [
                "",
                f"### {title} - {res[key]['events']:,} events",
                "",
                f"| {col.replace('_', ' ')} | Wall s | Events/s | Peak RSS MB | Parquet MB | Files | Stage MB/s | Publish p50 ms |",
                "|---|---:|---:|---:|---:|---:|---:|---:|",
            ]
            for c in res[key]["cases"]:
                s = c["summary"]
                name = f"{c[col]:,}" if col == "batch_size" else c[col]
                lines.append(
                    f"| {name} | {_fmt_s(s['elapsed_s'])} | {s['events_per_second']['median']:,.0f} | "
                    f"{s['peak_rss_mb']['median']:.0f} | {s['storage_bytes']['median'] / 1e6:.1f} | "
                    f"{c['files']:,} | {s['parquet_write_mb_per_s']['median']:.0f} | "
                    f"{c['publish_latency_ms_p50']['median']:.1f} |"
                )
    q = next((c.get("queries") for c in res["scales"].values() if "queries" in c), None)
    if q:
        lines += ["", "### DuckDB query latency (median of 5, seconds; each includes opening the connection and the view)", "",
                  "| Query | median | first | min-max |", "|---|---:|---:|---:|"]  # fmt: skip
        lines.append(
            f"| connect + view setup only | {q['connect_and_view_setup_s']['median']:.3f} | - | {q['connect_and_view_setup_s']['min']:.3f}-{q['connect_and_view_setup_s']['max']:.3f} |"
        )
        for name in QUERIES:
            x = q[name]
            lines.append(
                f"| {name} | {x['median']:.3f} | {x['first']:.3f} | {x['min']:.3f}-{x['max']:.3f} |"
            )
    rv = next(
        (c.get("replay_verify") for c in res["scales"].values() if "replay_verify" in c), None
    )
    if rv or "recovery" in res:
        lines += [
            "",
            "### Recovery, replay, verification",
            "",
            "| Operation | Seconds | Notes |",
            "|---|---:|---|",
        ]
        if "recovery" in res:
            r = res["recovery"]
            lines.append(
                f"| `recover` after crash in last batch, **resume** path | {r['resume']['seconds']:.2f} | includes deep check of all committed files; actions {r['resume']['actions']} |"
            )
            lines.append(
                f"| `recover` after crash in last batch, **replay** path | {r['replay']['seconds']:.2f} | actions {r['replay']['actions']} |"
            )
        if rv:
            lines.append(
                f"| `replay` of all committed batches | {rv['replay']['seconds']:.2f} | {rv['replay']['rows_per_second']:,.0f} rows/s, {rv['replay']['batches']} batches, identical: {rv['replay']['all_identical']} |"
            )
            lines.append(
                f"| deep file check (size+SHA-256+Parquet) | {rv['deep_file_check_s']:.2f} | problems: {rv['deep_file_check_problems']} |"
            )
            lines.append(
                f"| `verify` ({rv['verify']['assertions']} SQL assertions) | {rv['verify']['seconds']:.2f} | {rv['verify']['result']} |"
            )
    gen = res.get("generation", {})
    if gen:
        lines += [
            "",
            "### Source generation (single thread)",
            "",
            "| Events | Seconds | Events/s | Source MB |",
            "|---:|---:|---:|---:|",
        ]
        for n, g in gen.items():
            lines.append(
                f"| {int(n):,} | {g['seconds']:.1f} | {g['events_per_second']:,.0f} | {g['source_bytes'] / 1e6:.0f} |"
            )
    return "\n".join(lines)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "worker":
        print(json.dumps(_worker_ingest(json.loads(sys.argv[2]))))
    else:
        raise SystemExit("internal entry point: use `telemetry benchmark`")

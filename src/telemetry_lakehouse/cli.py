"""Command-line interface: ``telemetry simulate | ingest | query``.

Exit codes: 0 success, 1 runtime failure (I/O, conflicts), 2 usage or configuration error,
3 integrity finding (the gate blocked a batch, or sequence gaps were proven).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from pydantic import ValidationError

from telemetry_lakehouse import failpoints, log
from telemetry_lakehouse.config import (
    FAULT_KINDS,
    BatchConfig,
    FaultConfig,
    GateConfig,
    SimulatorConfig,
    StorageConfig,
)
from telemetry_lakehouse.fsutil import ImmutableFileExistsError
from telemetry_lakehouse.ingest import JsonlSource, run_ingest
from telemetry_lakehouse.layout import StorageLayout
from telemetry_lakehouse.manifest import BatchState, Manifest, ManifestError
from telemetry_lakehouse.reader import NAMED_QUERIES, run_named_query
from telemetry_lakehouse.recovery import recover, replay_committed
from telemetry_lakehouse.simulator import write_source
from telemetry_lakehouse.verify import VerifyConfig, load_assertions, verify, write_report

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_INTEGRITY = 3


class UsageError(Exception):
    pass


def _common() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument(
        "--root",
        type=Path,
        default=Path(os.environ.get("TELEMETRY_ROOT", "lakehouse")),
        help="lakehouse root directory (env TELEMETRY_ROOT, default ./lakehouse)",
    )
    p.add_argument(
        "--log-level",
        default=os.environ.get("TELEMETRY_LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="stderr log level (env TELEMETRY_LOG_LEVEL, default INFO)",
    )
    return p


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="telemetry", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    common = _common()

    sim = sub.add_parser("simulate", parents=[common], help="generate a deterministic source log")
    sim.add_argument("--devices", type=int, default=1000)
    sim.add_argument("--events", type=int, default=100_000)
    sim.add_argument("--seed", type=int, default=42)
    sim.add_argument("--device-prefix", default="dev", help="device id prefix (unique per source)")
    sim.add_argument("--rate", type=float, default=1.0, help="simulated events/second")
    sim.add_argument("--start", type=str, default=None, help="ISO-8601 start time with offset")
    sim.add_argument("--mode", choices=["accelerated", "realtime"], default="accelerated")
    sim.add_argument("--out", type=Path, default=None, help="source log path")
    sim.add_argument(
        "--fault",
        action="append",
        default=[],
        metavar="KIND=RATE",
        help=f"inject a fault (repeatable). kinds: {', '.join(FAULT_KINDS)}",
    )
    sim.add_argument("--reorder-window", type=int, default=50)
    sim.add_argument("--delay-positions", type=int, default=5000)
    sim.add_argument(
        "--interrupt-after", type=int, default=None, help="stop the source after N records"
    )

    ing = sub.add_parser("ingest", parents=[common], help="micro-batch the source into Parquet")
    ing.add_argument("--source", type=Path, default=None, help="source log (default <root>/source)")
    ing.add_argument("--batch-size", type=int, default=5000)
    ing.add_argument("--max-batch-age", type=float, default=None, help="time-based flush (s)")
    ing.add_argument(
        "--compression", default="zstd", choices=["zstd", "snappy", "gzip", "lz4", "none"]
    )
    ing.add_argument("--row-group-size", type=int, default=131_072)
    ing.add_argument("--mode", choices=["strict", "quarantine"], default="strict")
    ing.add_argument("--late-policy", choices=["accept", "reject", "hold"], default="accept")
    ing.add_argument("--max-lateness", type=float, default=3600.0, help="seconds")
    ing.add_argument("--max-future-skew", type=float, default=300.0, help="seconds")
    ing.add_argument("--gap-grace", type=float, default=3600.0, help="seconds")
    ing.add_argument("--on-block", choices=["halt", "continue"], default="halt")
    ing.add_argument("--format", choices=["json", "text"], default="json", help="summary format")
    ing.add_argument(
        "--resume",
        action="store_true",
        help="start at the first offset not covered by committed batches (from the manifest)",
    )

    sub.add_parser("status", parents=[common], help="show batch states from the manifest")

    rec = sub.add_parser(
        "recover", parents=[common], help="resolve unfinished batches, sweep orphans"
    )
    rec.add_argument("--source", type=Path, default=None, help="source log (default <root>/source)")
    rec.add_argument("--no-verify-files", action="store_true", help="skip committed-file checks")
    rec.add_argument(
        "--repair",
        action="store_true",
        help="restore damaged committed files if replay reproduces them byte for byte",
    )
    rec.add_argument("--mode", choices=["strict", "quarantine"], default="strict")

    ver = sub.add_parser(
        "verify",
        parents=[common],
        help="independently verify the published dataset with DuckDB SQL",
    )
    ver.add_argument("--format", choices=["text", "json"], default="text")
    ver.add_argument(
        "--allow-incomplete", action="store_true", help="completeness failures -> WARN"
    )
    ver.add_argument("--grace", type=float, default=3600.0, help="watermark grace period (s)")
    ver.add_argument("--max-future-skew", type=float, default=300.0, help="seconds")
    ver.add_argument(
        "--source", type=Path, action="append", default=[], help="extra source sidecar"
    )
    ver.add_argument("--assertion", action="append", default=None, help="run only this assertion")
    ver.add_argument("--list", action="store_true", help="list assertions and exit")
    ver.add_argument("--no-report-file", action="store_true")

    bm = sub.add_parser("benchmark", parents=[common], help="reproducible performance benchmark")
    bm.add_argument("--events", type=int, default=1_000_000, help="main scale (events)")
    bm.add_argument(
        "--scales", default="", help="extra scales, comma separated (e.g. 100000,10000000)"
    )
    bm.add_argument("--devices", type=int, default=1000)
    bm.add_argument("--seed", type=int, default=42)
    bm.add_argument("--repeats", type=int, default=3)
    bm.add_argument("--batch-sizes", default="", help="comma separated sweep, e.g. 1000,5000,50000")
    bm.add_argument(
        "--compressions", default="", help="comma separated sweep, e.g. zstd,snappy,none"
    )
    bm.add_argument("--sweep-events", type=int, default=None, help="events for the sweeps")
    bm.add_argument("--no-recovery", action="store_true")
    bm.add_argument("--label", default="run")
    bm.add_argument("--out-dir", type=Path, default=Path("benchmarks/results"))
    bm.add_argument("--workdir", type=Path, default=None, help="scratch dir (default: a temp dir)")
    bm.add_argument("--keep", action="store_true", help="keep the scratch dir")
    bm.add_argument("--render-only", type=Path, default=None, help="re-render a results JSON")

    rpl = sub.add_parser(
        "replay", parents=[common], help="rebuild committed batches from the source"
    )
    rpl.add_argument("--source", type=Path, default=None, help="source log (default <root>/source)")
    rpl.add_argument("--batch-id", default=None)

    qry = sub.add_parser("query", parents=[common], help="run a named analytical query")
    qry.add_argument("--name", choices=sorted(NAMED_QUERIES), required=True)
    qry.add_argument("--limit", type=int, default=20)
    qry.add_argument("--format", choices=["table", "json"], default="table")
    return parser


def _parse_faults(args: argparse.Namespace) -> FaultConfig:
    rates: dict[str, float] = {}
    for item in args.fault:
        kind, sep, value = item.partition("=")
        if not sep or kind not in FAULT_KINDS:
            raise UsageError(f"--fault expects KIND=RATE with KIND in {', '.join(FAULT_KINDS)}")
        try:
            rates[kind] = float(value)
        except ValueError as exc:
            raise UsageError(f"--fault {item}: rate is not a number") from exc
    return FaultConfig(
        **rates,
        reorder_window=args.reorder_window,
        delay_positions=args.delay_positions,
        interrupt_after=args.interrupt_after,
    )


def _cmd_simulate(args: argparse.Namespace) -> int:
    layout = StorageLayout(args.root)
    fields: dict = {
        "devices": args.devices,
        "events": args.events,
        "seed": args.seed,
        "device_prefix": args.device_prefix,
        "rate_per_second": args.rate,
        "mode": args.mode,
    }
    if args.start:
        fields["start_time"] = datetime.fromisoformat(args.start)
    cfg = SimulatorConfig(**fields)
    out = args.out or layout.default_source
    meta = write_source(cfg, out, faults=_parse_faults(args))
    log.emit("source_written", path=str(out), records=meta["records"], source_id=meta["source_id"])
    print(
        json.dumps(
            {
                "source": str(out),
                **{k: meta[k] for k in ("source_id", "records", "complete")},
                "faults_injected": meta["faults"]["injected"],
            }
        )
    )
    return EXIT_OK


def _cmd_ingest(args: argparse.Namespace) -> int:
    layout = StorageLayout(args.root)
    batch_cfg = BatchConfig(batch_size=args.batch_size, max_age_seconds=args.max_batch_age)
    storage_cfg = StorageConfig(compression=args.compression, row_group_size=args.row_group_size)
    path = args.source or layout.default_source
    try:
        source = JsonlSource.open(path)
        if args.resume:
            layout.ensure()
            with Manifest.open(layout) as manifest:
                source.start_offset = manifest.committed_frontier(source.source_id)
    except FileNotFoundError as exc:
        raise UsageError(str(exc)) from exc
    gate_cfg = GateConfig(
        mode=args.mode,
        late_policy=args.late_policy,
        max_lateness_seconds=args.max_lateness,
        max_future_skew_seconds=args.max_future_skew,
        gap_grace_seconds=args.gap_grace,
        on_block=args.on_block,
    )
    summary = run_ingest(source, layout, batch_cfg, storage_cfg, gate_cfg)
    print(
        summary.render_text() if args.format == "text" else json.dumps(summary.to_dict(), indent=2)
    )
    return EXIT_OK if summary.integrity_ok else EXIT_INTEGRITY


def _open_source_if_present(args: argparse.Namespace, layout: StorageLayout) -> JsonlSource | None:
    path = args.source or layout.default_source
    if not path.exists():
        if args.source:
            raise UsageError(f"source log not found: {path}")
        return None
    return JsonlSource.open(path)


def _cmd_recover(args: argparse.Namespace) -> int:
    layout = StorageLayout(args.root)
    source = _open_source_if_present(args, layout)
    with Manifest.open(layout) as manifest:
        report = recover(
            layout,
            manifest,
            source=source,
            gate=GateConfig(mode=args.mode),
            verify_files=not args.no_verify_files,
            repair=args.repair,
        )
    print(json.dumps(report.to_dict(), indent=2))
    return EXIT_OK if report.ok else EXIT_INTEGRITY


def _cmd_verify(args: argparse.Namespace) -> int:
    if args.list:
        for spec in load_assertions():
            print(f"{spec.name:<42} {spec.category:<14} {spec.description}")
        return EXIT_OK
    known = {s.name for s in load_assertions()}
    if args.assertion and not set(args.assertion) <= known:
        raise UsageError(f"unknown assertion(s): {', '.join(sorted(set(args.assertion) - known))}")
    layout = StorageLayout(args.root)
    report = verify(
        layout,
        VerifyConfig(
            grace_seconds=args.grace,
            future_skew_seconds=args.max_future_skew,
            allow_incomplete=args.allow_incomplete,
            sidecars=tuple(args.source),
            only=frozenset(args.assertion) if args.assertion else None,
        ),
    )
    if not args.no_report_file:
        write_report(layout, report)
    print(
        json.dumps(report.to_dict(), indent=2, default=str)
        if args.format == "json"
        else report.render()
    )
    return EXIT_OK if report.ok else EXIT_INTEGRITY


def _ints(text: str) -> tuple[int, ...]:
    try:
        return tuple(int(x) for x in text.split(",") if x.strip())
    except ValueError as exc:
        raise UsageError(f"expected comma separated integers, got {text!r}") from exc


def _cmd_benchmark(args: argparse.Namespace) -> int:
    from telemetry_lakehouse.benchmark import BenchConfig, render_markdown, run_suite

    log.configure("ERROR")
    if args.render_only:
        print(render_markdown(json.loads(args.render_only.read_text())))
        return EXIT_OK
    codecs = tuple(c for c in args.compressions.split(",") if c.strip())
    if any(c not in ("zstd", "snappy", "gzip", "lz4", "none") for c in codecs):
        raise UsageError("unknown compression in --compressions")
    cfg = BenchConfig(
        events=args.events, devices=args.devices, seed=args.seed, repeats=args.repeats,
        batch_sizes=_ints(args.batch_sizes), compressions=codecs, sweep_events=args.sweep_events,
        scales=_ints(args.scales), recovery=not args.no_recovery, label=args.label,
        workdir=args.workdir, keep=args.keep,
    )  # fmt: skip
    if cfg.repeats < 1 or cfg.events < 1:
        raise UsageError("--events and --repeats must be >= 1")
    result = run_suite(cfg, progress=lambda m: print(m, file=sys.stderr))
    args.out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    path = args.out_dir / f"{stamp}-{args.label}.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    print(render_markdown(result))
    print(f"\nraw results: {path}", file=sys.stderr)
    return EXIT_OK


def _cmd_replay(args: argparse.Namespace) -> int:
    layout = StorageLayout(args.root)
    source = _open_source_if_present(args, layout)
    if source is None:
        raise UsageError("replay needs a source log (--source)")
    with Manifest.open(layout) as manifest:
        results = replay_committed(layout, manifest, source, StorageConfig(), args.batch_id)
    rows = sum(r.rows for r in results)
    seconds = sum(r.seconds for r in results)
    summary = {
        "batches": len(results),
        "identical": sum(r.ok for r in results),
        "rows": rows,
        "seconds": round(seconds, 3),
        "rows_per_second": round(rows / seconds, 1) if seconds else 0.0,
        "mismatches": [
            {"batch_id": r.batch_id, "offsets": list(r.offsets), "detail": r.detail}
            for r in results
            if not r.ok
        ],
    }
    print(json.dumps(summary, indent=2))
    return EXIT_OK if results and not summary["mismatches"] else EXIT_INTEGRITY


def _cmd_status(args: argparse.Namespace) -> int:
    manifest = Manifest.open_readonly(StorageLayout(args.root))
    if manifest is None:
        print(json.dumps({"batches": {}, "in_flight": [], "last_commit_seq": 0}))
        return EXIT_OK
    with manifest:
        in_flight = manifest.in_flight()
        committed = manifest.batches(BatchState.COMMITTED)
        print(
            json.dumps(
                {
                    "batches": manifest.state_counts(),
                    "last_commit_seq": max((b.commit_seq or 0 for b in committed), default=0),
                    "committed_rows": sum(b.committed_rows or 0 for b in committed),
                    "in_flight": [
                        {
                            "batch_id": b.batch_id,
                            "state": b.state,
                            "offsets": [b.start_offset, b.end_offset],
                        }
                        for b in in_flight
                    ],
                },
                indent=2,
            )
        )
    return EXIT_INTEGRITY if in_flight else EXIT_OK


def _format_table(columns: list[str], rows: list[tuple]) -> str:
    cells = [[str(c) for c in columns], *[["" if v is None else str(v) for v in r] for r in rows]]
    widths = [max(len(row[i]) for row in cells) for i in range(len(columns))]
    lines = ["  ".join(c.ljust(w) for c, w in zip(row, widths, strict=True)) for row in cells]
    lines.insert(1, "  ".join("-" * w for w in widths))
    return "\n".join(lines)


def _cmd_query(args: argparse.Namespace) -> int:
    columns, rows = run_named_query(StorageLayout(args.root), args.name, limit=args.limit)
    if args.format == "json":
        print(json.dumps([dict(zip(columns, r, strict=True)) for r in rows], default=str))
    else:
        print(_format_table(columns, rows))
    return EXIT_OK


_COMMANDS = {
    "simulate": _cmd_simulate,
    "ingest": _cmd_ingest,
    "query": _cmd_query,
    "status": _cmd_status,
    "recover": _cmd_recover,
    "verify": _cmd_verify,
    "benchmark": _cmd_benchmark,
    "replay": _cmd_replay,
}


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    log.configure(args.log_level)
    failpoints.load_from_env()
    try:
        return _COMMANDS[args.command](args)
    except (UsageError, ValidationError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except ManifestError as exc:
        log.emit("manifest_error", logging.ERROR, detail=str(exc))
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except ImmutableFileExistsError as exc:
        log.emit("publish_conflict", logging.ERROR, detail=str(exc))
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())

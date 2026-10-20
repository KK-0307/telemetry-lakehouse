#!/usr/bin/env python3
"""Record a short demo from a REAL run and render it as an animated terminal SVG.

    uv run python scripts/record_demo.py      # writes docs/demo/{session.json,transcript.txt,demo.svg}

Phase 1 executes the commands below in a scratch directory and captures their exact output and exit
status. Phase 2 renders that capture as a self-contained animated SVG (no JavaScript, no external
assets, so it plays inside a GitHub README). What is synthetic: the typing speed and the pacing at
which output lines appear. What is not: every output line and every exit status is what the command
really printed/returned; the real wall time of each command is stored in session.json.
"""

from __future__ import annotations

import html
import json
import os
import re
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "demo"

# (kind, text-or-args). "say" = shell comment, "run" = command, "echo" = print the previous exit status.
STEPS: list[tuple[str, object]] = [
    ("say", "# 1. A valid stream: 20,000 events from 1,000 devices -> committed batches"),
    ("run", ["simulate", "--devices", "1000", "--events", "20000", "--seed", "42"]),
    ("run", ["ingest", "--batch-size", "5000", "--format", "text"]),
    ("say", "# DuckDB re-verifies the published Parquet with 18 independent SQL assertions"),
    ("run", ["verify"]),
    ("say", "# 2. A second source with corrupted measurements injected"),
    (
        "run",
        [
            "simulate",
            "--events",
            "20000",
            "--seed",
            "43",
            "--device-prefix",
            "edge",
            "--out",
            "b.jsonl",
            "--fault",
            "invalid_value=0.0002",
        ],
    ),
    ("run", ["ingest", "--source", "b.jsonl", "--batch-size", "5000", "--format", "text"]),
    ("echo", None),
    ("run", ["status"]),
    ("say", "# 3. The rejected batch never reached the trusted data - DuckDB agrees"),
    ("run", ["verify", "--allow-incomplete"]),
    ("run", ["query", "--name", "fleet-summary"]),
]

COLS, ROWS, LH, FS = 112, 30, 18, 13
CHAR_W = 7.8
W = int(COLS * CHAR_W) + 40
H = ROWS * LH + 52
TYPE_S, OUT_LINE_S = 0.04, 0.085


def capture() -> list[dict]:
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src"), "TELEMETRY_LOG_LEVEL": "ERROR"}
    session: list[dict] = []
    last_code = 0
    with tempfile.TemporaryDirectory(prefix="telemetry-demo-") as tmp:
        for kind, payload in STEPS:
            if kind == "say":
                session.append({"kind": "say", "text": payload})
            elif kind == "echo":
                session.append(
                    {
                        "kind": "run",
                        "display": 'echo "exit status: $?"',
                        "output": [f"exit status: {last_code}"],
                        "exit": 0,
                        "seconds": 0.0,
                    }
                )
            else:
                args = list(payload)  # type: ignore[arg-type]
                t0 = time.perf_counter()
                proc = subprocess.run(
                    [sys.executable, "-m", "telemetry_lakehouse.cli", *args],
                    cwd=tmp, env=env, capture_output=True, text=True,
                )  # fmt: skip
                seconds = time.perf_counter() - t0
                last_code = proc.returncode
                text = (proc.stdout + proc.stderr).rstrip("\n")
                session.append({
                    "kind": "run",
                    "display": "uv run telemetry " + " ".join(args),
                    "output": text.split("\n") if text else [],
                    "exit": proc.returncode,
                    "seconds": round(seconds, 3),
                })  # fmt: skip
    return session


def wrap(line: str) -> list[str]:
    if len(line) <= COLS:
        return [line]
    indent = len(line) - len(line.lstrip())
    return textwrap.wrap(line, COLS, subsequent_indent=" " * (indent + 4), break_long_words=False,
                         break_on_hyphens=False, replace_whitespace=False, drop_whitespace=False)  # fmt: skip


def colour(line: str) -> list[tuple[str, str]]:
    """Split a line into (text, fill) spans."""
    for token, fill in (
        ("PASS", "#3fb950"),
        ("FAIL", "#f85149"),
        ("WARN", "#d29922"),
        ("SKIP", "#8b949e"),
    ):
        if line.startswith(token + " "):
            return [(token, fill), (line[len(token) :], "#c9d1d9")]
    if line.startswith("RESULT: PASS"):
        return [(line, "#3fb950")]
    if line.startswith("RESULT: FAIL"):
        return [(line, "#f85149")]
    m = re.match(r"^(\s*)(BLOCKED batch)(.*)$", line)
    if m:
        return [(m.group(1), "#c9d1d9"), (m.group(2), "#f85149"), (m.group(3), "#c9d1d9")]
    if line.startswith("exit status: 3"):
        return [(line, "#d29922")]
    if line.startswith("exit status: 0"):
        return [(line, "#3fb950")]
    if line.lstrip().startswith("run halted"):
        return [(line, "#d29922")]
    return [(line, "#c9d1d9")]


def build_timeline(session: list[dict]) -> tuple[list[dict], float]:
    """-> list of display lines [{spans|typed, t}], total duration. Typed lines carry chunk times."""
    lines: list[dict] = []
    t = 0.8
    for item in session:
        if item["kind"] == "say":
            if lines:
                lines.append({"kind": "out", "text": "", "t": t})
            t += 0.5
            lines.append({"kind": "say", "text": item["text"], "t": t})
            t += 1.6
            continue
        cmd = item["display"]
        chunks = 9
        step = max(1, len(cmd) // chunks)
        stops = [*range(step, len(cmd), step), len(cmd)]
        times = []
        for s in stops:
            times.append(t + s * TYPE_S)
        lines.append({"kind": "cmd", "text": cmd, "stops": stops, "times": times, "t": times[0]})
        t = times[-1] + 0.5
        for raw in item["output"]:
            for piece in wrap(raw):
                lines.append({"kind": "out", "text": piece, "t": t})
                t += OUT_LINE_S
        t += 3.0
    t += 4.0
    return lines, t


def render_svg(session: list[dict]) -> str:
    lines, total = build_timeline(session)
    pct = lambda x: min(99.9, max(0.0, 100.0 * x / total))  # noqa: E731
    css: list[str] = []
    body: list[str] = []
    for i, ln in enumerate(lines):
        y = (i + 1) * LH
        ln["y"] = y
    # scroll: offset grows when a line beyond ROWS is revealed
    scroll = [(0.0, 0)]
    for i, ln in enumerate(lines):
        if i >= ROWS - 1:
            scroll.append((ln["t"], (i - (ROWS - 2)) * LH))
    kf = ["0%{transform:translateY(0)}"]
    for t, off in scroll[1:]:
        kf.append(f"{pct(t - 0.15):.3f}%{{transform:translateY({-(off - LH)}px)}}")
        kf.append(f"{pct(t):.3f}%{{transform:translateY({-off}px)}}")
    kf.append(f"{pct(total - 0.2):.3f}%{{transform:translateY({-scroll[-1][1]}px)}}")
    kf.append("100%{transform:translateY(0)}")
    css.append("@keyframes scroll{" + "".join(kf) + "}")

    def show(name: str, start: float, end: float | None = None) -> None:
        end_part = f"{pct(end):.3f}%{{opacity:0}}" if end is not None else ""
        css.append(
            f"@keyframes {name}{{0%{{opacity:0}}{pct(start):.3f}%{{opacity:0}}"
            f"{min(99.95, pct(start) + 0.001):.3f}%{{opacity:1}}"
            + (
                f"{pct(end):.3f}%{{opacity:1}}{min(99.97, pct(end) + 0.001):.3f}%{{opacity:0}}"
                if end_part
                else ""
            )
            + f"{pct(total - 0.2):.3f}%{{opacity:1}}99.9%{{opacity:0}}100%{{opacity:0}}}}"
        )
        css.append(f".{name}{{animation:{name} {total:.2f}s linear infinite;opacity:0}}")

    for i, ln in enumerate(lines):
        y = ln["y"]
        if ln["kind"] == "say":
            show(f"s{i}", ln["t"])
            body.append(
                f'<text class="s{i}" x="0" y="{y}" fill="#7d8590" font-style="italic">{html.escape(ln["text"])}</text>'
            )
        elif ln["kind"] == "cmd":
            n = len(ln["stops"])
            for k, (stop, tk) in enumerate(zip(ln["stops"], ln["times"], strict=True)):
                end = ln["times"][k + 1] if k + 1 < n else None
                name = f"c{i}_{k}"
                show(name, tk, end)
                body.append(
                    f'<text class="{name}" x="0" y="{y}"><tspan fill="#3fb950">$ </tspan>'
                    f'<tspan fill="#e6edf3">{html.escape(ln["text"][:stop])}</tspan></text>'
                )
        else:
            show(f"o{i}", ln["t"])
            spans = "".join(
                f'<tspan fill="{fill}">{html.escape(txt)}</tspan>'
                for txt, fill in colour(ln["text"])
            )
            body.append(f'<text class="o{i}" x="0" y="{y}">{spans}</text>')

    real = sum(item.get("seconds", 0.0) for item in session if item["kind"] == "run")
    desc = (
        "Animated terminal recording of a real run: a valid stream is ingested and committed, DuckDB verifies it "
        f"(18 SQL assertions), a corrupted batch is rejected, and DuckDB verifies the data is unchanged. "
        f"Playback {total:.0f}s; the commands really took {real:.1f}s in total."
    )
    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" role="img" aria-labelledby="t d">
<title id="t">Telemetry Lakehouse demo</title><desc id="d">{html.escape(desc)}</desc>
<style>
text{{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,'Liberation Mono','DejaVu Sans Mono',monospace;font-size:{FS}px;white-space:pre}}
{chr(10).join(css)}
.scroll{{animation:scroll {total:.2f}s linear infinite}}
</style>
<defs><clipPath id="v"><rect x="0" y="0" width="{W - 40}" height="{ROWS * LH + 4}"/></clipPath></defs>
<rect width="{W}" height="{H}" rx="10" fill="#0d1117"/>
<rect width="{W}" height="30" rx="10" fill="#161b22"/><rect y="20" width="{W}" height="10" fill="#161b22"/>
<circle cx="18" cy="15" r="6" fill="#ff5f56"/><circle cx="38" cy="15" r="6" fill="#ffbd2e"/><circle cx="58" cy="15" r="6" fill="#27c93f"/>
<text x="{W // 2}" y="20" fill="#8b949e" text-anchor="middle" style="font-size:12px">telemetry-lakehouse - real run, replayed (about {total:.0f}s)</text>
<g transform="translate(20,44)"><g clip-path="url(#v)"><g class="scroll">
{chr(10).join(body)}
</g></g></g>
</svg>
"""


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    session = capture()
    (OUT / "session.json").write_text(json.dumps(session, indent=2) + "\n")
    transcript = []
    for item in session:
        if item["kind"] == "say":
            transcript.append(item["text"])
        else:
            transcript.append(f"$ {item['display']}")
            transcript += item["output"]
        transcript.append("")
    (OUT / "transcript.txt").write_text("\n".join(transcript))
    svg = render_svg(session)
    (OUT / "demo.svg").write_text(svg)
    lines, total = build_timeline(session)
    print(f"captured {sum(1 for s in session if s['kind'] == 'run')} commands, "
          f"{len(lines)} lines, playback {total:.1f}s, svg {len(svg) / 1024:.0f} KiB")  # fmt: skip
    codes = [(s["display"], s["exit"]) for s in session if s["kind"] == "run"]
    print("exit statuses:", codes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

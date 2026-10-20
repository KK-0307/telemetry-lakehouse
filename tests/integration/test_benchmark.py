"""The benchmark harness itself must work end to end (tiny scale; numbers are not asserted)."""

from __future__ import annotations

import json

from telemetry_lakehouse.benchmark import BenchConfig, hardware_info, render_markdown, run_suite
from telemetry_lakehouse.cli import EXIT_OK, main


def test_suite_runs_every_section_and_renders(tmp_path):
    cfg = BenchConfig(
        events=6000, devices=50, repeats=1, batch_sizes=(1000, 3000), compressions=("zstd", "none"),
        recovery=True, label="unit", workdir=tmp_path / "work",
    )  # fmt: skip
    result = run_suite(cfg, progress=lambda _m: None)

    scale = result["scales"]["6000"]
    assert scale["records"] == 6000 and scale["summary"]["events_per_second"]["median"] > 0
    assert scale["summary"]["storage_bytes"]["median"] > 0
    assert set(scale["queries"]) >= {"count", "fleet-summary", "point-lookup"}
    assert scale["replay_verify"]["replay"]["all_identical"] is True
    assert scale["replay_verify"]["verify"]["result"] == "PASS"
    assert [c["batch_size"] for c in result["batch_size_sweep"]["cases"]] == [1000, 3000]
    assert [c["compression"] for c in result["compression_sweep"]["cases"]] == ["zstd", "none"]
    assert result["recovery"]["resume"]["ok"] and result["recovery"]["replay"]["ok"]
    assert result["recovery"]["resume"]["actions"] == ["resumed_and_committed"]
    assert "interrupted_for_replay" in result["recovery"]["replay"]["actions"]
    assert not (tmp_path / "work").exists()  # scratch space cleaned up
    assert "### Scale" in render_markdown(json.loads(json.dumps(result)))


def test_hardware_info_is_complete():
    hw = hardware_info()
    assert hw["python"] and hw["pyarrow"] and hw["duckdb"] and hw["logical_cores"]


def test_cli_benchmark_writes_results_and_can_rerender(tmp_path, capsys):
    out = tmp_path / "res"
    argv = ["benchmark", "--events", "3000", "--devices", "20", "--repeats", "1", "--no-recovery",
            "--label", "cli", "--out-dir", str(out), "--workdir", str(tmp_path / "w")]  # fmt: skip
    assert main(argv) == EXIT_OK
    assert "### Scale" in capsys.readouterr().out
    (path,) = out.glob("*-cli.json")
    assert main(["benchmark", "--render-only", str(path)]) == EXIT_OK
    assert "3,000" in capsys.readouterr().out

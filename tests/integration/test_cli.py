from __future__ import annotations

import json

import pytest

from telemetry_lakehouse.cli import EXIT_INTEGRITY, EXIT_OK, EXIT_USAGE, main


def run(capsys, *argv: str):
    code = main(list(argv))
    out = capsys.readouterr()
    return code, out.out, out.err


def test_simulate_ingest_query_pipeline(tmp_path, capsys):
    root = str(tmp_path / "lh")
    code, out, _ = run(
        capsys, "simulate", "--root", root, "--devices", "30", "--events", "2500", "--seed", "3",
        "--rate", "0.05", "--start", "2026-03-01T20:00:00+00:00",
    )  # fmt: skip
    assert code == EXIT_OK
    assert json.loads(out)["records"] == 2500

    code, out, _ = run(capsys, "ingest", "--root", root, "--batch-size", "1000")
    assert code == EXIT_OK
    summary = json.loads(out)
    assert summary["records"] == 2500
    assert summary["batches"] == 3
    assert summary["events_per_second"] > 0

    code, out, _ = run(
        capsys, "query", "--root", root, "--name", "fleet-summary", "--format", "json"
    )
    assert code == EXIT_OK
    assert sum(r["events"] for r in json.loads(out)) == 2500

    code, out, _ = run(capsys, "query", "--root", root, "--name", "hourly-device-health")
    assert code == EXIT_OK
    assert "hour_utc" in out.splitlines()[0]


def test_ingest_without_source_is_a_usage_error(tmp_path, capsys):
    code, _, err = run(capsys, "ingest", "--root", str(tmp_path / "empty"))
    assert code == EXIT_USAGE
    assert "telemetry simulate" in err


@pytest.mark.parametrize("bad", [["--devices", "0"], ["--rate", "-1"], ["--start", "not-a-date"]])
def test_invalid_simulator_config_is_a_usage_error(tmp_path, capsys, bad):
    code, _, err = run(capsys, "simulate", "--root", str(tmp_path), *bad)
    assert code == EXIT_USAGE
    assert err.startswith("error:")


def test_invalid_batch_size_is_a_usage_error(tmp_path, capsys):
    run(capsys, "simulate", "--root", str(tmp_path), "--events", "10", "--devices", "2")
    code, _, _ = run(capsys, "ingest", "--root", str(tmp_path), "--batch-size", "0")
    assert code == EXIT_USAGE


def test_bad_record_blocks_the_batch_and_exits_with_integrity_status(tmp_path, capsys):
    root = tmp_path
    run(capsys, "simulate", "--root", str(root), "--events", "50", "--devices", "5")
    source = root / "source" / "events.jsonl"
    lines = source.read_bytes().splitlines(keepends=True)
    lines[7] = b"{not json}\n"
    source.write_bytes(b"".join(lines))
    code, out, _ = run(capsys, "ingest", "--root", str(root), "--batch-size", "20")
    assert code == EXIT_INTEGRITY
    summary = json.loads(out)
    assert summary["blocked_batches"][0]["reason_counts"] == {"MALFORMED_PAYLOAD": 1}
    assert summary["accounting"]["committed"] == 0 and summary["halted"] is True
    report = next((root / "rejected").glob("*/attempt-0001/report.json"))
    assert json.loads(report.read_text())["rejections"][0]["offset"] == 7


def test_second_ingest_is_a_harmless_noop(tmp_path, capsys):
    run(capsys, "simulate", "--root", str(tmp_path), "--events", "50", "--devices", "5")
    assert run(capsys, "ingest", "--root", str(tmp_path))[0] == EXIT_OK
    code, out, _ = run(capsys, "ingest", "--root", str(tmp_path))
    assert code == EXIT_OK
    summary = json.loads(out)
    assert summary["skipped_committed_batches"] == 1 and summary["accounting"] == {}


def test_status_reports_batch_states(tmp_path, capsys):
    root = str(tmp_path)
    run(capsys, "simulate", "--root", root, "--events", "250", "--devices", "5")
    run(capsys, "ingest", "--root", root, "--batch-size", "100")
    code, out, _ = run(capsys, "status", "--root", root)
    status = json.loads(out)
    assert code == EXIT_OK and status["batches"] == {"COMMITTED": 3}
    assert status["last_commit_seq"] == 3 and status["committed_rows"] == 250
    assert status["in_flight"] == []


def test_faulty_simulation_reports_injected_faults_and_ingest_finds_them(tmp_path, capsys):
    root = str(tmp_path)
    code, out, _ = run(
        capsys, "simulate", "--root", root, "--devices", "20", "--events", "3000",
        "--fault", "duplicate=0.03", "--fault", "drop=0.02",
    )  # fmt: skip
    assert code == EXIT_OK
    injected = json.loads(out)["faults_injected"]
    code, out, _ = run(capsys, "ingest", "--root", root, "--batch-size", "1000")
    summary = json.loads(out)
    assert code == EXIT_INTEGRITY  # proven sequence gaps are an integrity finding
    assert summary["accounting"]["duplicates"] == injected["duplicate"]
    assert summary["sequence"]["missing_events"] == injected["drop"]


BAD_FAULT_OPTIONS = [
    ["--fault", "bogus=0.1"],
    ["--fault", "drop"],
    ["--fault", "drop=x"],
    ["--fault", "drop=0.7", "--fault", "duplicate=0.7"],
]


@pytest.mark.parametrize("bad", BAD_FAULT_OPTIONS)
def test_invalid_fault_options_are_usage_errors(tmp_path, capsys, bad):
    code, _, err = run(capsys, "simulate", "--root", str(tmp_path), *bad)
    assert code == EXIT_USAGE and err.startswith("error:")


def test_ingest_text_format_summarises_commit_and_block(tmp_path, capsys):
    root = str(tmp_path)
    run(capsys, "simulate", "--root", root, "--events", "300", "--devices", "5")
    code, out, _ = run(capsys, "ingest", "--root", root, "--batch-size", "100", "--format", "text")
    assert code == EXIT_OK and "300 rows committed in" in out and "0 blocked" in out
    assert out.lstrip().startswith("source sim-seed42")

    bad = str(tmp_path / "b.jsonl")
    run(
        capsys, "simulate", "--root", root, "--events", "300", "--devices", "5", "--seed", "9",
        "--device-prefix", "x", "--out", bad, "--fault", "invalid_value=0.02",
    )  # fmt: skip
    code, out, _ = run(
        capsys, "ingest", "--root", root, "--source", bad, "--batch-size", "100", "--format", "text"
    )
    assert code == EXIT_INTEGRITY
    assert "BLOCKED batch" in out and "OUT_OF_RANGE=" in out and "run halted" in out
    assert "evidence:" in out

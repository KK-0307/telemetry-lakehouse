#!/usr/bin/env bash
# End-to-end demonstration of the eight Definition-of-Done steps, with machine-checked expectations.
#
#   scripts/demo.sh                      # ~1 min; EVENTS=100000
#   EVENTS=20000 RUN_TESTS=0 RUN_BENCH=0 scripts/demo.sh     # quick (used by CI)
#
# Environment: DEMO_ROOT (default ./lakehouse-demo, wiped at start), EVENTS, RUN_TESTS, RUN_BENCH.
# Exit status 0 only if every expectation below held. Needs: uv, python3 (stdlib only).
set -uo pipefail
cd "$(dirname "$0")/.."
# Robust to macOS flagging the editable-install .pth file hidden (see README troubleshooting).
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"

ROOT="${DEMO_ROOT:-lakehouse-demo}"
EVENTS="${EVENTS:-100000}"
RUN_TESTS="${RUN_TESTS:-1}"
RUN_BENCH="${RUN_BENCH:-1}"
case "$ROOT" in *demo*) ;; *) echo "refusing to wipe '$ROOT' (DEMO_ROOT must contain 'demo')" >&2; exit 2;; esac
rm -rf "$ROOT"; mkdir -p "$ROOT/.demo"
TMP="$ROOT/.demo"
T="uv run telemetry"
PASSED=0; FAILED=0

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }
pass() { PASSED=$((PASSED + 1)); printf '   \033[32mOK\033[0m   %s\n' "$*"; }
fail() { FAILED=$((FAILED + 1)); printf '   \033[31mFAIL\033[0m %s\n' "$*"; }

# run <expected-exit> <label> <command...>   stdout -> $TMP/out, stderr -> $TMP/err
run() {
  local want="$1" label="$2"; shift 2
  "$@" >"$TMP/out" 2>"$TMP/err"; local got=$?
  if [ "$got" -eq "$want" ]; then pass "$label (exit $got)"; else
    fail "$label: expected exit $want, got $got"; tail -5 "$TMP/err" | sed 's/^/        /'; fi
}
# jget <expr over d>  e.g. jget 'd["records"]'   (reads $TMP/out)
jget() { python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(eval(sys.argv[2]))' "$TMP/out" "$1"; }
check() { # check <label> <python-expr over d> ; passes if truthy
  if [ "$(jget "$2")" = "True" ]; then pass "$1"; else fail "$1  [$2 -> $(jget "$2")]"; fi
}
# Digest of the committed Parquet files of one source, hashed from the bytes actually on disk.
fingerprint() {
  python3 - "$ROOT" "$1" <<'PY'
import hashlib, sqlite3, sys
root, source = sys.argv[1], sys.argv[2]
con = sqlite3.connect(f"{root}/meta/manifest.sqlite")
rows = con.execute("SELECT f.relative_path FROM batch_files f JOIN batches b USING (batch_id) "
                   "WHERE b.state='COMMITTED' AND b.source_id=? ORDER BY 1", (source,)).fetchall()
h = hashlib.sha256()
for (rel,) in rows:
    h.update(rel.encode()); h.update(hashlib.sha256(open(f"{root}/data/{rel}", "rb").read()).digest())
print(f"{h.hexdigest()[:16]} ({len(rows)} files)")
PY
}
visible_events() { $T query --root "$ROOT" --name fleet-summary --format json | python3 -c 'import json,sys; print(sum(r["events"] for r in json.load(sys.stdin)))'; }

R="--root $ROOT --log-level ERROR"

say "1. Generate a deterministic telemetry dataset  (${EVENTS} events, 1,000 devices, seed 42)"
run 0 "simulate source A" $T simulate $R --devices 1000 --events "$EVENTS" --seed 42
$T simulate $R --devices 1000 --events "$EVENTS" --seed 42 --out "$TMP/a-again.jsonl" >/dev/null 2>&1
if cmp -s "$ROOT/source/events.jsonl" "$TMP/a-again.jsonl"; then pass "same seed => byte-identical source log"; else fail "source not reproducible"; fi

say "2. Ingest valid events into partitioned Parquet; verify with independent SQL"
run 0 "ingest A" $T ingest $R --batch-size 5000
check "all ${EVENTS} events committed" "d['accounting']['committed'] == $EVENTS and d['blocked_batches'] == []"
note "layout: $(cd "$ROOT/data" && ls | tr '\n' ' ')"
run 0 "verify (18 independent DuckDB assertions)" $T verify $R --format json
check "verification result is PASS" "d['result'] == 'PASS' and d['counts'] == {'PASS': 18}"
FP_A="$(fingerprint sim-seed42)"; note "fingerprint of source A's committed files: $FP_A"

say "3. Submit a corrupted batch: publication is blocked"
run 0 "simulate source B (20,000 events, with invalid measurements injected)" \
  $T simulate $R --devices 200 --events 20000 --seed 43 --device-prefix edge --out "$ROOT/source/b.jsonl" --fault invalid_value=0.0002
check "faults were injected" "sum(d['faults_injected'].values()) > 0"
note "injected: $(jget 'd["faults_injected"]')"
run 3 "ingest B is blocked (exit 3 = integrity finding)" $T ingest $R --source "$ROOT/source/b.jsonl" --batch-size 5000
check "a batch was blocked with OUT_OF_RANGE and the run halted" \
  "d['halted'] and d['blocked_batches'][0]['reason_counts'].get('OUT_OF_RANGE', 0) >= 1"
note "evidence: $(jget 'd["blocked_batches"][0]["evidence"]')/report.json"

say "4. The trusted dataset is unchanged by the blocked batch"
FP_A2="$(fingerprint sim-seed42)"
if [ "$FP_A" = "$FP_A2" ]; then pass "source A's committed files are byte-identical ($FP_A2)"; else fail "A changed: $FP_A -> $FP_A2"; fi
run 0 "no row from a non-committed batch is in the data; identities consistent" \
  $T verify $R --format json --assertion no_untrusted_batches_in_data --assertion events_and_index_agree \
  --assertion unique_event_id --assertion unique_device_sequence --assertion batch_row_counts_reconcile
$T verify $R --format json >"$TMP/out" 2>/dev/null
check "full verify reports only COMPLETENESS failures (B is known-incomplete)" \
  "d['result']=='FAIL' and {a['name'] for a in d['assertions'] if a['status']=='FAIL'} <= {'sequence_gaps_beyond_grace','sequences_complete_to_end_of_stream','offset_coverage_no_holes'}"

say "5. Interrupt publication mid-commit, then restart"
run 0 "simulate source C (30,000 events)" $T simulate $R --devices 300 --events 30000 --seed 44 --device-prefix gw --out "$ROOT/source/c.jsonl"
TELEMETRY_FAILPOINT=in_commit:3 $T ingest $R --source "$ROOT/source/c.jsonl" --batch-size 5000 >"$TMP/out" 2>"$TMP/err"
if [ $? -eq 137 ]; then pass "process killed inside the commit transaction of C's 3rd batch (exit 137)"; else fail "expected the kill (137)"; fi
run 3 "status shows an unfinished batch" $T status $R
check "batch is VERIFIED but not committed" "[b['state'] for b in d['in_flight']] == ['VERIFIED']"
VISIBLE="$(visible_events)"; COMMITTED="$(jget 'd["committed_rows"]')"
if [ "$VISIBLE" = "$COMMITTED" ]; then pass "readers see exactly the committed rows ($VISIBLE); the half-published batch is invisible"; else fail "readers see $VISIBLE, manifest says $COMMITTED"; fi
run 3 "verify detects the unfinished batch and its unreferenced files" $T verify $R --format json --assertion no_unfinished_batches --assertion no_unreferenced_files_in_data

say "6. Recover without duplicate or partial publication"
run 0 "recover" $T recover $R --source "$ROOT/source/c.jsonl"
check "the batch was resumed from its verified staged files" "[a['action'] for a in d['actions']] == ['resumed_and_committed'] and d['ok']"
run 0 "ingest C --resume finishes the source" $T ingest $R --source "$ROOT/source/c.jsonl" --batch-size 5000 --resume
check "the remaining batches (and only those) were committed" "d['accounting']['committed'] == 30000 - 3 * 5000"
run 0 "re-submitting all of C is an idempotent no-op" $T ingest $R --source "$ROOT/source/c.jsonl" --batch-size 5000
check "6 batches skipped as already committed, nothing re-published" "d['skipped_committed_batches'] == 6 and d['accounting'] == {}"
run 0 "no duplicates after recovery" $T verify $R --format json --assertion unique_event_id --assertion unique_device_sequence --assertion no_unreferenced_files_in_data

say "7. The producer fixes source B; the rejected batch is resubmitted and accepted"
run 0 "regenerate source B without faults" $T simulate $R --devices 200 --events 20000 --seed 43 --device-prefix edge --out "$ROOT/source/b.jsonl"
run 0 "ingest B again" $T ingest $R --source "$ROOT/source/b.jsonl" --batch-size 5000
check "earlier batches skipped, blocked batch and the rest committed, no gaps" "d['skipped_committed_batches'] >= 1 and d['blocked_batches'] == [] and d['sequence']['proven_gaps'] == 0"

say "8. Prove the documented invariants with DuckDB, then replay history"
run 0 "verify: every assertion PASSES" $T verify $R --format json
check "18/18 assertions PASS" "d['result'] == 'PASS' and d['counts'] == {'PASS': 18}"
TOTAL=$((EVENTS + 20000 + 30000))
check "dataset profile: all $TOTAL events, 3 sources" "d['profile']['events'] == $TOTAL"
for src in events.jsonl b.jsonl c.jsonl; do
  run 0 "replay $src from the source: rebuilt batches identical to committed" $T replay $R --source "$ROOT/source/$src"
done
$T verify $R | sed 's/^/   /'
note "analytics on the trusted dataset:"
$T query $R --name fleet-summary | sed 's/^/   /'
$T query $R --name battery-health | sed 's/^/   /'
$T query $R --name hourly-device-health --limit 5 | sed 's/^/   /'

if [ "$RUN_TESTS" = "1" ]; then
  say "9. Automated tests (unit, integration, property-based, failure-injection)"
  if uv run pytest -q -p no:logging >"$TMP/pytest.log" 2>&1; then
    pass "test suite: $(tail -1 "$TMP/pytest.log")"
  else
    fail "test suite"; tail -15 "$TMP/pytest.log" | sed 's/^/        /'
  fi
fi
if [ "$RUN_BENCH" = "1" ]; then
  say "10. Reproducible benchmark (small scale; see docs/benchmarks.md for the full runs)"
  run 0 "benchmark" $T benchmark --events 100000 --repeats 1 --no-recovery --label demo --out-dir "$ROOT/bench"
  sed -n '1,8p' "$TMP/out" | sed 's/^/   /'
fi

say "Result"
printf '   %d checks passed, %d failed\n' "$PASSED" "$FAILED"
[ "$FAILED" -eq 0 ]

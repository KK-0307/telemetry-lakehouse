# Benchmarks

Every number below was produced by the harness in this repository (`telemetry benchmark`, source:
[`benchmark.py`](../src/telemetry_lakehouse/benchmark.py)) and is stored raw in
[`benchmarks/results/`](../benchmarks/results). Nothing is estimated or extrapolated; where something was not
measured, or the cause of a result was not established, this document says so.

## 1. Setup and method

| | |
|---|---|
| Machine | Apple M4 Max, 14 logical cores, 36 GiB RAM, macOS (APFS, internal SSD) |
| Software | Python 3.14.4, pyarrow 26.0.0, duckdb 1.5.6, pydantic 2.14.1, SQLite 3.53.3 |
| Workload | 1,000 simulated devices, seed 42, clean stream (no injected faults), ~1 event/s of simulated time, event ≈ 306 bytes of JSON |
| Defaults | batch size 5,000, ZSTD, `synchronous=FULL` WAL manifest, strict gate, row groups 131,072 |

* The source log is generated once per scale; generation time is reported separately.
* **Every ingest measurement runs in a fresh subprocess on a fresh lakehouse directory.** Peak RSS is that
  process's `ru_maxrss` (it includes interpreter and library baseline ≈ 100 MB). Wall time is measured inside the
  worker around `run_ingest`, so interpreter start-up is excluded.
* Repeats: 3 per configuration at 100k / 1M (median shown, range in parentheses); **1 per configuration at 10M**.
  Single-run figures carry unknown variance. Between two runs of the same code at 1M the median moved by about
  ±5% (58.8k vs 60.2k events/s; 17.0 vs 16.6 s), so treat differences under ~10% as noise.
* Stage timings are the pipeline's own per-batch measurements. *Stage* = build Arrow table, sort, encode Parquet,
  fsync, SHA-256, and record the files in the manifest. "Stage MB/s" divides output bytes by that whole stage time,
  not by pure encode time.
* **Durability caveat for this machine.** On macOS, `fsync` does not force the drive's cache to stable media
  (`F_FULLFSYNC` does, and is used neither by Python's `os.fsync` nor by SQLite by default). Throughput here is for
  plain `fsync`; on Linux ext4/XFS, or with `F_FULLFSYNC`, expect different (likely lower) numbers. Process-crash
  safety is what the tests demonstrate; power-loss behaviour was not measured.

Reproduce:

```bash
uv run telemetry benchmark --events 1000000 --scales 100000 --repeats 3 \
    --batch-sizes 1000,5000,20000,100000 --compressions zstd,snappy,lz4,gzip,none \
    --sweep-events 1000000 --label final
uv run telemetry benchmark --events 10000000 --repeats 1 --no-recovery --label scale-10M   # ~20 min, ~3.6 GB scratch
uv run python benchmarks/sqlite_event_index_bench.py                                          # SQLite micro-benchmark
uv run telemetry benchmark --render-only benchmarks/results/<file>.json                      # re-render any result
```

## 2. Headline results (batch 5,000, ZSTD)

| Events | Wall s | Events/s | Peak RSS | Parquet | Gate p50 / p95 | Publish p50 / p95 | Runs |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100,000 | 1.22 (1.22-1.26) | **81,696** | 205 MB | 4.6 MB | 22.7 / 28.5 ms | 53.8 / 65.2 ms | 3 |
| 1,000,000 | 17.01 (16.91-17.06) | **58,785** | 731 MB | 46.0 MB | 23.4 / 28.7 ms | 68.1 / 140.9 ms | 3 |
| 10,000,000 | 576.4 | **17,350** | 3,629 MB | 463.2 MB | 24.4 / 26.9 ms | 177.7 / 829.2 ms | 1 |

"Gate" = batch validation latency (parse + validate + identity + sequence checks of 5,000 events).
"Publish" = whole per-batch latency from gate start to commit (gate + stage + verify + place + commit).
The gate is flat across scales (≈ 24 ms per 5,000 events, ≈ 4.8 µs/event); end-to-end throughput falls with scale
because the **commit stage grows**:

| Events | gate | stage | verify | place | commit | other (read, batching, bookkeeping) |
|---:|---:|---:|---:|---:|---:|---:|
| 100,000 | 38% | 13% | 5% | 0% | 31% | 12% |
| 1,000,000 | 29% | 9% | 2% | 0% | 49% | 10% |
| 10,000,000 | 9% | 3% | 1% | 0% | **84%** | 3% |

(Shares of wall time, median over runs.) Parquet size is ≈ 46 bytes/event with ZSTD (vs ≈ 306 bytes of JSON in the
source log, a 6.6× reduction).

Memory scales with the number of committed events because the identity/sequence index is held in RAM
(≈ 360 bytes/event at 10M including the baseline). That is the design limit stated in
[architecture.md §9](architecture.md); lookup-on-demand against SQLite is the obvious next step and was not built.

### Open item: why commit grows at 10M

At 10M events the commit transaction (inserting 5,000 rows into `event_index`: a unique 16-byte key index and a
unique `(device_id, sequence_number)` index) takes 84% of the time. I tried the obvious knob once: raising the
SQLite page cache from 128 MB to 2 GiB (`TELEMETRY_SQLITE_CACHE_MB=2048`, label `scale-10M-sqlite-cache-2048MB`):

| SQLite cache | Events/s | Wall s | Peak RSS | Publish p95 |
|---:|---:|---:|---:|---:|
| 128 MB (default) | 17,350 | 576.4 | 3,629 MB | 829 ms |
| 2,048 MB | 12,955 | 771.9 | 5,718 MB | 1,389 ms |

The larger cache was **slower** (one run each, so the size of the effect is uncertain, but it did not help). So the
growth is *not* explained by cache misses alone. Plausible but **untested** causes are write amplification from WAL
checkpoints of scattered B-tree pages and the cost of checkpointing a very large WAL; I did not profile this further.
The 128 MB default stays.

## 3. What was optimised, and what it was worth

Method: baseline run → `cProfile` and per-stage timers at 500k events → fix the largest cost → re-measure.

**Baseline** (`benchmarks/results/20261019-194408-baseline-pre-optimization.json`, code as of Milestone 5):

| Events | Events/s | Wall s | Peak RSS | gate / stage / verify / commit share |
|---:|---:|---:|---:|---|
| 100,000 | 54,095 | 1.85 | 181 MB | 29% / 9% / 3% / **48%** |
| 1,000,000 | 29,441 | 33.97 | 572 MB | 17% / 5% / 1% / **70%** |

Profile at 500k events: `commit` was **61% of wall time** (94 ms per batch and growing with table size), the gate 18%.
The SQLite `event_index` was the cause. Isolated micro-benchmark
([`benchmarks/sqlite_event_index_bench.py`](../benchmarks/sqlite_event_index_bench.py), 1M rows, 5,000/txn, one run per row):

| Layout / settings | Total | Batch time early → late |
|---|---:|---:|
| v1: `TEXT` primary key, `WITHOUT ROWID`, SQLite defaults (before) | 20.6 s | 34.5 → 147.2 ms |
| rowid table + 16-byte hashed-key unique index | 15.7 s | 15.5 → 125.7 ms |
| … + 128 MB page cache | 11.4 s | 15.1 → 87.5 ms |
| … + `wal_autocheckpoint=20000` (now) | **6.3 s** | 15.1 → 44.8 ms |

**Changes made** (each re-checked against the full test suite; 348 tests pass):

1. `event_index` redesign (manifest schema v2): clustered rowid, `event_key = blake2b-128(event_id)` as the unique
   index, original `event_id` kept as an ordinary column. Trade-off: a hash collision between two distinct event IDs
   would raise a `CommitConflictError` (fail-safe), never merge events silently.
2. SQLite tuning: page cache 128 MB (`TELEMETRY_SQLITE_CACHE_MB`) and `wal_autocheckpoint` 20,000 pages
   (`TELEMETRY_SQLITE_WAL_PAGES`). `synchronous=FULL` is **unchanged** — durability was not traded for speed. A larger
   WAL means crash recovery replays up to ~80 MB of WAL.
3. Gate: timestamps converted to microseconds once per event instead of seven times; the identity fingerprint takes the
   already-computed value; batch input digest hashes one joined buffer instead of three updates per record.
4. `event_index(batch_id)` index and a single grouped scan in `check_consistency`, which had been scanning the whole
   index once per committed batch (found by the recovery benchmark below).

**Result** (3 repeats each, same machine):

| Events | Before | After | Change |
|---:|---:|---:|---:|
| 100,000 | 54,095 events/s | 81,696 events/s | **1.5×** |
| 1,000,000 | 29,441 events/s | 58,785 events/s | **2.0×** |
| 1,000,000 wall time | 33.97 s | 17.01 s | −50% |
| 1,000,000 peak RSS | 572 MB | 731 MB | +159 MB (page cache) |

The remaining distribution at 1M is gate 29% / commit 49%. Not attempted: parallel validation across processes,
vectorised (Arrow-compute) validation with a row-level fallback, or fewer/larger SQLite transactions.

## 4. Batch size sweep (1M events, ZSTD, 3 repeats)

| Batch size | Wall s | Events/s | Peak RSS | Parquet | Files | Stage MB/s | Publish p50 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 25.65 (25.52-26.38) | 38,992 | 690 MB | 50.4 MB | 1,009 | 20 | 19.6 ms |
| 5,000 | 17.80 (17.16-18.11) | 56,194 | 730 MB | 46.0 MB | 211 | 28 | 71.3 ms |
| 20,000 | 15.08 (14.77-15.33) | 66,308 | 847 MB | 44.0 MB | 61 | 28 | 238.2 ms |
| 100,000 | 13.49 (13.39-13.62) | 74,148 | 1,258 MB | 43.0 MB | 21 | 25 | 1,135.2 ms |

Larger batches amortise per-batch costs (transactions, fsyncs, file creation) and give better compression, but
increase per-batch latency (time before a batch becomes visible), peak memory, and the amount of work lost to a crash.
Batch 5,000 is the default; going from 5,000 to 100,000 buys ≈ 32% throughput for ~16× the publish latency and 1.7× the
memory. (The 5,000 row differs from §2 by ~5% — the same noise band noted above.)

## 5. Compression sweep (1M events, batch 5,000, 3 repeats)

| Codec | Wall s | Events/s | Parquet | Stage MB/s | Publish p50 |
|---|---:|---:|---:|---:|---:|
| zstd | 17.42 (17.07-17.64) | 57,404 | **46.0 MB** | 28 | 68.8 ms |
| snappy | 21.32 (20.21-21.53) | 46,915 | 67.6 MB | 40 | 72.6 ms |
| lz4 | 18.67 (18.28-22.21) | 53,568 | 68.9 MB | 42 | 72.0 ms |
| gzip | 27.30 (26.74-27.49) | 36,635 | 47.2 MB | 6 | 103.4 ms |
| none | 20.01 (18.98-20.90) | 49,985 | 82.4 MB | 54 | 70.5 ms |

ZSTD (the default) gave the smallest files and, in this run, the highest end-to-end throughput; gzip is the only
codec that clearly hurts (6 MB/s through the stage). The ordering among zstd, lz4, snappy and none by throughput is
within ~15% and several ranges overlap, so I would not read more into it than "ZSTD is not slower here". The harness
does not isolate pure encode throughput; "Stage MB/s" includes everything in the stage.

## 6. DuckDB query latency (median of 5; each includes opening the connection and creating the view over all files)

| Query | 1M events (211 files) | 10M events (2,111 files) |
|---|---:|---:|
| connection + view setup only | 0.007 s | 0.030 s |
| `count(*)` | 0.013 s | 0.090 s |
| `point-lookup` (one `event_id`) | 0.015 s | 0.112 s |
| `fleet-summary` (group by day) | 0.020 s | 0.142 s |
| `battery-health` (per-day latest reading per device) | 0.026 s | 0.244 s |
| `hourly-device-health` (group by hour × device, `LIMIT 20`) | 0.041 s | 0.350 s |

All warm-ish (OS page cache; the dataset is 46 MB / 463 MB). Cold-cache disk reads were not measured. There are no
indexes; DuckDB scans Parquet column chunks and uses row-group statistics, so latency grows roughly linearly with
data (10× data → 7–10× time here).

## 7. Recovery, replay and verification

| Operation | 1M events | 10M events |
|---|---:|---:|
| `recover` after a crash in the last batch, **resume** path (adopts intact staged files, commits; plus deep check of all committed files) | **0.57 s** | not measured |
| `recover` after a crash in the last batch, **replay** path (re-runs the batch from the source; loads the identity index) | **2.15 s** | not measured |
| `replay` of **all** committed batches from the source (rebuild + byte/logical comparison) | 11.13 s (89,820 rows/s) | 488.3 s (20,478 rows/s) |
| deep file check (size + SHA-256 + Parquet metadata, all files) | 0.03 s | 0.85 s |
| `telemetry verify` (18 SQL assertions) | 1.85 s | 23.08 s |

All replayed batches were byte-identical to the committed ones at both scales; `verify` returned PASS.

Benchmarking recovery exposed a real defect: before fix 4 above, `recover` took **6.93 s** on the same 1M dataset
(resume path) and `replay` ran at 61,362 rows/s, because per-batch queries scanned the entire `event_index`
(intermediate run `20261019-200312-optimized.json`). Per-event replay cost is ≈ 4.4× higher at 10M than at 1M; I did
not profile why (the same `event_index` growth effect as §2 is a candidate, not a finding).

The recovery runs crash a worker with `TELEMETRY_FAILPOINT` in the **last** batch, so they measure one-batch recovery
plus the whole-dataset checks, not recovery of a half-finished 1M-event run. Recovery at 10M was not run.

## 8. Source generation

| Events | Seconds | Events/s | Source size |
|---:|---:|---:|---:|
| 100,000 | 0.8 | 126,389 | 30 MB |
| 1,000,000 | 7.7 | 129,625 | 306 MB |
| 10,000,000 | 79.7 | 125,540 | 3,079 MB |

Single-threaded; the simulator is ≈ 2× faster than the pipeline that consumes it, so the pipeline, not the source,
is the bottleneck.

## 9. Limitations of these measurements

* One machine, one filesystem, one Python version; no Linux numbers.
* 10M results are single runs; 100k / 1M have 3 repeats. Run-to-run spread of ~5% was observed.
* The workload is a clean synthetic stream. Fault-heavy streams (rejections, evidence files, quarantine) were not
  benchmarked; the gate does more work for rejected records.
* Throughput is single-process and single-writer by design; no parallelism was attempted.
* The cause of commit-time growth at 10M, and of the higher per-event replay cost at 10M, was not established.
* Raw results for every run are in `benchmarks/results/`: `…baseline-pre-optimization.json`, `…optimized.json`
  (intermediate), `…final.json`, `…scale-10M.json`, `…scale-10M-sqlite-cache-2048MB.json`.

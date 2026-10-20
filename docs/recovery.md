# Publication protocol, crash windows and recovery

Code: [`publisher.py`](../src/telemetry_lakehouse/publisher.py),
[`manifest.py`](../src/telemetry_lakehouse/manifest.py),
[`failpoints.py`](../src/telemetry_lakehouse/failpoints.py).

**Status.** Milestone 3 delivered *safety*: whatever happens, trusted readers see only whole committed
batches. Milestone 4 adds *recovery* (`telemetry recover`), orphan handling, deterministic replay
(`telemetry replay`) and detection/repair of damaged committed files. Ingestion still refuses to run past an
unfinished batch until `recover` has resolved it (or, for an `INTERRUPTED` batch, replays it itself).

## 1. The protocol

```
begin_batch      RECEIVED     boundaries [start, end) + input digest made durable BEFORE any work
gate             VALIDATING   (a BLOCK ends here: REJECTED + evidence, no files)
stage            STAGED       Parquet -> staging/<batch>/…, fsync, SHA-256; files recorded in SQLite
verify           VERIFIED     staged bytes re-read: hash, schema, row counts, partition, event IDs
place                         hard-link into data/ (no-clobber), fsync dirs   [files exist, unreferenced]
commit           COMMITTED    ONE SQLite transaction: state flip + commit_seq + event_index rows
cleanup                       drop staging links (best effort)
```

**The visibility boundary is the commit transaction.** `reader.visible_files` returns the files of
`COMMITTED` batches and nothing else; every query path (`telemetry query`, the property tests, and the
Milestone 5 verifier) goes through it.

**Atomicity scope, stated precisely.** The flip is atomic for *manifest-aware* readers. A reader that
globs `data/**/*.parquet` can see files that were placed but not committed, and orphans left by a crash.
This is deliberate and documented rather than hidden: filesystem renames cannot make a multi-file batch
atomic, and the project explicitly avoids pretending otherwise.

**Why place-then-commit (not commit-then-place).** A committed manifest row must never reference a file
that is not there (P2). Placing first means the worst crash outcome is an *unreferenced* file, which is
harmless to readers and cleanly identifiable (it has no manifest reference). Commit-then-place would make
the worst outcome a *dangling reference*. `tests/unit/test_manifest.py` and a mutation run (commit
moved before place) confirm the suite catches the wrong order.

**Enforced in the database, not just in Python** (`CHECK`/`UNIQUE`/`PRIMARY KEY`):
state ∈ the seven states; every transition is a compare-and-set (`UPDATE … WHERE state = ?`); a
`COMMITTED` row needs `commit_seq` and `committed_rows = accepted` (A3); a non-committed row has neither;
`event_index` rejects a second row for the same `event_id` (I1) or `(device_id, sequence_number)` (I2);
`batch_files.relative_path` is unique.

## 2. State machine

```
RECEIVED → VALIDATING → STAGED → VERIFIED → COMMITTED          (terminal)
              │            │         │
              └────────────┴─────────┴──→ REJECTED   → RECEIVED   (resubmission = new attempt)
 any in-flight state ──→ INTERRUPTED (set by recovery) → RECEIVED (new attempt)
```

`REJECTED` is terminal *for that attempt*; resubmitting the interval (e.g. after the source was fixed)
starts attempt `n+1`. `COMMITTED` is terminal for good: re-submitting it is a no-op, and if the source
bytes at those offsets have changed (input digest differs) the batch is blocked as `SOURCE_MUTATED` — the
same interval can never produce two different committed outputs.

## 3. Crash windows

Each window has a named failpoint. Tests crash there **in-process** (`SimulatedCrash`, rolls back) and
as a **real process kill** (`os._exit(137)`: no `finally`, no rollback code, only SQLite WAL recovery).
Both are in `tests/failure_injection/test_crash_windows.py` and assert the persisted state, not mocks.

| # | Window | Failpoint | Manifest state afterwards | On disk | Visible rows | Retry without recovery | `telemetry recover` |
|---|---|---|---|---|---|---|---|
| 1 | Before any file is written | `before_stage` | `VALIDATING` | nothing | prior batches only | `InFlightBatchError` | Mark `INTERRUPTED`, then **replay** the recorded interval from the source. |
| 2 | During Parquet writing | `write_tmp_partial` | `VALIDATING` | hidden `.tmp` (half a file) in `staging/` | prior only | `InFlightBatchError` | Mark `INTERRUPTED`; `staging/<batch>` (with the torn temp file) moves to `rejected/<batch>/attempt-N/staging-remnants/`; **replay**. |
| 3a | Staged, before placement | `after_stage` | `STAGED` (files + SHA recorded) | complete files in `staging/` | prior only | `InFlightBatchError` | Recorded SHA-256s and content digest verify ⇒ **resume** (place, commit). Any mismatch ⇒ evidence + replay. |
| 3b | Placed, before commit | `after_place` | `VERIFIED` | files in `staging/` **and** `data/` (unreferenced) | prior only | `InFlightBatchError` | **Resume** iff each file has an intact copy in `staging/` or `data/`; a damaged placed copy is moved to evidence and replaced from the intact staged copy. Otherwise replay. |
| 4 | During the metadata commit | `in_commit` | `VERIFIED` (txn rolled back) | as 3b | prior only | `InFlightBatchError` | Same as 3b. The transaction is all-or-nothing: no batch row, file row or `event_index` row leaked. |
| 5 | After commit, before the producer is told | `after_commit` | `COMMITTED` | complete (staging links may remain) | prior **+ this batch** | Batch recognised as `COMMITTED` and skipped; later batches proceed; no duplicates | Nothing to repair; leftover staging links are removed after checking the `data/` copies. |

Recovery never trusts file *existence*; it trusts only `(path, size, SHA-256)` recorded in SQLite.

## 4. Recovery algorithm (implemented in `recovery.py`)

**Resume-or-replay rule, per unfinished batch** (a pure function of durable state):

* **Resume** iff the batch is `STAGED`/`VERIFIED`, **every** recorded file has an intact copy (size and SHA-256
  match) in `staging/` or `data/`, the event count read back from those files equals the recorded `accepted`,
  and their recomputed **logical digest** equals the one recorded when the gate approved the batch. Then: move any
  damaged unreferenced copy in `data/` to evidence, finish placement, run the commit transaction. No source needed.
* **Replay** otherwise: `mark_interrupted` (durably logged in `attempt_log` with the state at the crash), the
  staging directory is preserved under `rejected/<batch>/attempt-N/staging-remnants/` with an `interrupted.json`,
  and the batch is re-run through the *same* `Pipeline` over its **recorded** `[start, end)` - so a
  time-flushed batch replays with its original boundaries.

**Run order** (each step is itself re-runnable; crashing inside recovery is tested):
1. resume or interrupt each in-flight batch → 2. **orphan sweep** → 3. replay `INTERRUPTED` batches (needs the
source) → 4. committed-file verification (+ optional `--repair`).

**Orphans.** Any file in `data/` not referenced by a committed batch is *moved* to `rejected/orphans/<path>`
(never deleted - it is evidence, and moving it also protects glob readers afterwards). Torn `.tmp` files are
deleted. Staging directories of committed batches (window-5 leftovers) are removed once the `data/` copies verify;
other staging directories are preserved as evidence. The sweep must precede replay: a stale placed file would
make the no-clobber placement of the retried batch fail.

**Committed-file verification** compares every committed file with the manifest: existence, size, SHA-256,
Parquet readability and row count (`MISSING`, `SIZE_MISMATCH`, `SHA_MISMATCH`, `UNREADABLE`, `ROWS_MISMATCH`), plus
cross-table checks (`committed_rows` = `event_index` rows = file rows per batch; no `event_index` rows from
non-committed batches). Damage is *reported* (exit 3), never silently masked.

**Repair (`--repair`)** rebuilds a damaged file by deterministic replay: re-validate the batch's source records,
keep exactly the events the manifest says were published (fingerprint-matched), write with the same code - and
install the result **only if its SHA-256 equals the committed one**. The damaged copy is kept in
`rejected/orphans/`. If the source changed or the bytes differ (e.g. another codec), the file is left alone and
reported unresolved.

## 5. Deterministic replay (`telemetry replay`)

`rebuild_batch` re-derives a committed batch from the source in a scratch directory; `replay_batch` compares
files and the logical digest with the manifest and classifies the result: *identical*, *same logical content but
different bytes* (library/version drift), or *content differs* (the source no longer reproduces the batch).
Tested: replay of batches that survived duplicates, reordering, conflicts, malformed rows and quarantine is
identical; two independent runs have identical `output_digest`/`logical_digest` per batch; a crash + recovery run
has digests identical to a run with no crash; replay never writes to `data/`.

## 6. Idempotent retries

* Re-submitting a committed batch is a no-op (window 5 test: after a crash post-commit, the retry skips
  batches 1–2, commits only batch 3, and `event_index` has no duplicates).
* If batch boundaries differ between runs, batch IDs differ; the event-level gate then treats every
  re-delivered event as an identical duplicate (`test_rerunning_with_different_batch_boundaries_…`).
* The resume point is derived from the manifest (`telemetry ingest --resume` starts at the first offset not
  covered by an unbroken chain of committed batches), never from producer memory.

## 7. What is and isn't claimed

* ✅ **Safety under crashes (tested):** at all six windows, readers see only committed batches, identities
  are committed with their batch or not at all, and retrying a committed batch never duplicates it.
* ✅ **Recovery converges (tested, in-process and after real `os._exit(137)` kills):** crash → `recover` →
  `ingest` ends with the same committed batches and byte-identical Parquet as a run with no crash, no orphans,
  and an empty staging area. Recovery is idempotent and survives a crash of its own.
* ✅ **Idempotent publication per batch interval** for a replayable, offset-addressable source; the same
  interval can never yield two different committed outputs (`SOURCE_MUTATED` is refused).
* ✅ **Damage to committed files is detected and, when replay reproduces the bytes, repaired.**
* ❌ **Not claimed:** end-to-end exactly-once from device to dashboard. Device→source is at-least-once and
  de-duplicated by `event_id`. Recovery assumes the source is retained and append-only; without it only
  resumable batches can be recovered and the rest are reported unresolved.
* ❌ **Byte-identical replay depends on library versions.** The Parquet writer's output is deterministic for a
  fixed pyarrow version; a different version may produce a logically identical file with different bytes, which
  replay reports as such and repair refuses to install.
* Single writer assumed; `BEGIN IMMEDIATE` serialises accidental concurrent writers but no multi-process
  coordination (leases, fencing) exists, and `recover` must not run concurrently with `ingest`.
* `synchronous=FULL` + `fsync` of files and directories gives durability on a local POSIX filesystem; the
  tests simulate process death, not power loss or a lying disk.

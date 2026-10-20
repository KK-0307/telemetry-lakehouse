# Correctness: the integrity gate

What the gate guarantees, how it decides, and — equally important — what it does **not** prove.
Code: [`validation.py`](../src/telemetry_lakehouse/validation.py),
[`index.py`](../src/telemetry_lakehouse/index.py), [`evidence.py`](../src/telemetry_lakehouse/evidence.py).
Status: gate implemented in Milestone 2; publication atomicity ([recovery.md](recovery.md)) in Milestone 3; independent SQL verification (§7) in Milestone 5.

## 1. Record classification

Every source record in a batch lands in **exactly one** bucket:

| Bucket | Meaning | Published? |
|---|---|---|
| `accepted` | valid, new, consistent | yes (unless the batch is blocked) |
| `duplicates` | identical re-delivery of an event already seen in this batch or already committed | no — counted, benign |
| `rejected` | contract violation, identity conflict, future clock, or late under `late_policy=reject` | no — evidence kept |
| `unresolved` | valid but held (late under `late_policy=hold`) | no — evidence kept |

### Accounting invariants (checked on every batch; re-asserted in persisted reports and tests)

* **A1** `consumed == end_offset − start_offset == len(batch)`
* **A2** `consumed == accepted + duplicates + rejected + unresolved`
* **A3** admitted batch: `committed == accepted`; blocked batch: `committed == 0`

An A1/A2 violation means the gate itself is wrong for that batch, so it **blocks in every mode**
(`critical_failures` in the report). A3 is additionally enforced by a `CHECK` constraint in the manifest
schema. A4/A5 (manifest vs. physical Parquet row counts, offset tiling) are asserted independently by the
SQL verifier (Milestone 5).

## 2. Reason codes

| Code | Trigger |
|---|---|
| `MALFORMED_PAYLOAD` | not valid JSON, or not a JSON object |
| `MISSING_FIELD` / `UNKNOWN_FIELD` | required field absent / unexpected field present |
| `INVALID_TYPE` | wrong JSON type (no coercion), unknown `status` |
| `OUT_OF_RANGE` | measurement outside its closed range, NaN/Inf |
| `INVALID_TIMESTAMP` | missing/naive/unparseable timestamp |
| `UNSUPPORTED_SCHEMA_VERSION` | `schema_version ∉ {1}` |
| `FUTURE_EVENT_TIME` | `event_time` more than `max_future_skew` ahead of `ingestion_time` (device clock ahead) |
| `LATE_EVENT` | late and `late_policy=reject` |
| `CONFLICTING_DUPLICATE` | same `event_id`, different payload — in the batch or vs. committed data |
| `SEQUENCE_CONFLICT` | same `(device_id, sequence_number)` claimed by two different `event_id`s |

## 3. Identity rules

* **Payload identity** is the *device-originated* content: `device_id, sequence_number, event_time,
  schema_version`, the four measurements and `status`. `ingestion_time` is excluded because it is the
  platform's receipt stamp — a legitimate re-delivery differs only there.
* Same `event_id` + same payload ⇒ **duplicate** (benign). Different payload ⇒ **conflict**.
* On an in-batch conflict **both** copies are rejected: the gate cannot know which one the device meant,
  and trusting the first-arrived would make correctness depend on delivery order. Against *committed* data,
  only the new copy is rejected; the committed original is never modified (files are immutable).
* Consequence worth knowing: if a conflicting twin arrives in a *later* batch after its original was in a
  **blocked** batch, the original is not committed, so the twin is accepted as a first sighting.
  Strict blocking is conservative per batch, not a global conflict oracle.

## 4. Sequence integrity

Batch boundaries are arbitrary, so sequence checks are **not** performed per batch. The tracker keeps,
per device, `contiguous_through` (everything `1..n` present) and the set of numbers seen above a gap.

* Repeated / conflicting sequence numbers are critical (see `SEQUENCE_CONFLICT`, duplicates above).
* Out-of-order arrival is normal: it is counted (`out_of_order`) and never blocks.
* A temporarily open gap is **not** data loss. A gap becomes *proven missing* only by one of:
  * **End-of-stream marker** — the source declares `complete: true` plus each device's final sequence
    number. Then every missing number up to that final sequence, including trailing loss, is proven lost.
  * **Watermark** — the event just above the gap arrived at least `gap_grace_seconds` before the stream
    watermark (max `ingestion_time` seen), so a late arrival has had its chance. Weaker: a very late
    event can still arrive after the grace period.
* An interrupted source (`interrupt_after`) gets **no** marker, so only the watermark rule applies.
* Proven gaps do not block batches (the events that *did* arrive are valid). They are reported in the run
  summary and in `meta/completeness-<source_id>.json`, and make `telemetry ingest` exit `3`. A dataset
  with proven gaps is internally consistent but **known to be incomplete**.

## 5. Admission: whole-batch rejection vs. row-level quarantine

| | `strict` (default) | `quarantine` |
|---|---|---|
| Any rejected/unresolved record | blocks the **entire** batch | set aside; the rest publishes |
| Unit of trust | the batch | the row |
| Published data after a fault | exactly the admitted batches — a bad batch leaves no trace in `data/` | complete except for the quarantined rows |
| Evidence | `rejected/<batch>/attempt-N/report.json`; the source still holds every row at the recorded offsets | the same, plus `quarantine.jsonl` with every set-aside payload (full, base64) |
| Use when | a batch is one upload/transaction and partial acceptance would mislead | rows are independent and availability matters more than completeness |

`on_block=halt` (default) stops at the first blocked batch: nothing after it is published, so the offset
frontier cannot silently skip a batch. `continue` keeps going and leaves a hole that completeness
reporting will flag as sequence gaps.

## 6. What was injected → what is found

Verified by `tests/integration/test_integrity_gate.py` against exact ground truth from the fault ledger:

| Injected | Detected as | Blocks (strict)? |
|---|---|---|
| duplicate delivery | `duplicates` (count == injected, across batch boundaries too) | no |
| reorder / delay | `out_of_order`, `late`; **published == reference** | no |
| dropped event | proven sequence gap (set of `(device, seq)` == dropped set) | no (exit 3 at run end) |
| conflicting duplicate | 2 × `CONFLICTING_DUPLICATE` | yes |
| malformed payload | `MALFORMED_PAYLOAD` | yes |
| invalid value | `OUT_OF_RANGE` | yes |
| unsupported version | `UNSUPPORTED_SCHEMA_VERSION` | yes |
| clock skew (future) | `FUTURE_EVENT_TIME` | yes |
| clock skew (past) | `late` → policy `accept`/`reject`/`hold` | per policy |
| source interruption | no end-of-stream marker; watermark-only completeness | no |

Test strength was checked by mutation: disabling strict blocking, skipping the index update after publish,
and treating conflicts as duplicates each make the suite fail.

## 7. Independent SQL verification (`telemetry verify`)

The gate and the recovery code *claim* the dataset is sound. `verify` re-derives that from first principles:
it reads only the Parquet files that committed manifest rows reference, loads the manifest tables next to
them in an in-memory DuckDB, and runs 18 assertions from [`sql/assertions/`](../src/telemetry_lakehouse/sql/assertions).
Each assertion is a query returning **violating rows**; zero rows = PASS. It shares no validation code with
the writer (only the documented range constants), and does not trust the manifest's hashes: the content
assertions run on the bytes actually on disk.

| Assertion | Category | Proves |
|---|---|---|
| `unique_event_id` | identity | I1 - no logical event published twice |
| `unique_device_sequence` | identity | I2 - one event per (device, sequence) |
| `required_fields_complete` | contract | no NULL / empty required field |
| `valid_measurement_ranges` | contract | closed ranges hold; NaN/Inf rejected |
| `valid_enums_and_versions` | contract | known `status`, supported `schema_version`, `sequence_number ≥ 1` |
| `event_time_not_in_future` | contract | no event further ahead of its receipt than the skew allowance |
| `parquet_schema_matches_contract` | contract | every file: exact columns, REQUIRED, expected physical types; dataset has the expected logical types |
| `partition_matches_event_time` | partitioning | `event_date=` equals the UTC date of `event_time` |
| `batch_accounting_identity` | reconciliation | A1, A2, A3 from the manifest's persisted counters |
| `batch_row_counts_reconcile` | reconciliation | A4 - Parquet rows = manifest file rows = `event_index` rows = `committed_rows` |
| `no_untrusted_batches_in_data` | trust | P3 - no row from a rejected / interrupted / unknown batch |
| `events_and_index_agree` | trust | `event_index` and Parquet describe the same events and batches |
| `no_unreferenced_files_in_data` | trust | `data/` holds nothing a glob reader would wrongly trust |
| `manifest_files_match_physical` | consistency | P2 - each file exists, is readable, matches recorded size, SHA-256, rows, time bounds |
| `no_unfinished_batches` | consistency | publication state is settled (else: `telemetry recover`) |
| `sequence_gaps_beyond_grace` | completeness | S2 (watermark) - no gap provably permanent by the grace rule |
| `sequences_complete_to_end_of_stream` | completeness | S2 (marker) - each device has exactly `1..final` (skipped without a marker) |
| `offset_coverage_no_holes` | completeness | A5 - committed batches cover each source's offsets from 0 |

**Statuses.** `PASS`, `FAIL`, `SKIP` (precondition absent - e.g. an interrupted source has no end-of-stream
marker; reported, never counted as a pass) and `WARN`. Exit code `3` iff any `FAIL`. An assertion that cannot
be evaluated is a `FAIL`, never a pass.

**Completeness vs soundness.** Content/identity/consistency failures mean the dataset is *wrong*. Completeness
failures mean it is *sound but known to be missing data* (dropped events, a rejected batch, an unfinished tail).
`--allow-incomplete` downgrades only the completeness category to `WARN`, e.g. for quarantine-mode datasets;
it never hides a content failure. The report is also written to `meta/verify-report.json`.

**How strong is the evidence?** Every one of the 18 assertions was neutered in turn and the suite failed each
time, i.e. none is vacuous. Targeted corruptions injected behind the pipeline (duplicate IDs, NULLs, NaN/Inf,
out-of-range values, bad enums, future timestamps, misfiled partitions, type drift, extra columns, broken
accounting, mismatched counts, rejected-batch data in trusted files, index/Parquet disagreement, missing /
truncated / corrupt / swapped files, stray files, unfinished batches, dropped events, tail loss, offset holes) each
fail the intended assertion - including a swapped file whose manifest hash was patched to match.

**What it does not prove.** It cannot show the *source* was complete beyond what the end-of-stream marker
states; it trusts DuckDB's Parquet reader and the SHA-256 recorded at commit; and it checks the dataset as it is
now, not that every intermediate state was safe (that is the crash-window tests' job).

## 8. Known limitations

* The committed index is an in-memory cache warmed from the manifest's `event_index` (uniqueness itself is
  enforced by SQLite's PRIMARY KEY at commit). Start-up is O(committed events).
* Gaps declared lost are not persisted across runs; a later run re-derives them from the data.
* Late-event and skew thresholds are static; there is no per-device clock model.
* `unresolved` can only arise from `late_policy=hold`; there is no operator workflow to resolve held rows yet.
* Completeness over the *whole stream* needs the source's end-of-stream marker; a real source that cannot
  issue one only ever gets watermark-grade (probabilistic) completeness.
* Strict blocking is per batch and conservative, not a global conflict oracle (see §3).
* `device_id` is a single global namespace: the same device appearing in two sources would collide on
  `(device_id, sequence_number)` (enforced by `event_index` and checked by `verify`).

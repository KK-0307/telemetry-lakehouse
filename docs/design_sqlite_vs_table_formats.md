# Design note: SQLite manifest vs. Iceberg / Delta Lake

**Status:** decided, implemented. **Scope:** how committed state is represented and made visible.
**Evidence level:** the SQLite design is implemented, tested and benchmarked in this repository. Iceberg and Delta
Lake were **not** implemented or benchmarked here; their side of the comparison rests on their documented design
and my understanding of it, and is marked as such.

## 1. Problem

The pipeline must make a *multi-file* batch visible atomically to analytical readers, keep rejected and
interrupted batches out of the trusted dataset, survive crashes at any step, and let an independent verifier
prove what was committed. A directory of Parquet files cannot do this: renames and file creation are per-file, so
a reader that lists the directory can observe half a batch. Something must be the single source of truth for
"which files are committed". The question is what.

## 2. Requirements that drove the choice

| # | Requirement | Why it matters here |
|---|---|---|
| R1 | Atomic visibility of N files + their metadata | The central correctness property (P1) |
| R2 | Batch identity and source offsets stored **in the same transaction** as the commit | Idempotent retries, replay over recorded boundaries |
| R3 | Enforced uniqueness of `event_id` and `(device_id, sequence_number)` at commit | Last line of defence for I1/I2 under any bug or race |
| R4 | Persist rejection reasons, accounting counters, attempt history | Evidence and reconciliation, not just file lists |
| R5 | Cheap to run and to inspect; deterministic in tests; no services | A portfolio-grade system must be reproducible by a reviewer with one command |
| R6 | A clean seam for a later migration | The architecture should not paint itself into a corner |

Single writer, local filesystem, one engine (DuckDB) reading — the stated scope.

## 3. Options considered

**A. Directory convention only** (e.g. write files, then a `_SUCCESS` marker; readers glob). Rejected: markers are
per-directory, readers can still list uncommitted files, and nothing enforces uniqueness or records evidence.

**B. SQLite manifest (chosen).** One database holds `batches` (state machine, offsets, accounting, digests),
`batch_files` (path, size, SHA-256, row count, time bounds) and `event_index` (identity uniqueness). A batch becomes
visible when one `BEGIN IMMEDIATE … COMMIT` flips it to `COMMITTED`. Readers ask the manifest for the file list.

**C. Apache Iceberg.** A table format with snapshot metadata (metadata file → manifest list → manifests → data
files); a commit atomically swaps the table's metadata pointer through a catalog. Strengths: engine interoperability
(Spark, Trino, Flink, DuckDB readers), snapshot isolation, time travel, schema and partition evolution, row-level
deletes, compaction/maintenance procedures. It could be driven from Python with a SQL catalog (which could itself
be SQLite).

**D. Delta Lake.** A transaction log (`_delta_log`) of ordered JSON commits plus checkpoints; a commit is an atomic
create of the next log file (put-if-absent). Strengths: similar to Iceberg, plus built-in idempotent-writer
markers (transaction identifiers per application) that map naturally to our batch identity. Python access through
delta-rs.

## 4. Comparison

| | B. SQLite manifest | C. Iceberg | D. Delta |
|---|---|---|---|
| R1 atomic multi-file visibility | ✅ one transaction (tested with real process kills) | ✅ catalog pointer swap | ✅ log-file put-if-absent |
| R2 batch identity + offsets with the commit | ✅ native columns, same transaction | ⚠️ must be carried in snapshot properties/summary | ✅ per-app transaction id/version, offsets via commit info |
| R3 uniqueness at commit | ✅ `UNIQUE` / PK constraints | ❌ no constraints; dedupe needs a merge or an external index | ❌ same (constraints are limited to CHECK-style) |
| R4 rejections, accounting, attempts | ✅ ordinary tables | ❌ outside the table; needs a side store | ❌ same |
| R5 operational cost | ✅ stdlib module, one file, inspectable with `sqlite3` | ⚠️ library + catalog (+ maintenance of snapshots/manifests) | ⚠️ library + log/checkpoint maintenance |
| Multi-writer | ❌ single writer | ✅ optimistic concurrency | ✅ optimistic concurrency |
| Multi-engine reads | ❌ our `reader.py` only | ✅ | ✅ |
| Time travel / evolution / row deletes | ❌ | ✅ | ✅ |
| Object-store commits | ❌ local FS design | ✅ (catalog-mediated) | ✅ (store must offer atomic create) |

(C/D cells reflect each format's documented design; none was run here.)

The decisive point is **R3 + R4**. Table formats make *file* visibility atomic, but they do not give this pipeline
its integrity state: unique-key enforcement, per-batch accounting, rejection evidence and attempt history. We would
still need a transactional store next to the table — most likely the same SQLite database — and then have **two**
commit points to keep consistent (table commit and index/evidence commit), turning a one-transaction story into a
two-phase one. For a single-writer pipeline that is more machinery with fewer guarantees, and it would obscure the very
mechanism (single transactional visibility boundary) the project exists to demonstrate and test.

## 5. Decision

Use a SQLite manifest. Keep migration cheap by construction:

* All trusted reads go through one function, `reader.visible_files(layout)`; nothing else decides what is visible.
* The manifest tables are shaped like a table format's metadata: `batches` ≈ snapshots/commits, `batch_files` ≈
  manifests, with per-file size, SHA-256, row counts and event-time bounds already recorded.
* Data files are immutable, Hive-partitioned Parquet with an explicit schema — directly adoptable by either format.
* `StorageLayout` and `JsonlSource` are the only places that know about the local filesystem.

## 6. Consequences

Positive: one transaction is the whole publication story and is exercised by crash tests; schema-level `CHECK` and
`UNIQUE` constraints back the Python logic; the verifier can load the manifest into DuckDB and cross-check it
against the Parquet; zero services.

Negative / accepted (all measured or documented elsewhere in this repo):

* **Single writer.** `BEGIN IMMEDIATE` serialises writers on one host; there is no lease or fencing.
* **Only manifest-aware readers are protected.** A glob reader can see unreferenced files until `recover` moves them.
* **The identity index scales with the dataset.** At 10M events the in-memory cache costs ≈ 3.6 GB RSS and commit
  time dominates (17k events/s vs 59k at 1M); the cause of that growth is not yet established
  ([benchmarks.md](benchmarks.md)). SQLite is not the limit in principle, but this design is not tuned for 10⁹ events.
* **Small files.** One file per `(batch, date)`; no compaction. Table formats ship maintenance for this.
* **No time travel, schema evolution or row-level deletes.**

## 7. When a full transactional table format becomes necessary

Adopt Iceberg or Delta when **any** of the following becomes a requirement, not merely a convenience:

1. **Concurrent writers** — several ingesters, or ingesters on several hosts, committing to the same dataset.
2. **Multiple engines** — Spark/Trino/Flink or other teams must read the same table with consistent, documented
   semantics rather than through `reader.py`.
3. **Object storage** — S3/GCS/ADLS-style stores where our hard-link + `fsync` placement does not exist; commits
   need the store's conditional-write primitives or a catalog.
4. **Historical queries and audit** — "what did the table look like at commit N" as a feature, not a debugging aid.
5. **Evolution and mutation** — partition or schema evolution without rewrites, GDPR-style deletes, or late
   corrections that rewrite files rather than append.
6. **Metadata scale** — enough files/rows that one SQLite file and an in-memory identity index are the bottleneck
   (this repo's measured symptom starts to show in the tens of millions of events).
7. **Maintenance at scale** — automated compaction, snapshot expiry and orphan-file cleanup.

## 8. Migration sketch (not implemented)

1. Keep SQLite as the **pipeline-state store** (gate, accounting, evidence, `event_index`) — table formats do not
   replace it.
2. Replace `Manifest.commit` + `reader.visible_files` with an append to the table, carrying `batch_id`, source id
   and offsets in the commit metadata (Delta's per-application transaction id is the closest native fit; Iceberg
   snapshot properties otherwise) so idempotent retry stays a metadata lookup.
3. Make the table commit the visibility boundary and the SQLite state change a *consequence* of it, with recovery
   reconciling the two (the crash windows in [recovery.md](recovery.md) map one-to-one: stage → place → table commit).
4. Re-point `verify` at the table's file list; the SQL assertions themselves do not change.

The cost of this migration is dominated by item 3 — deciding which store is authoritative for each fact — not by
file layout, which is why that decision is recorded here before it is needed.

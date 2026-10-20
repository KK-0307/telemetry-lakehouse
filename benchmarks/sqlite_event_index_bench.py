"""Micro-benchmark that motivated the `event_index` redesign (see docs/benchmarks.md).

Inserts N synthetic events (random UUID ids, 1,000 devices) into SQLite in transactions of 5,000 rows,
the way the commit transaction does, and reports total time plus per-batch time early and late in the
run. Variants differ only in table layout and PRAGMAs. Run:

    uv run python benchmarks/sqlite_event_index_bench.py [N]     # default 1,000,000

Single run per variant: treat differences under ~10% as noise.
"""

from __future__ import annotations

import os
import random
import sqlite3
import sys
import tempfile
import time
import uuid

N = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
BATCH = 5000

OLD = (
    "CREATE TABLE event_index (event_id TEXT PRIMARY KEY, device_id TEXT NOT NULL, "
    "sequence_number INTEGER NOT NULL, fingerprint BLOB NOT NULL, ingestion_us INTEGER NOT NULL, "
    "batch_id TEXT NOT NULL, UNIQUE (device_id, sequence_number)) WITHOUT ROWID, STRICT"
)
NEW = (
    "CREATE TABLE event_index (id INTEGER PRIMARY KEY, event_key BLOB NOT NULL UNIQUE, "
    "event_id TEXT NOT NULL, device_id TEXT NOT NULL, sequence_number INTEGER NOT NULL, "
    "fingerprint BLOB NOT NULL, ingestion_us INTEGER NOT NULL, batch_id TEXT NOT NULL, "
    "UNIQUE (device_id, sequence_number)) STRICT"
)
OLD_INSERT = "INSERT INTO event_index VALUES (?, ?, ?, ?, ?, ?)"
NEW_INSERT = (
    "INSERT INTO event_index (event_key, event_id, device_id, sequence_number, fingerprint, "
    "ingestion_us, batch_id) VALUES (?, ?, ?, ?, ?, ?, ?)"
)


def make_rows() -> list[tuple]:
    rng = random.Random(1)
    seq = [0] * 1000
    rows = []
    for i in range(N):
        d = rng.randrange(1000)
        seq[d] += 1
        rows.append((str(uuid.UUID(int=rng.getrandbits(128))), f"dev-{d:07d}", seq[d],
                     os.urandom(12), 1_000_000 + i, "b"))  # fmt: skip
    return rows


def run(
    name: str,
    ddl: str,
    insert: str,
    rows: list[tuple],
    *,
    cache_kb: int | None,
    wal_pages: int | None,
    keyed: bool,
):
    con = sqlite3.connect(os.path.join(tempfile.mkdtemp(), "x.db"), isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    if cache_kb:
        con.execute(f"PRAGMA cache_size={-cache_kb}")
    if wal_pages:
        con.execute(f"PRAGMA wal_autocheckpoint={wal_pages}")
    con.execute(ddl)
    import hashlib

    times = []
    for i in range(0, N, BATCH):
        chunk = rows[i : i + BATCH]
        if keyed:
            chunk = [(hashlib.blake2b(r[0].encode(), digest_size=16).digest(), *r) for r in chunk]
        t = time.perf_counter()
        con.execute("BEGIN IMMEDIATE")
        con.executemany(insert, chunk)
        con.execute("COMMIT")
        times.append(time.perf_counter() - t)
    k = max(1, len(times) // 10)
    print(f"{name:<52} total {sum(times):6.1f}s | first {k} batches {sum(times[:k]) / k * 1e3:6.1f} ms "
          f"| last {k} {sum(times[-k:]) / k * 1e3:6.1f} ms", flush=True)  # fmt: skip


if __name__ == "__main__":
    print(f"{N:,} rows, {BATCH:,} per transaction, SQLite {sqlite3.sqlite_version}")
    rows = make_rows()
    run(
        "v1: TEXT PK, WITHOUT ROWID, defaults (before)",
        OLD,
        OLD_INSERT,
        rows,
        cache_kb=None,
        wal_pages=None,
        keyed=False,
    )
    run(
        "v2 layout only: rowid + 16-byte key index",
        NEW,
        NEW_INSERT,
        rows,
        cache_kb=None,
        wal_pages=None,
        keyed=True,
    )
    run(
        "v2 layout + 128 MB cache",
        NEW,
        NEW_INSERT,
        rows,
        cache_kb=128 * 1024,
        wal_pages=None,
        keyed=True,
    )
    run("v2 layout + 128 MB cache + wal_autocheckpoint=20000 (now)", NEW, NEW_INSERT, rows, cache_kb=128 * 1024, wal_pages=20000, keyed=True)  # fmt: skip

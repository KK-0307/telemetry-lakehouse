# Data Contract — Telemetry Event v1

Source of truth in code: [`schemas.py`](../src/telemetry_lakehouse/schemas.py) (`TelemetryEvent`,
`EVENT_SCHEMA`). Enforced by `tests/unit/test_schemas.py`.

## Fields

| Field | Arrow type | Null | Rule |
|---|---|---|---|
| `event_id` | `string` | no | 1–64 chars of `[A-Za-z0-9_.:-]`; logical identity, globally unique |
| `device_id` | `string` | no | same alphabet/length |
| `sequence_number` | `int64` | no | ≥ 1; per-device, monotonically increasing at the device |
| `event_time` | `timestamp[us, UTC]` | no | ISO-8601 **with explicit offset**; normalised to UTC |
| `ingestion_time` | `timestamp[us, UTC]` | no | stamped once at the platform edge, persisted in the source record |
| `schema_version` | `int32` | no | must be in `{1}` |
| `temperature_c` | `float64` | no | −60.0 … 100.0 (inclusive), finite |
| `humidity_pct` | `float64` | no | 0.0 … 100.0, finite |
| `battery_pct` | `float64` | no | 0.0 … 100.0, finite |
| `signal_dbm` | `int32` | no | −130 … 0 |
| `status` | `string` | no | one of `OK`, `DEGRADED`, `LOW_BATTERY`, `MAINTENANCE` |

Partition column (not stored in files): `event_date` = UTC calendar date of `event_time`.

## Policies

* **Nullability.** No nulls anywhere in v1. A missing reading is an invalid event; a sensor outage is
  reported via `status`, with the last valid reading or a new schema version, not a NULL. Allowing
  nullable measurements is a schema-version bump so that old readers and assertions stay sound.
* **Ranges.** Closed intervals; violations are rejected, **never clamped** (clamping would silently
  fabricate data). NaN/±Infinity are invalid.
* **Types are strict.** `"21.5"` is not a float, `"1"` is not an int, unknown fields are errors.
* **Time.** Naive timestamps are invalid. Offsets are converted to UTC before partitioning, so
  `2026-01-01T23:30:00-05:00` lands in `event_date=2026-01-02`. Midnight is the start of the new
  day (half-open days). Arrival order is **not** assumed to equal generation order.
* **Schema versions.** Only listed versions are accepted; others are rejected with an explicit
  "unsupported schema_version" reason.
* **Duplicate identities** (enforced by the integrity gate). Same `event_id`, identical payload ⇒ duplicate
  delivery (dropped, counted). Same `event_id`, different payload ⇒ conflict (critical). Same
  `(device_id, sequence_number)`, different `event_id` ⇒ conflict (critical). "Payload" means the
  device-originated fields; `ingestion_time` is the platform's receipt stamp and is excluded.
* **Clock skew / lateness.** `event_time` more than `max_future_skew` (default 300 s) ahead of
  `ingestion_time` is rejected. `ingestion_time − event_time` above `max_lateness` (default 3600 s) marks the
  event *late*: accepted, rejected, or held per `late_policy` (default accept).

## Storage layout

```
data/event_date=2026-01-01/<batch_id>.parquet     # one file per (batch, UTC date)
```

* Rows inside a file are sorted by `(device_id, sequence_number)`; compression defaults to ZSTD;
  row-group size is configurable (`--row-group-size`, default 131,072).
* Files are immutable and created with a no-clobber, fsynced write. Paths are a pure function of
  `(event_date, batch_id)`.

## Source log format (`jsonl-v1`)

One JSON object per line, ordinal position = source offset. A sidecar `events.jsonl.meta.json`
holds `source_id`, `records`, the simulator and fault config, and the **completion marker**: `complete`
and, when true, each device's final sequence number (absent for an interrupted source). `ingestion_time` is part of the record, so
replaying an offset range always yields identical rows.

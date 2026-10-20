-- name: required_fields_complete
-- category: contract
-- description: Every one of the 11 contract fields is non-NULL (and identifiers are non-empty).
SELECT filename, event_id, device_id, sequence_number
FROM events
WHERE event_id IS NULL OR device_id IS NULL OR sequence_number IS NULL
   OR event_time IS NULL OR ingestion_time IS NULL OR schema_version IS NULL
   OR temperature_c IS NULL OR humidity_pct IS NULL OR battery_pct IS NULL
   OR signal_dbm IS NULL OR status IS NULL
   OR event_id = '' OR device_id = ''

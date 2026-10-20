-- name: valid_enums_and_versions
-- category: contract
-- description: status is a known value, schema_version is supported, sequence_number >= 1.
SELECT event_id, status, schema_version, sequence_number
FROM events
WHERE NOT coalesce(list_contains($statuses, status), false)
   OR NOT coalesce(list_contains($versions, schema_version), false)
   OR NOT coalesce(sequence_number >= 1, false)

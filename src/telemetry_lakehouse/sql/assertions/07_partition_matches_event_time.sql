-- name: partition_matches_event_time
-- category: partitioning
-- description: Every row sits in the event_date= directory equal to the UTC date of its event_time.
SELECT event_id, event_date, CAST(event_time AT TIME ZONE 'UTC' AS DATE) AS utc_date, filename
FROM events
WHERE event_date IS NULL OR event_date <> CAST(event_time AT TIME ZONE 'UTC' AS DATE)

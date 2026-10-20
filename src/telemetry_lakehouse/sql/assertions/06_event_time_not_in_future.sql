-- name: event_time_not_in_future
-- category: contract
-- description: No event_time is further ahead of its ingestion_time than the allowed clock skew.
SELECT event_id, event_time, ingestion_time
FROM events
WHERE event_time > ingestion_time + $future_skew_s * INTERVAL 1 SECOND

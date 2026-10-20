-- name: unique_device_sequence
-- category: identity
-- description: Each (device_id, sequence_number) is published at most once (invariant I2).
SELECT device_id, sequence_number, count(*) AS copies, list(event_id) AS event_ids
FROM events
GROUP BY device_id, sequence_number
HAVING count(*) > 1

-- name: unique_event_id
-- category: identity
-- description: No logical event appears twice in the published data (invariant I1).
SELECT event_id, count(*) AS copies, list(DISTINCT filename) AS files
FROM events
GROUP BY event_id
HAVING count(*) > 1

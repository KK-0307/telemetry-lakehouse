-- name: sequence_gaps_beyond_grace
-- category: completeness
-- description: No per-device sequence gap is provably permanent by watermark (the next event above it arrived at least the grace period before the newest ingestion_time).
WITH s AS (
    SELECT device_id, sequence_number AS seq, ingestion_time,
           coalesce(lag(sequence_number) OVER w, 0) AS prev
    FROM events
    WINDOW w AS (PARTITION BY device_id ORDER BY sequence_number)),
wm AS (SELECT max(ingestion_time) AS newest FROM events)
SELECT device_id, prev + 1 AS first_missing, seq - 1 AS last_missing, seq - prev - 1 AS missing
FROM s, wm
WHERE seq - prev > 1 AND s.ingestion_time <= wm.newest - $grace_s * INTERVAL 1 SECOND

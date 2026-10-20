-- name: no_untrusted_batches_in_data
-- category: trust
-- description: No row in the published data came from a batch that is not COMMITTED (rejected, interrupted, unknown).
SELECT e.batch_id, coalesce(b.state, 'UNKNOWN') AS state, count(*) AS rows
FROM events e LEFT JOIN m_batches b USING (batch_id)
WHERE b.state IS DISTINCT FROM 'COMMITTED'
GROUP BY e.batch_id, b.state

-- name: no_unfinished_batches
-- category: consistency
-- description: No batch is stuck in an in-flight state; the publication state is settled (otherwise run `telemetry recover`).
SELECT batch_id, state, start_offset, end_offset FROM m_batches
WHERE state IN ('RECEIVED', 'VALIDATING', 'STAGED', 'VERIFIED', 'INTERRUPTED')

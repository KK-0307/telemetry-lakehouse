-- name: events_and_index_agree
-- category: trust
-- description: The manifest's event_index and the Parquet data describe exactly the same events, attributed to the same batches.
SELECT event_id, 'in Parquet but not in event_index' AS problem
FROM events WHERE event_id NOT IN (SELECT event_id FROM m_index)
UNION ALL
SELECT event_id, 'in event_index but not in Parquet'
FROM m_index WHERE event_id NOT IN (SELECT event_id FROM events)
UNION ALL
SELECT e.event_id, 'batch attribution differs'
FROM events e JOIN m_index i USING (event_id) WHERE e.batch_id <> i.batch_id

-- name: batch_row_counts_reconcile
-- category: reconciliation
-- description: For every committed batch, rows physically in Parquet = manifest file rows = event_index rows = committed_rows (A4).
WITH phys AS (SELECT batch_id, count(*) AS n FROM events GROUP BY batch_id),
     idx  AS (SELECT batch_id, count(*) AS n FROM m_index GROUP BY batch_id),
     fil  AS (SELECT batch_id, sum(rows) AS n FROM m_files GROUP BY batch_id)
SELECT b.batch_id, b.committed_rows,
       coalesce(p.n, 0) AS parquet_rows, coalesce(f.n, 0) AS manifest_file_rows,
       coalesce(i.n, 0) AS event_index_rows
FROM m_batches b
LEFT JOIN phys p USING (batch_id)
LEFT JOIN idx i USING (batch_id)
LEFT JOIN fil f USING (batch_id)
WHERE b.state = 'COMMITTED'
  AND (b.committed_rows <> coalesce(p.n, 0)
       OR b.committed_rows <> coalesce(f.n, 0)
       OR b.committed_rows <> coalesce(i.n, 0))

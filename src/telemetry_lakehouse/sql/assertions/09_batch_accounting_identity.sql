-- name: batch_accounting_identity
-- category: reconciliation
-- description: Per batch, consumed = accepted + duplicates + rejected + unresolved = offset span (A1, A2); committed => committed_rows = accepted (A3).
SELECT batch_id, state, consumed, accepted, duplicates, rejected, unresolved,
       end_offset - start_offset AS offset_span, committed_rows
FROM m_batches
WHERE consumed IS NOT NULL
  AND (consumed <> accepted + duplicates + rejected + unresolved
       OR consumed <> end_offset - start_offset
       OR (state = 'COMMITTED' AND committed_rows IS DISTINCT FROM accepted)
       OR (state <> 'COMMITTED' AND committed_rows IS NOT NULL))

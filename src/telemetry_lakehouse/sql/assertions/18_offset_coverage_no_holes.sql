-- name: offset_coverage_no_holes
-- category: completeness
-- description: Committed batches of each source cover its offsets from 0 without holes (A5); a hole is a batch that was rejected or never processed.
WITH c AS (
    SELECT source_id, start_offset, end_offset,
           max(end_offset) OVER (PARTITION BY source_id ORDER BY start_offset, end_offset
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS covered
    FROM m_batches WHERE state = 'COMMITTED')
SELECT source_id, covered AS hole_start, start_offset AS hole_end FROM c
WHERE covered IS NOT NULL AND start_offset > covered
UNION ALL
SELECT source_id, 0, min(start_offset) FROM m_batches WHERE state = 'COMMITTED'
GROUP BY source_id HAVING min(start_offset) > 0

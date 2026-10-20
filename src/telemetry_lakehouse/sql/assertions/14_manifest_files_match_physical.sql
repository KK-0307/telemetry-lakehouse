-- name: manifest_files_match_physical
-- category: consistency
-- description: Each committed file exists, is readable, and matches the manifest's size, SHA-256, row count and event_time bounds.
SELECT relative_path, 'missing or unreadable' AS problem FROM file_checks WHERE NOT (present AND readable)
UNION ALL
SELECT relative_path, 'size differs from manifest' FROM file_checks WHERE present AND readable AND NOT size_matches
UNION ALL
SELECT relative_path, 'SHA-256 differs from manifest' FROM file_checks WHERE present AND readable AND NOT sha_matches
UNION ALL
SELECT f.relative_path, 'rows or event_time bounds differ from manifest'
FROM m_committed_files f
JOIN file_checks c USING (relative_path)
LEFT JOIN (
    SELECT filename, count(*) AS n, epoch_us(min(event_time)) AS lo, epoch_us(max(event_time)) AS hi
    FROM events GROUP BY filename) s ON s.filename = c.abs_path
WHERE c.present AND c.readable
  AND (s.n IS DISTINCT FROM f.rows OR s.lo IS DISTINCT FROM f.min_event_time_us
       OR s.hi IS DISTINCT FROM f.max_event_time_us)

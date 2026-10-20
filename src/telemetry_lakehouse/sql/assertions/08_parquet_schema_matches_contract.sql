-- name: parquet_schema_matches_contract
-- category: contract
-- description: Every file has exactly the contract columns, all REQUIRED, with the contract physical types; the dataset has the contract logical types.
SELECT f.filename, c.column_name, 'column missing, nullable, or of the wrong physical type in file' AS problem
FROM (SELECT DISTINCT filename FROM file_schemas) f
CROSS JOIN expected_columns c
WHERE NOT EXISTS (
    SELECT 1 FROM file_schemas s
    WHERE s.filename = f.filename AND s.name = c.column_name AND s.repetition_type = 'REQUIRED'
      AND s.physical_type = c.physical_type)
UNION ALL
SELECT filename, name, 'unexpected column in file'
FROM file_schemas
WHERE num_children IS NULL AND name NOT IN (SELECT column_name FROM expected_columns)
UNION ALL
SELECT 'ALL', e.column_name, 'logical type ' || coalesce(t.column_type, 'MISSING') || ' != ' || e.column_type
FROM expected_types e LEFT JOIN events_types t USING (column_name)
WHERE t.column_type IS DISTINCT FROM e.column_type

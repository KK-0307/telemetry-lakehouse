-- name: sequences_complete_to_end_of_stream
-- category: completeness
-- requires: final_sequences
-- description: Against the source's end-of-stream marker, every device has exactly sequences 1..final published, none beyond.
SELECT f.device_id, f.final_seq, count(DISTINCT e.sequence_number) AS present,
       f.final_seq - count(DISTINCT e.sequence_number) AS missing
FROM final_sequences f
LEFT JOIN events e ON e.device_id = f.device_id AND e.sequence_number <= f.final_seq
GROUP BY f.device_id, f.final_seq
HAVING count(DISTINCT e.sequence_number) <> f.final_seq
UNION ALL
SELECT e.device_id, f.final_seq, e.sequence_number, NULL
FROM events e JOIN final_sequences f USING (device_id)
WHERE e.sequence_number > f.final_seq

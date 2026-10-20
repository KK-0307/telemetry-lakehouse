-- name: valid_measurement_ranges
-- category: contract
-- description: Every measurement lies inside its closed range; NaN and infinity are out of range.
SELECT event_id, temperature_c, humidity_pct, battery_pct, signal_dbm
FROM events
WHERE NOT coalesce(temperature_c BETWEEN $temp_min AND $temp_max, false)
   OR NOT coalesce(humidity_pct BETWEEN $humidity_min AND $humidity_max, false)
   OR NOT coalesce(battery_pct BETWEEN $battery_min AND $battery_max, false)
   OR NOT coalesce(signal_dbm BETWEEN $signal_min AND $signal_max, false)

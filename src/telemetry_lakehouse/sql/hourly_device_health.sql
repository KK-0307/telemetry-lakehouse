-- Hourly per-device health: average climate readings, worst battery, link quality, and
-- the number of non-OK status events. Rows are ordered by hour, then device.
SELECT
    date_trunc('hour', event_time) AT TIME ZONE 'UTC' AS hour_utc,
    device_id,
    count(*)                              AS readings,
    round(avg(temperature_c), 2)          AS avg_temperature_c,
    round(avg(humidity_pct), 2)           AS avg_humidity_pct,
    round(min(battery_pct), 2)            AS min_battery_pct,
    round(avg(signal_dbm), 1)             AS avg_signal_dbm,
    count(*) FILTER (WHERE status <> 'OK') AS non_ok_events
FROM events
GROUP BY hour_utc, device_id
ORDER BY hour_utc, device_id
LIMIT $limit

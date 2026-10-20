-- Fleet-level daily summary: volume, active devices, battery, and anomaly (non-OK) rate.
SELECT
    event_date,
    count(*)                                                    AS events,
    count(DISTINCT device_id)                                   AS active_devices,
    round(avg(temperature_c), 2)                                AS avg_temperature_c,
    round(avg(battery_pct), 2)                                  AS avg_battery_pct,
    round(100.0 * count(*) FILTER (WHERE status <> 'OK') / count(*), 3) AS non_ok_pct
FROM events
GROUP BY event_date
ORDER BY event_date
LIMIT $limit

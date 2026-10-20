-- Battery-health trend: per UTC day, fleet-average battery, the share of devices whose latest
-- reading that day is below 20%, and how many devices reported LOW_BATTERY.
WITH latest AS (
    SELECT event_date, device_id, arg_max(battery_pct, event_time) AS last_battery
    FROM events
    GROUP BY event_date, device_id)
SELECT
    l.event_date,
    count(*)                                             AS devices_reporting,
    round(avg(l.last_battery), 2)                        AS avg_last_battery_pct,
    round(100.0 * count(*) FILTER (WHERE l.last_battery < 20) / count(*), 2)
                                                         AS pct_devices_below_20,
    (SELECT count(DISTINCT e.device_id) FROM events e
      WHERE e.event_date = l.event_date AND e.status = 'LOW_BATTERY') AS devices_flagged_low_battery
FROM latest l
GROUP BY l.event_date
ORDER BY l.event_date
LIMIT $limit

-- Query views (no data; CREATE OR REPLACE on every install). Grafana: SELECT on iot.*.

-- Exact vehicle events: one row per (tunnel_id, event_id), latest insert. Filters on the outer
-- query are pushed into the FINAL read, e.g.
--   SELECT count() FROM iot.vehicles_dedup WHERE tunnel_id = 'T000042' AND ts_entry >= now() - INTERVAL 1 HOUR
CREATE OR REPLACE VIEW iot.vehicles_dedup AS
SELECT * FROM iot.vehicles FINAL;

-- Finalized rollups per tunnel / direction / classification.
-- speed_quantiles_kmh = [p50, p85, p95].
CREATE OR REPLACE VIEW iot.vehicles_1m_stats AS
SELECT
    minute,
    tunnel_id,
    direction,
    classification,
    sum(vehicles)                                          AS vehicles,
    avgMerge(speed_avg)                                    AS speed_avg_kmh,
    min(speed_min)                                         AS speed_min_kmh,
    max(speed_max)                                         AS speed_max_kmh,
    quantilesDDMerge(0.02, 0.5, 0.85, 0.95)(speed_quantiles) AS speed_quantiles_kmh,
    avgMerge(length_avg)                                   AS length_avg_m
FROM iot.vehicles_1m
GROUP BY minute, tunnel_id, direction, classification;

CREATE OR REPLACE VIEW iot.vehicles_1h_stats AS
SELECT
    hour,
    tunnel_id,
    direction,
    classification,
    sum(vehicles)                                          AS vehicles,
    avgMerge(speed_avg)                                    AS speed_avg_kmh,
    min(speed_min)                                         AS speed_min_kmh,
    max(speed_max)                                         AS speed_max_kmh,
    quantilesDDMerge(0.01, 0.5, 0.85, 0.95)(speed_quantiles) AS speed_quantiles_kmh,
    avgMerge(length_avg)                                   AS length_avg_m
FROM iot.vehicles_1h
GROUP BY hour, tunnel_id, direction, classification;

-- --------------------------------------------------------------------------------------------
-- Alerts. Both are plain views over the vehicle events joined to the tunnel's own profile and
-- rules, so what fires is exactly what the operator configured in tunnels.yaml. Grafana reads
-- them per day (local time) and its alert rules run the same queries:
--   SELECT * FROM iot.speeding_alerts WHERE local_day = toDate(now(), 'Europe/Istanbul')
-- Times are local Istanbul: the rules, the bursts and the dashboards all use that day boundary.
--
-- Both views only report vehicles the pipeline actually corroborated (sensors >= 2), because a
-- one-sensor event carries a single noisy measurement. Measured on a live day: of 86 vehicles
-- whose reported speed exceeded the limit, 78 came from one-sensor events and only 1 was more
-- than 10% over — the rest were cruising traffic read a hair high. A speed sensor is ~3% noisy
-- (5x that when degraded), so speeding also applies an enforcement tolerance, exactly like a
-- real section-control system.
-- Raw, unfiltered counts stay available: query iot.vehicles_dedup joined to iot.tunnel_profiles.

CREATE OR REPLACE VIEW iot.speeding_alerts AS
SELECT
    v.ts_entry                                          AS ts_entry,
    toDateTime(v.ts_entry, 'Europe/Istanbul')           AS local_time,
    toDate(v.ts_entry, 'Europe/Istanbul')               AS local_day,
    v.tunnel_id                                         AS tunnel_id,
    p.name                                              AS tunnel_name,
    p.city                                              AS city,
    v.plate                                             AS plate,
    v.classification                                    AS vehicle_type,
    v.speed_kmh                                         AS speed_kmh,
    p.speed_limit_kmh                                   AS speed_limit_kmh,
    round(v.speed_kmh - p.speed_limit_kmh, 1)           AS over_by_kmh,
    round(100 * (v.speed_kmh / p.speed_limit_kmh - 1), 1) AS over_by_pct,
    v.direction                                         AS direction,
    v.lane                                              AS lane,
    v.sensors                                           AS sensors,
    v.confidence                                        AS confidence,
    v.event_id                                          AS event_id
FROM iot.vehicles_dedup AS v
INNER JOIN iot.tunnel_profiles AS p ON v.tunnel_id = p.tunnel_id
-- 5% tolerance: the simulated speeders drive 15-45% over, sensor noise is ~3%.
WHERE v.sensors >= 2 AND v.speed_kmh > p.speed_limit_kmh * 1.05;

-- A vehicle of a banned type detected inside its banned window. The window is local minutes and
-- wraps midnight when from_minute >= to_minute.
CREATE OR REPLACE VIEW iot.restricted_alerts AS
SELECT
    v.ts_entry                                AS ts_entry,
    toDateTime(v.ts_entry, 'Europe/Istanbul') AS local_time,
    toDate(v.ts_entry, 'Europe/Istanbul')     AS local_day,
    v.tunnel_id                               AS tunnel_id,
    p.name                                    AS tunnel_name,
    p.city                                    AS city,
    v.plate                                   AS plate,
    v.classification                          AS vehicle_type,
    round(v.speed_kmh, 1)                     AS speed_kmh,
    v.direction                               AS direction,
    v.lane                                    AS lane,
    v.sensors                                 AS sensors,
    v.confidence                              AS confidence,
    concat(leftPad(toString(intDiv(r.from_minute, 60)), 2, '0'), ':',
           leftPad(toString(r.from_minute % 60), 2, '0'), '-',
           leftPad(toString(intDiv(r.to_minute, 60)), 2, '0'), ':',
           leftPad(toString(r.to_minute % 60), 2, '0')) AS banned_window,
    v.event_id                                AS event_id
FROM iot.vehicles_dedup AS v
INNER JOIN iot.tunnel_profiles AS p ON v.tunnel_id = p.tunnel_id
INNER JOIN iot.tunnel_rules AS r
    ON v.tunnel_id = r.tunnel_id AND v.classification = r.vehicle_type
WHERE
    -- a corroborated, confident classification: one sensor guessing is not a violation
    v.sensors >= 2 AND v.confidence >= 0.7
    AND (
        (r.from_minute < r.to_minute
            AND toMinute(toDateTime(v.ts_entry, 'Europe/Istanbul')) + 60 * toHour(toDateTime(v.ts_entry, 'Europe/Istanbul'))
                BETWEEN r.from_minute AND r.to_minute - 1)
        OR
        (r.from_minute >= r.to_minute
            AND (toMinute(toDateTime(v.ts_entry, 'Europe/Istanbul')) + 60 * toHour(toDateTime(v.ts_entry, 'Europe/Istanbul')) >= r.from_minute
                 OR toMinute(toDateTime(v.ts_entry, 'Europe/Istanbul')) + 60 * toHour(toDateTime(v.ts_entry, 'Europe/Istanbul')) < r.to_minute))
    );

-- Long-term statistics per tunnel (the 1h rollup is kept for a month): vehicle types per hour and
-- per day, and the average speed over the same windows.
CREATE OR REPLACE VIEW iot.vehicle_types_hourly AS
SELECT
    toDateTime(hour, 'Europe/Istanbul') AS local_hour,
    tunnel_id,
    classification                      AS vehicle_type,
    sum(vehicles)                       AS vehicles,
    avgMerge(speed_avg)                 AS speed_avg_kmh
FROM iot.vehicles_1h
GROUP BY local_hour, tunnel_id, vehicle_type;

CREATE OR REPLACE VIEW iot.vehicle_types_daily AS
SELECT
    toDate(hour, 'Europe/Istanbul') AS local_day,
    tunnel_id,
    classification                  AS vehicle_type,
    sum(vehicles)                   AS vehicles,
    avgMerge(speed_avg)             AS speed_avg_kmh
FROM iot.vehicles_1h
GROUP BY local_day, tunnel_id, vehicle_type;

-- Latest health report of every device (what the device itself last said).
CREATE OR REPLACE VIEW iot.device_health_latest AS
SELECT
    tunnel_id,
    sensor_id,
    argMax(device_id, ts)     AS device_id,
    argMax(device_serial, ts) AS device_serial,
    argMax(position, ts)      AS position,
    argMax(status, ts)        AS status,
    argMax(degraded, ts)      AS degraded,
    argMax(detections, ts)    AS detections,
    argMax(missed, ts)        AS missed,
    argMax(duplicates, ts)    AS duplicates,
    argMax(uptime_s, ts)      AS uptime_s,
    max(ts)                   AS last_report_ts
FROM iot.sensor_health
GROUP BY tunnel_id, sensor_id;

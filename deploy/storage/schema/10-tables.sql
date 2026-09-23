-- Database iot: storage tables. Applied on every install (IF NOT EXISTS: existing tables and data
-- are never touched). Retention/tiering is set separately by 20-ttl.sql.tpl, the Kafka ingestion by
-- 40-streams.sql.tpl. To evolve an existing table, append idempotent statements below its CREATE
-- (ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...).
-- All tables use storage policy "tiered" (hot: local PVC, cold: S3 on SeaweedFS). Timestamps are UTC
-- DateTime64(3) converted from the epoch-ms integers in the Kafka messages.

CREATE DATABASE IF NOT EXISTS iot;

-- Checksums of the conditionally applied schema files (scripts/install/85-storage.sh).
CREATE TABLE IF NOT EXISTS iot.schema_state
(
    name       String,
    checksum   String,
    applied_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(applied_at)
ORDER BY name;

-- Normalized detections (topic iot.detections, telemetry-processor), raw: sensor-level duplicates
-- (same message_id) are kept. Per-tunnel time queries: WHERE tunnel_id = ... AND ts BETWEEN ...
CREATE TABLE IF NOT EXISTS iot.detections
(
    ts                 DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),   -- sensor time
    tunnel_id          LowCardinality(String),
    sensor_id          LowCardinality(String),
    device_id          LowCardinality(String),                       -- physical address (MAC) of the device
    device_serial      LowCardinality(String),
    plate              String CODEC(ZSTD(1)),                        -- Turkish plate read by the device
    position           LowCardinality(String),
    direction          LowCardinality(String),
    lane               UInt8,
    seq                UInt64 CODEC(Delta, ZSTD(1)),
    message_id         String CODEC(ZSTD(1)),
    speed_kmh          Float32 CODEC(ZSTD(1)),
    length_m           Float32 CODEC(ZSTD(1)),
    occupancy_ms       UInt32 CODEC(ZSTD(1)),
    classification     LowCardinality(String),
    confidence         Float32 CODEC(ZSTD(1)),
    vehicle_id         Nullable(String),                           -- evaluation only
    true_class         LowCardinality(Nullable(String)),           -- evaluation only
    received_ts        DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),   -- TBMQ receive time
    processed_ts       DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),   -- telemetry-processor output time
    inserted_at        DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD(1)),  -- ClickHouse insert
    tbmq_node          LowCardinality(Nullable(String)),
    schema             UInt8,
    -- latencies (computed on read, not stored)
    mqtt_latency_ms    Int64 ALIAS dateDiff('millisecond', ts, received_ts),
    process_latency_ms Int64 ALIAS dateDiff('millisecond', received_ts, processed_ts),
    ingest_latency_ms  Int64 ALIAS dateDiff('millisecond', received_ts, inserted_at)
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (tunnel_id, ts)
SETTINGS storage_policy = 'tiered', ttl_only_drop_parts = 1;

-- Rejected telemetry (topic iot.detections.rejected). tunnel_id = record key ('' when null).
CREATE TABLE IF NOT EXISTS iot.detections_rejected
(
    processed_ts   DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    received_ts    Nullable(DateTime64(3, 'UTC')),
    reason         LowCardinality(String),
    detail         String CODEC(ZSTD(3)),
    tunnel_id      String CODEC(ZSTD(1)),
    topic          Nullable(String) CODEC(ZSTD(3)),
    client_cert_cn Nullable(String) CODEC(ZSTD(3)),
    envelope       String CODEC(ZSTD(3)),
    inserted_at    DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toDate(processed_ts)
ORDER BY (reason, processed_ts)
SETTINGS storage_policy = 'tiered', ttl_only_drop_parts = 1;

-- Consensus vehicle events (topic iot.vehicles). Consumer rebalances in consensus replay detections
-- and re-emit events with the same event_id, possibly with slightly different derived values
-- (ts_entry, speed): the dedup key is therefore (tunnel_id, event_id) only, and the latest insert wins.
-- Background merges dedupe within a day partition eventually; exact results need FINAL
-- (view iot.vehicles_dedup) or GROUP BY event_id / argMax(..., inserted_at).
CREATE TABLE IF NOT EXISTS iot.vehicles
(
    ts_entry        DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    ts_exit         DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    tunnel_id       LowCardinality(String),
    event_id        String CODEC(ZSTD(1)),
    direction       LowCardinality(String),
    lane            UInt8,
    plate           String CODEC(ZSTD(1)),
    speed_kmh       Float32 CODEC(ZSTD(1)),
    length_m        Float32 CODEC(ZSTD(1)),
    classification  LowCardinality(String),
    confidence      Float32 CODEC(ZSTD(1)),
    agreement       Float32 CODEC(ZSTD(1)),
    sensors         UInt8,
    missing         Array(LowCardinality(String)),
    detections      Array(String) CODEC(ZSTD(1)),
    vehicle_id      Nullable(String),                       -- evaluation only
    true_class      LowCardinality(Nullable(String)),       -- evaluation only
    schema          UInt8,
    kafka_partition UInt16,
    kafka_offset    UInt64 CODEC(Delta, ZSTD(1)),
    inserted_at     DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD(1)),
    INDEX ts_entry_minmax ts_entry TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toDate(ts_entry)
ORDER BY (tunnel_id, event_id)
SETTINGS storage_policy = 'tiered', ttl_only_drop_parts = 1;

-- Warm rollups per tunnel / direction / classification, fed from the vehicle stream after
-- event_id dedup (40-streams.sql.tpl). Read them through the *_stats views (30-views.sql) or with
-- sum(vehicles), avgMerge(speed_avg), quantilesDDMerge(<accuracy>, 0.5, 0.85, 0.95)(speed_quantiles).
CREATE TABLE IF NOT EXISTS iot.vehicles_1m
(
    minute          DateTime('UTC'),
    tunnel_id       LowCardinality(String),
    direction       LowCardinality(String),
    classification  LowCardinality(String),
    vehicles        SimpleAggregateFunction(sum, UInt64),
    speed_avg       AggregateFunction(avg, Float32),
    speed_min       SimpleAggregateFunction(min, Float32),
    speed_max       SimpleAggregateFunction(max, Float32),
    speed_quantiles AggregateFunction(quantilesDD(0.02, 0.5, 0.85, 0.95), Float32),  -- DDSketch, 2% rel. error
    length_avg      AggregateFunction(avg, Float32)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(minute)
ORDER BY (tunnel_id, direction, classification, minute)
SETTINGS storage_policy = 'tiered', ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS iot.vehicles_1h
(
    hour            DateTime('UTC'),
    tunnel_id       LowCardinality(String),
    direction       LowCardinality(String),
    classification  LowCardinality(String),
    vehicles        SimpleAggregateFunction(sum, UInt64),
    speed_avg       AggregateFunction(avg, Float32),
    speed_min       SimpleAggregateFunction(min, Float32),
    speed_max       SimpleAggregateFunction(max, Float32),
    speed_quantiles AggregateFunction(quantilesDD(0.01, 0.5, 0.85, 0.95), Float32),  -- DDSketch, 1% rel. error
    length_avg      AggregateFunction(avg, Float32)
)
ENGINE = AggregatingMergeTree
PARTITION BY toStartOfMonth(hour)
ORDER BY (tunnel_id, direction, classification, hour)
SETTINGS storage_policy = 'tiered', ttl_only_drop_parts = 1;

-- Per-tunnel traffic windows (topic iot.traffic); nested direction counts flattened.
CREATE TABLE IF NOT EXISTS iot.traffic
(
    ts                 DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    tunnel_id          LowCardinality(String),
    window_s           Float32,
    length_m           Nullable(Float32),
    vehicles           UInt32,
    -- vehicle type -> count, per direction (ten types; the old car/truck columns are gone)
    start_to_end       Map(LowCardinality(String), UInt32),
    end_to_start       Map(LowCardinality(String), UInt32),
    avg_speed_kmh      Nullable(Float32),
    schema             UInt8,
    inserted_at        DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (tunnel_id, ts)
SETTINGS storage_policy = 'tiered', ttl_only_drop_parts = 1;

-- Per-device health windows (topic iot.sensor-health): the devices report their own condition
-- (simulator -> tunnels/{tunnel_id}/sensors/{position}/health -> telemetry-processor).
CREATE TABLE IF NOT EXISTS iot.sensor_health
(
    ts                DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    tunnel_id         LowCardinality(String),
    sensor_id         LowCardinality(String),
    position          LowCardinality(String),
    device_id         LowCardinality(String),
    device_serial     LowCardinality(String),
    status            LowCardinality(String),      -- ok | degraded | silent (what the device reports)
    degraded          UInt8,
    window_s          Float32,
    uptime_s          Float32,
    detections        UInt32,
    duplicates        UInt32,
    missed            UInt32,
    schema            UInt8,
    inserted_at       DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (tunnel_id, sensor_id, ts)
SETTINGS storage_policy = 'tiered', ttl_only_drop_parts = 1;

-- Tunnel profiles and access rules, rendered from apps/simulator/config/tunnels.yaml by
-- scripts/install/85-storage.sh (50-profiles.sql). The alert views join detections against them,
-- so Grafana reads exactly the limits and rules the devices are simulating.
CREATE TABLE IF NOT EXISTS iot.tunnel_profiles
(
    tunnel_id           LowCardinality(String),
    name                String,
    city                LowCardinality(String),
    city_code           UInt8,
    length_m            Float32,
    lanes_per_direction UInt8,
    speed_limit_kmh     Float32,
    updated_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY tunnel_id;

-- One row per (tunnel, vehicle type, window). A window with from_minute >= to_minute wraps
-- midnight (22:00-06:00); 00:00-24:00 is an all-day ban. Minutes are Europe/Istanbul local time.
CREATE TABLE IF NOT EXISTS iot.tunnel_rules
(
    tunnel_id    LowCardinality(String),
    vehicle_type LowCardinality(String),
    from_minute  UInt16,
    to_minute    UInt16,
    updated_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tunnel_id, vehicle_type, from_minute, to_minute);

-- --------------------------------------------------------------------------------------------
-- Schema 2 migration (idempotent; no-ops on a fresh install, where the CREATEs above already
-- have these columns). Detections carry the device that produced them and the plate it read,
-- traffic counts the ten vehicle types, and health is what the device reports about itself.
ALTER TABLE iot.detections
    ADD COLUMN IF NOT EXISTS device_id     LowCardinality(String) AFTER sensor_id,
    ADD COLUMN IF NOT EXISTS device_serial LowCardinality(String) AFTER device_id,
    ADD COLUMN IF NOT EXISTS plate         String CODEC(ZSTD(1))  AFTER device_serial;

ALTER TABLE iot.vehicles
    ADD COLUMN IF NOT EXISTS plate String CODEC(ZSTD(1)) AFTER lane;

ALTER TABLE iot.traffic
    ADD COLUMN IF NOT EXISTS start_to_end Map(LowCardinality(String), UInt32) AFTER vehicles,
    ADD COLUMN IF NOT EXISTS end_to_start Map(LowCardinality(String), UInt32) AFTER start_to_end,
    DROP COLUMN IF EXISTS start_to_end_car,
    DROP COLUMN IF EXISTS start_to_end_truck,
    DROP COLUMN IF EXISTS end_to_start_car,
    DROP COLUMN IF EXISTS end_to_start_truck;

ALTER TABLE iot.sensor_health
    ADD COLUMN IF NOT EXISTS device_id     LowCardinality(String) AFTER sensor_id,
    ADD COLUMN IF NOT EXISTS device_serial LowCardinality(String) AFTER device_id,
    ADD COLUMN IF NOT EXISTS degraded      UInt8   AFTER status,
    ADD COLUMN IF NOT EXISTS uptime_s      Float32 AFTER window_s,
    ADD COLUMN IF NOT EXISTS missed        UInt32  AFTER duplicates,
    DROP COLUMN IF EXISTS lost_messages,
    DROP COLUMN IF EXISTS miss_rate,
    DROP COLUMN IF EXISTS disagreement_rate,
    DROP COLUMN IF EXISTS reliability,
    DROP COLUMN IF EXISTS speed_noise,
    DROP COLUMN IF EXISTS length_noise_m;

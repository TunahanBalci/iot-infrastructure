-- Kafka ingestion: Kafka engine tables + materialized views into the storage tables.
-- These objects hold no data (the dedup window table is a rebuildable cache). When this rendered
-- file changes, scripts/install/85-storage.sh drops every Kafka / MaterializedView / Null /
-- EmbeddedRocksDB object in database iot (Kafka tables first: consumption stops) and re-runs this
-- file. Consumer group offsets live in Kafka, so consumption resumes where it stopped; a block that
-- was read but not yet written is discarded uncommitted and read again (at-least-once; verified
-- with no loss and no duplicates while producing).
--
-- Every Kafka table: app Kafka SASL_SSL/SCRAM-SHA-512 settings from config.d/40-kafka.xml,
-- JSONEachRow, unknown fields ignored. Unparseable messages are skipped, at most 1000 per consumed
-- batch (kafka_skip_broken_messages); beyond that the batch fails and is retried, i.e. the table stops
-- consuming until the producer is fixed (exception in system.kafka_consumers and the server log).
-- Skips are visible as rate(ClickHouseProfileEvents_KafkaMessagesRead - ..._KafkaRowsRead) and
-- ClickHouseErrorMetric_CANNOT_PARSE_INPUT_ASSERTION_FAILED on :9363.
-- Parseable messages without a plausible epoch-ms timestamp (<= 2001-09-09, e.g. missing field → 0)
-- are dropped by the views instead of creating 1970 partitions.
-- Beware: a field whose type no longer parses (e.g. a producer switching a number to a string)
-- makes every such message "broken" → skipped. Change the Kafka table type first.
-- Create order: downstream tables and views before the view that starts reading a Kafka table.

-- ---------------------------------------------------------------------------------------------
-- iot.detections (12 partitions, telemetry-processor)
CREATE TABLE IF NOT EXISTS iot.detections_kafka
(
    schema         UInt8,
    message_id     String,
    sensor_id      String,
    device_id      String,
    device_serial  String,
    tunnel_id      String,
    position       String,
    seq            UInt64,
    ts             Int64,
    direction      String,
    lane           UInt8,
    plate          String,
    speed_kmh      Float32,
    length_m       Float32,
    occupancy_ms   UInt32,
    classification String,
    confidence     Float32,
    vehicle_id     Nullable(String),
    true_class     Nullable(String),
    received_ts    Int64,
    processed_ts   Int64,
    tbmq_node      Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = '${CLICKHOUSE_KAFKA_BOOTSTRAP}',
         kafka_topic_list = 'iot.detections',
         kafka_group_name = 'clickhouse-detections',
         kafka_client_id = 'clickhouse-detections',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = ${CLICKHOUSE_KAFKA_DETECTIONS_CONSUMERS},
         kafka_max_block_size = 65536,
         kafka_flush_interval_ms = 5000,
         kafka_skip_broken_messages = 1000,
         input_format_skip_unknown_fields = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS iot.detections_mv TO iot.detections AS
SELECT
    fromUnixTimestamp64Milli(k.ts, 'UTC')           AS ts,
    k.tunnel_id                                     AS tunnel_id,
    k.sensor_id                                     AS sensor_id,
    k.device_id                                     AS device_id,
    k.device_serial                                 AS device_serial,
    k.plate                                         AS plate,
    k.position                                      AS position,
    k.direction                                     AS direction,
    k.lane                                          AS lane,
    k.seq                                           AS seq,
    k.message_id                                    AS message_id,
    k.speed_kmh                                     AS speed_kmh,
    k.length_m                                      AS length_m,
    k.occupancy_ms                                  AS occupancy_ms,
    k.classification                                AS classification,
    k.confidence                                    AS confidence,
    k.vehicle_id                                    AS vehicle_id,
    k.true_class                                    AS true_class,
    fromUnixTimestamp64Milli(k.received_ts, 'UTC')  AS received_ts,
    fromUnixTimestamp64Milli(k.processed_ts, 'UTC') AS processed_ts,
    k.tbmq_node                                     AS tbmq_node,
    k.schema                                        AS schema
FROM iot.detections_kafka AS k
WHERE k.ts > 1000000000000;

-- ---------------------------------------------------------------------------------------------
-- iot.detections.rejected (1 partition, telemetry-processor; key tunnel_id or null)
CREATE TABLE IF NOT EXISTS iot.detections_rejected_kafka
(
    reason         String,
    detail         String,
    topic          Nullable(String),
    client_cert_cn Nullable(String),
    received_ts    Nullable(Int64),
    processed_ts   Int64,
    envelope       String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = '${CLICKHOUSE_KAFKA_BOOTSTRAP}',
         kafka_topic_list = 'iot.detections.rejected',
         kafka_group_name = 'clickhouse-rejected',
         kafka_client_id = 'clickhouse-rejected',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_max_block_size = 16384,
         kafka_flush_interval_ms = 5000,
         kafka_skip_broken_messages = 1000,
         input_format_skip_unknown_fields = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS iot.detections_rejected_mv TO iot.detections_rejected AS
SELECT
    fromUnixTimestamp64Milli(k.processed_ts, 'UTC') AS processed_ts,
    if(k.received_ts IS NULL, NULL, fromUnixTimestamp64Milli(assumeNotNull(k.received_ts), 'UTC')) AS received_ts,
    k.reason                                        AS reason,
    k.detail                                        AS detail,
    k._key                                          AS tunnel_id,
    k.topic                                         AS topic,
    k.client_cert_cn                                AS client_cert_cn,
    k.envelope                                      AS envelope
FROM iot.detections_rejected_kafka AS k
WHERE k.processed_ts > 1000000000000;

-- ---------------------------------------------------------------------------------------------
-- iot.vehicles (12 partitions, consensus)
--   vehicles_kafka ─┬─ vehicles_mv ──────────────────────────────────▶ vehicles (all records, RMT)
--                   └─ vehicles_first_mv (new event_ids only) ───▶ vehicles_first (Null)
--                                  ├─ vehicle_event_ids_mv ─▶ vehicle_event_ids (RocksDB, TTL window)
--                                  ├─ vehicles_1m_mv ───────▶ vehicles_1m
--                                  └─ vehicles_1h_mv ───────▶ vehicles_1h
-- Rollups count every event_id once: duplicates inside a block are removed with LIMIT 1 BY, later
-- ones by a direct key lookup in vehicle_event_ids (window ${CLICKHOUSE_VEHICLES_DEDUP_WINDOW_S}s).
CREATE TABLE IF NOT EXISTS iot.vehicle_event_ids
(
    event_id   String,
    first_seen DateTime DEFAULT now()
)
ENGINE = EmbeddedRocksDB(${CLICKHOUSE_VEHICLES_DEDUP_WINDOW_S})
PRIMARY KEY event_id;

CREATE TABLE IF NOT EXISTS iot.vehicles_first
(
    event_id       String,
    tunnel_id      String,
    direction      String,
    classification String,
    ts_entry       Int64,
    speed_kmh      Float32,
    length_m       Float32
)
ENGINE = Null;

CREATE MATERIALIZED VIEW IF NOT EXISTS iot.vehicle_event_ids_mv TO iot.vehicle_event_ids AS
SELECT event_id FROM iot.vehicles_first;

CREATE MATERIALIZED VIEW IF NOT EXISTS iot.vehicles_1m_mv TO iot.vehicles_1m AS
SELECT
    toStartOfMinute(fromUnixTimestamp64Milli(ts_entry, 'UTC'))  AS minute,
    tunnel_id,
    direction,
    classification,
    count()                                                     AS vehicles,
    avgState(speed_kmh)                                         AS speed_avg,
    min(speed_kmh)                                              AS speed_min,
    max(speed_kmh)                                              AS speed_max,
    quantilesDDState(0.02, 0.5, 0.85, 0.95)(speed_kmh)          AS speed_quantiles,
    avgState(length_m)                                          AS length_avg
FROM iot.vehicles_first
GROUP BY minute, tunnel_id, direction, classification;

CREATE MATERIALIZED VIEW IF NOT EXISTS iot.vehicles_1h_mv TO iot.vehicles_1h AS
SELECT
    toStartOfHour(fromUnixTimestamp64Milli(ts_entry, 'UTC'))    AS hour,
    tunnel_id,
    direction,
    classification,
    count()                                                     AS vehicles,
    avgState(speed_kmh)                                         AS speed_avg,
    min(speed_kmh)                                              AS speed_min,
    max(speed_kmh)                                              AS speed_max,
    quantilesDDState(0.01, 0.5, 0.85, 0.95)(speed_kmh)          AS speed_quantiles,
    avgState(length_m)                                          AS length_avg
FROM iot.vehicles_first
GROUP BY hour, tunnel_id, direction, classification;

CREATE TABLE IF NOT EXISTS iot.vehicles_kafka
(
    schema         UInt8,
    event_id       String,
    tunnel_id      String,
    direction      String,
    lane           UInt8,
    plate          String,
    ts_entry       Int64,
    ts_exit        Int64,
    speed_kmh      Float32,
    length_m       Float32,
    classification String,
    confidence     Float32,
    agreement      Float32,
    sensors        UInt8,
    missing        Array(String),
    detections     Array(String),
    vehicle_id     Nullable(String),
    true_class     Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = '${CLICKHOUSE_KAFKA_BOOTSTRAP}',
         kafka_topic_list = 'iot.vehicles',
         kafka_group_name = 'clickhouse-vehicles',
         kafka_client_id = 'clickhouse-vehicles',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_max_block_size = 65536,
         kafka_flush_interval_ms = 5000,
         kafka_skip_broken_messages = 1000,
         input_format_skip_unknown_fields = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS iot.vehicles_mv TO iot.vehicles AS
SELECT
    fromUnixTimestamp64Milli(k.ts_entry, 'UTC') AS ts_entry,
    fromUnixTimestamp64Milli(k.ts_exit, 'UTC')  AS ts_exit,
    k.tunnel_id                                 AS tunnel_id,
    k.event_id                                  AS event_id,
    k.direction                                 AS direction,
    k.lane                                      AS lane,
    k.plate                                     AS plate,
    k.speed_kmh                                 AS speed_kmh,
    k.length_m                                  AS length_m,
    k.classification                            AS classification,
    k.confidence                                AS confidence,
    k.agreement                                 AS agreement,
    k.sensors                                   AS sensors,
    k.missing                                   AS missing,
    k.detections                                AS detections,
    k.vehicle_id                                AS vehicle_id,
    k.true_class                                AS true_class,
    k.schema                                    AS schema,
    k._partition                                AS kafka_partition,
    k._offset                                   AS kafka_offset
FROM iot.vehicles_kafka AS k
WHERE k.ts_entry > 1000000000000;

CREATE MATERIALIZED VIEW IF NOT EXISTS iot.vehicles_first_mv TO iot.vehicles_first AS
SELECT
    v.event_id       AS event_id,
    v.tunnel_id      AS tunnel_id,
    v.direction      AS direction,
    v.classification AS classification,
    v.ts_entry       AS ts_entry,
    v.speed_kmh      AS speed_kmh,
    v.length_m       AS length_m
FROM
(
    SELECT event_id, tunnel_id, direction, classification, ts_entry, speed_kmh, length_m
    FROM iot.vehicles_kafka
    WHERE ts_entry > 1000000000000
    LIMIT 1 BY event_id
) AS v
LEFT ANTI JOIN iot.vehicle_event_ids AS seen ON v.event_id = seen.event_id;   -- direct key lookups

-- ---------------------------------------------------------------------------------------------
-- iot.traffic (12 partitions, consensus)
CREATE TABLE IF NOT EXISTS iot.traffic_kafka
(
    schema        UInt8,
    tunnel_id     String,
    ts            Int64,
    window_s      Float32,
    length_m      Nullable(Float32),
    vehicles      UInt32,
    counts        Map(String, Map(String, UInt32)),   -- direction -> vehicle type -> count
    avg_speed_kmh Nullable(Float32)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = '${CLICKHOUSE_KAFKA_BOOTSTRAP}',
         kafka_topic_list = 'iot.traffic',
         kafka_group_name = 'clickhouse-traffic',
         kafka_client_id = 'clickhouse-traffic',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_max_block_size = 65536,
         kafka_flush_interval_ms = 5000,
         kafka_skip_broken_messages = 1000,
         input_format_skip_unknown_fields = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS iot.traffic_mv TO iot.traffic AS
SELECT
    fromUnixTimestamp64Milli(k.ts, 'UTC') AS ts,
    k.tunnel_id                           AS tunnel_id,
    k.window_s                            AS window_s,
    k.length_m                            AS length_m,
    k.vehicles                            AS vehicles,
    k.counts['start_to_end']              AS start_to_end,
    k.counts['end_to_start']              AS end_to_start,
    k.avg_speed_kmh                       AS avg_speed_kmh,
    k.schema                              AS schema
FROM iot.traffic_kafka AS k
WHERE k.ts > 1000000000000;

-- ---------------------------------------------------------------------------------------------
-- iot.sensor-health (12 partitions, telemetry-processor; the devices report their own health)
CREATE TABLE IF NOT EXISTS iot.sensor_health_kafka
(
    schema            UInt8,
    sensor_id         String,
    device_id         String,
    device_serial     String,
    tunnel_id         String,
    position          String,
    ts                Int64,
    status            String,
    degraded          UInt8,
    window_s          Float32,
    uptime_s          Float32,
    detections        UInt32,
    duplicates        UInt32,
    missed            UInt32
)
ENGINE = Kafka
SETTINGS kafka_broker_list = '${CLICKHOUSE_KAFKA_BOOTSTRAP}',
         kafka_topic_list = 'iot.sensor-health',
         kafka_group_name = 'clickhouse-sensor-health',
         kafka_client_id = 'clickhouse-sensor-health',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_max_block_size = 65536,
         kafka_flush_interval_ms = 5000,
         kafka_skip_broken_messages = 1000,
         input_format_skip_unknown_fields = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS iot.sensor_health_mv TO iot.sensor_health AS
SELECT
    fromUnixTimestamp64Milli(k.ts, 'UTC') AS ts,
    k.tunnel_id                           AS tunnel_id,
    k.sensor_id                           AS sensor_id,
    k.device_id                           AS device_id,
    k.device_serial                       AS device_serial,
    k.position                            AS position,
    k.status                              AS status,
    k.degraded                            AS degraded,
    k.window_s                            AS window_s,
    k.uptime_s                            AS uptime_s,
    k.detections                          AS detections,
    k.duplicates                          AS duplicates,
    k.missed                              AS missed,
    k.schema                              AS schema
FROM iot.sensor_health_kafka AS k
WHERE k.ts > 1000000000000;

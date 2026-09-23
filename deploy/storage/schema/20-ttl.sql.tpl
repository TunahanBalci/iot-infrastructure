-- Tiering and retention (rendered from config.env; unit ${CLICKHOUSE_TTL_UNIT}).
-- "TO VOLUME 'cold'" moves whole parts to the S3 disk once all their rows passed the interval;
-- "DELETE" drops whole parts (ttl_only_drop_parts = 1: no rewrites on S3).
-- Applied only when this rendered file changes (checksum in iot.schema_state); existing parts get
-- their TTL info recalculated in the background (materialize_ttl_recalculate_only, config.d).

ALTER TABLE iot.detections MODIFY TTL
    toDateTime(ts) + INTERVAL ${CLICKHOUSE_DETECTIONS_COLD_AFTER} ${CLICKHOUSE_TTL_UNIT} TO VOLUME 'cold',
    toDateTime(ts) + INTERVAL ${CLICKHOUSE_DETECTIONS_DELETE_AFTER} ${CLICKHOUSE_TTL_UNIT} DELETE;

ALTER TABLE iot.detections_rejected MODIFY TTL
    toDateTime(processed_ts) + INTERVAL ${CLICKHOUSE_REJECTED_DELETE_AFTER} ${CLICKHOUSE_TTL_UNIT} DELETE;

ALTER TABLE iot.vehicles MODIFY TTL
    toDateTime(ts_entry) + INTERVAL ${CLICKHOUSE_VEHICLES_COLD_AFTER} ${CLICKHOUSE_TTL_UNIT} TO VOLUME 'cold',
    toDateTime(ts_entry) + INTERVAL ${CLICKHOUSE_VEHICLES_DELETE_AFTER} ${CLICKHOUSE_TTL_UNIT} DELETE;

ALTER TABLE iot.traffic MODIFY TTL
    toDateTime(ts) + INTERVAL ${CLICKHOUSE_HEALTH_COLD_AFTER} ${CLICKHOUSE_TTL_UNIT} TO VOLUME 'cold',
    toDateTime(ts) + INTERVAL ${CLICKHOUSE_HEALTH_DELETE_AFTER} ${CLICKHOUSE_TTL_UNIT} DELETE;

ALTER TABLE iot.sensor_health MODIFY TTL
    toDateTime(ts) + INTERVAL ${CLICKHOUSE_HEALTH_COLD_AFTER} ${CLICKHOUSE_TTL_UNIT} TO VOLUME 'cold',
    toDateTime(ts) + INTERVAL ${CLICKHOUSE_HEALTH_DELETE_AFTER} ${CLICKHOUSE_TTL_UNIT} DELETE;

ALTER TABLE iot.vehicles_1m MODIFY TTL
    minute + INTERVAL ${CLICKHOUSE_ROLLUP_1M_COLD_AFTER} ${CLICKHOUSE_TTL_UNIT} TO VOLUME 'cold',
    minute + INTERVAL ${CLICKHOUSE_ROLLUP_1M_DELETE_AFTER} ${CLICKHOUSE_TTL_UNIT} DELETE;

ALTER TABLE iot.vehicles_1h MODIFY TTL
    hour + INTERVAL ${CLICKHOUSE_ROLLUP_1H_COLD_AFTER} ${CLICKHOUSE_TTL_UNIT} TO VOLUME 'cold',
    hour + INTERVAL ${CLICKHOUSE_ROLLUP_1H_DELETE_AFTER} ${CLICKHOUSE_TTL_UNIT} DELETE;

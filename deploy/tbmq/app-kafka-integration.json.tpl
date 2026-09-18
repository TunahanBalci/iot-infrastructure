{
  "_comment": [
    "TBMQ Kafka integration: the integration executor forwards device detections to the app Kafka.",
    "Applied over REST by scripts/install/70-tbmq.sh (tbmq_credentials.py ensure-integration).",
    "Secrets are added at apply time from the environment, never written here: SCRAM login",
    "(KafkaUser tbmq-ie) as sasl.jaas.config and the app Kafka cluster CA as ssl.truststore.certificates.",
    "key null: TBMQ can only set one static key, so records spread over partitions and the",
    "telemetry-processor re-keys them by tunnel id.",
    "Rendered once per sensor position (IE_SHARD_POSITION) and tunnel shard (IE_SHARD_SUFFIX, empty",
    "with TBMQ_IE_SHARDS=1): one integration = one ordered stream that TBMQ assigns to one executor,",
    "so TBMQ_IE_SHARDS x 3 integrations let that many executors share the load.",
    "topicFilters come in rendered (IE_TOPIC_FILTERS_JSON) because '+' matches a whole level: a shard",
    "of the tunnels can only be expressed by naming its tunnels."
  ],
  "name": "app-kafka-ingest-${IE_SHARD_POSITION}${IE_SHARD_SUFFIX}",
  "type": "KAFKA",
  "enabled": true,
  "configuration": {
    "topicFilters": ${IE_TOPIC_FILTERS_JSON},
    "metadata": {},
    "clientConfiguration": {
      "bootstrapServers": "app-kafka-kafka-bootstrap.iot-pipeline.svc:9093",
      "topic": "iot.mqtt.ingest",
      "key": null,
      "clientIdPrefix": "tbmq-ie-app-kafka-${IE_SHARD_POSITION}${IE_SHARD_SUFFIX}",
      "sendOnlyMsgPayload": false,
      "retries": 2147483647,
      "batchSize": 65536,
      "linger": 5,
      "bufferMemory": 33554432,
      "acks": "all",
      "compression": "lz4",
      "keySerializer": "org.apache.kafka.common.serialization.StringSerializer",
      "valueSerializer": "org.apache.kafka.common.serialization.StringSerializer",
      "kafkaHeaders": {},
      "kafkaHeadersCharset": "UTF-8",
      "otherProperties": {
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "ssl.truststore.type": "PEM",
        "enable.idempotence": "true",
        "delivery.timeout.ms": "120000"
      }
    }
  }
}

# Application topics. Keys are tunnel ids (Java murmur2 partitioner), so every record of a tunnel lands
# in one partition: consumers that own a partition own the tunnel. KEDA caps consumers at the partition count.
# iot.mqtt.ingest and iot.detections carry the full message rate, so they cap retention by bytes per
# partition as well: at the design rate a time limit alone would ask for more disk than the volume has.
# The remaining topics are low volume (per tunnel, not per message) and stay on time retention only.
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: iot.mqtt.ingest
  namespace: iot-pipeline
  labels:
    strimzi.io/cluster: app-kafka
spec:
  partitions: ${APP_KAFKA_INGEST_PARTITIONS}
  replicas: ${APP_KAFKA_REPLICATION_FACTOR}
  config:
    retention.ms: ${APP_KAFKA_INGEST_RETENTION_MS}          # raw TBMQ integration envelopes, unkeyed
    # The time limit is the intent; this is the one that holds. At the design rate an hour of
    # envelopes is far more than the volume, so the partition cap is what deletes first.
    retention.bytes: ${APP_KAFKA_INGEST_RETENTION_BYTES}
    min.insync.replicas: ${APP_KAFKA_MIN_ISR}
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: iot.detections
  namespace: iot-pipeline
  labels:
    strimzi.io/cluster: app-kafka
spec:
  partitions: ${APP_KAFKA_TUNNEL_PARTITIONS}
  replicas: ${APP_KAFKA_REPLICATION_FACTOR}
  config:
    # Longer than the ingest topic: ClickHouse and consensus both read this one, so a slow consumer
    # has to be able to fall behind without losing records.
    retention.ms: ${APP_KAFKA_DETECTIONS_RETENTION_MS}
    retention.bytes: ${APP_KAFKA_DETECTIONS_RETENTION_BYTES}
    min.insync.replicas: ${APP_KAFKA_MIN_ISR}
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: iot.detections.rejected
  namespace: iot-pipeline
  labels:
    strimzi.io/cluster: app-kafka
spec:
  partitions: 1
  replicas: ${APP_KAFKA_REPLICATION_FACTOR}
  config:
    retention.ms: 604800000
    min.insync.replicas: ${APP_KAFKA_MIN_ISR}
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: iot.vehicles
  namespace: iot-pipeline
  labels:
    strimzi.io/cluster: app-kafka
spec:
  partitions: ${APP_KAFKA_TUNNEL_PARTITIONS}
  replicas: ${APP_KAFKA_REPLICATION_FACTOR}
  config:
    retention.ms: 604800000
    min.insync.replicas: ${APP_KAFKA_MIN_ISR}
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: iot.traffic
  namespace: iot-pipeline
  labels:
    strimzi.io/cluster: app-kafka
spec:
  partitions: ${APP_KAFKA_TUNNEL_PARTITIONS}
  replicas: ${APP_KAFKA_REPLICATION_FACTOR}
  config:
    retention.ms: 604800000
    min.insync.replicas: ${APP_KAFKA_MIN_ISR}
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: iot.sensor-health
  namespace: iot-pipeline
  labels:
    strimzi.io/cluster: app-kafka
spec:
  partitions: ${APP_KAFKA_TUNNEL_PARTITIONS}
  replicas: ${APP_KAFKA_REPLICATION_FACTOR}
  config:
    retention.ms: 604800000
    min.insync.replicas: ${APP_KAFKA_MIN_ISR}
---
# Learned tunnel geometry of the consensus service: latest value per tunnel survives rebalances.
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: iot.consensus.geometry
  namespace: iot-pipeline
  labels:
    strimzi.io/cluster: app-kafka
spec:
  partitions: 1
  replicas: ${APP_KAFKA_REPLICATION_FACTOR}
  config:
    cleanup.policy: compact
    min.insync.replicas: ${APP_KAFKA_MIN_ISR}

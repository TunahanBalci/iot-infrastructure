# KEDA for consensus. Not the Kafka-lag trigger: consensus commits only up to its oldest pending
# (not yet fused) detection, so the committed offset trails the read position by minutes on purpose
# and KEDA's Kafka scaler would see permanent lag. Instead the consensus pods export
# consensus_kafka_lag_records (high watermark - read position) and KEDA queries VictoriaMetrics.
# Without the observability layer, scripts/install/80-processing.sh runs CONSENSUS_MIN_REPLICAS
# fixed replicas instead.
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: consensus
  namespace: iot-pipeline
spec:
  scaleTargetRef:
    name: consensus
  minReplicaCount: ${CONSENSUS_MIN_REPLICAS}
  maxReplicaCount: ${CONSENSUS_MAX_REPLICAS}
  pollingInterval: 30
  cooldownPeriod: 300
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleUp:
          stabilizationWindowSeconds: 120   # one rebalance per sustained backlog, not per spike
          policies:
            - type: Pods
              value: 1
              periodSeconds: 120
        scaleDown:
          stabilizationWindowSeconds: 600
          policies:
            - type: Pods
              value: 1
              periodSeconds: 300
  triggers:
    - type: prometheus
      metadata:
        serverAddress: ${VICTORIAMETRICS_URL}
        query: sum(consensus_kafka_lag_records{namespace="iot-pipeline"})
        threshold: "${CONSENSUS_LAG_THRESHOLD}"
        ignoreNullValues: "true"          # no series yet (fresh install): keep current replicas

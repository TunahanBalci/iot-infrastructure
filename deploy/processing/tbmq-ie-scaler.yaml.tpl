# KEDA: TBMQ integration executors scale on the lag of the integration message topics in TBMQ's
# internal Kafka (tbmq.msg.ie.<integration id>, one partition each). One trigger per integration
# (TBMQ_IE_SHARDS tunnel shards x 3 sensor positions); more executors than integrations would idle.
# Rendered by scripts/install/70-tbmq.sh once the integration ids are known.
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: tbmq-kafka-keda
  namespace: thingsboard-mqtt-broker
spec:
  secretTargetRef:                      # mTLS as KafkaUser keda-tbmq (internal Kafka has no ACLs)
    - parameter: ca
      name: tbmq-kafka-cluster-ca-cert
      key: ca.crt
    - parameter: cert
      name: keda-tbmq
      key: user.crt
    - parameter: key
      name: keda-tbmq
      key: user.key
---
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: tbmq-integration-executor
  namespace: thingsboard-mqtt-broker
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: StatefulSet
    name: tbmq-integration-executor
  minReplicaCount: ${TBMQ_IE_MIN_REPLICAS}
  maxReplicaCount: ${TBMQ_IE_MAX_REPLICAS}
  pollingInterval: 15
  cooldownPeriod: 300
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleDown:
          stabilizationWindowSeconds: 300
  triggers:
${IE_TRIGGERS_YAML}
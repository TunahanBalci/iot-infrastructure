#!/usr/bin/env bash
# Print records of an app Kafka topic live, as KafkaUser pipeline-viewer (read-only), from a short-lived
# console consumer pod in iot-pipeline. The pod is deleted on exit.
#   scripts/kafka-watch.sh [TOPIC]      # default iot.vehicles
source "$(dirname "$0")/lib.sh"
require kubectl

NS=iot-pipeline
topic=${1:-iot.vehicles}
api_ready || die "Kubernetes API not reachable"
k -n "$NS" get secret pipeline-viewer >/dev/null 2>&1 || die "KafkaUser pipeline-viewer missing — run: make install-app-kafka"

pod="pipeline-viewer-$(date +%s)"
trap 'kubectl -n "$NS" delete pod "$pod" --wait=false >/dev/null 2>&1 || true' EXIT
k apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $pod
  namespace: $NS
  labels:
    app: pipeline-viewer              # admitted by the app-kafka NetworkPolicy
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 1001
  containers:
    - name: consumer
      image: quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${KAFKA_VERSION}
      command: ["sh", "-c"]
      args:
        - |
          cat >/tmp/client.properties <<PROPS
          security.protocol=SASL_SSL
          sasl.mechanism=SCRAM-SHA-512
          sasl.jaas.config=\$SASL_JAAS_CONFIG
          ssl.truststore.type=PEM
          ssl.truststore.location=/etc/app-kafka/ca.crt
          PROPS
          exec /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server app-kafka-kafka-bootstrap:9093 \
            --consumer.config /tmp/client.properties --topic "$topic" --group "$pod" \
            --property print.key=true --property key.separator=' | '
      env:
        - name: SASL_JAAS_CONFIG
          valueFrom:
            secretKeyRef:
              name: pipeline-viewer
              key: sasl.jaas.config
      resources:
        limits:
          memory: 384Mi
      volumeMounts:
        - name: ca
          mountPath: /etc/app-kafka
          readOnly: true
  volumes:
    - name: ca
      secret:
        secretName: app-kafka-cluster-ca-cert
        items:
          - key: ca.crt
            path: ca.crt
YAML
kubectl -n "$NS" wait "pod/$pod" --for=condition=Ready --timeout=120s >/dev/null || die "viewer pod not ready: kubectl -n $NS describe pod $pod"
info "consuming $topic from now on (Ctrl-C to stop)"
kubectl -n "$NS" logs -f "$pod"

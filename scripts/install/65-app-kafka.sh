#!/usr/bin/env bash
# Step 65 — application Kafka (Strimzi app-kafka in iot-pipeline): topics, SCRAM users with ACLs.
# Idempotent: kustomize build | kubectl apply; Strimzi reconciles topics and users declaratively.
source "$(dirname "$0")/../lib.sh"
require kubectl

NS=iot-pipeline
step "65 app-kafka: Kafka x${APP_KAFKA_REPLICAS} in $NS, topics, SCRAM users + ACLs"
wait_api
for crd in kafkas.kafka.strimzi.io kafkatopics.kafka.strimzi.io kafkausers.kafka.strimzi.io; do
  crd_exists "$crd" || die "CRD $crd missing — run: make install-operators"
done

APP_KAFKA_REPLICATION_FACTOR=$((APP_KAFKA_REPLICAS < 3 ? APP_KAFKA_REPLICAS : 3))
APP_KAFKA_MIN_ISR=$((APP_KAFKA_REPLICATION_FACTOR > 1 ? APP_KAFKA_REPLICATION_FACTOR - 1 : 1))
export APP_KAFKA_REPLICATION_FACTOR APP_KAFKA_MIN_ISR

OUT="$GENERATED_DIR/app-kafka"
mkdir -p "$OUT"
for f in kustomization kafka topics; do
  render "$DEPLOY_DIR/app-kafka/$f.yaml.tpl" >"$OUT/$f.yaml"
done
k kustomize --load-restrictor=LoadRestrictionsNone "$OUT" >"$OUT/rendered.yaml"
k create namespace "$NS" --dry-run=client -o yaml | k apply -f - >/dev/null
k -n "$NS" apply -f "$DEPLOY_DIR/observability/strimzi-kafka-metrics.yaml" | sed 's/^/    /'  # Kafka metricsConfig
kube_apply "$OUT/rendered.yaml"

info "waiting for app-kafka (first start pulls images: a few minutes)"
kubectl -n "$NS" wait kafka/app-kafka --for=condition=Ready --timeout="${WAIT_TIMEOUT}s" >/dev/null ||
  die "app-kafka not ready: kubectl -n $NS describe kafka app-kafka"
ok "app-kafka ready (${APP_KAFKA_REPLICAS} node(s), RF ${APP_KAFKA_REPLICATION_FACTOR}, TLS + SCRAM-SHA-512 on 9093)"

kubectl -n "$NS" wait kafkatopic -l strimzi.io/cluster=app-kafka --for=condition=Ready --timeout="${WAIT_TIMEOUT}s" >/dev/null ||
  die "topics not ready: kubectl -n $NS get kafkatopic"
ok "topics ready: $(k -n "$NS" get kafkatopic -l strimzi.io/cluster=app-kafka -o jsonpath='{.items[*].metadata.name}')"
kubectl -n "$NS" wait kafkauser -l strimzi.io/cluster=app-kafka --for=condition=Ready --timeout="${WAIT_TIMEOUT}s" >/dev/null ||
  die "users not ready: kubectl -n $NS get kafkauser"
ok "SCRAM users ready: $(k -n "$NS" get kafkauser -l strimzi.io/cluster=app-kafka -o jsonpath='{.items[*].metadata.name}')"

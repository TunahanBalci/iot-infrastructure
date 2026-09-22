#!/usr/bin/env bash
# Step 80 — stream processing in iot-pipeline: telemetry-processor (stateless) and consensus (stateful),
# both consuming the app Kafka and scaled by KEDA on consumer-group lag.
# Idempotent: images rebuilt/imported only when apps/<service>/ changes (content-hash tags), manifests
# through kubectl apply, KEDA's Kafka login synced from its KafkaUser secret.
source "$(dirname "$0")/../lib.sh"
require kubectl docker

NS=iot-pipeline
step "80 processing: telemetry-processor ${PROCESSOR_MIN_REPLICAS}..${PROCESSOR_MAX_REPLICAS}, consensus ${CONSENSUS_MIN_REPLICAS}..${CONSENSUS_MAX_REPLICAS} (KEDA on Kafka lag)"
wait_api
crd_exists scaledobjects.keda.sh || die "KEDA missing — run: make install-operators"
k -n "$NS" get kafka/app-kafka >/dev/null 2>&1 || die "app Kafka missing — run: make install-app-kafka"
for u in telemetry-processor consensus keda; do
  k -n "$NS" get secret "$u" >/dev/null 2>&1 || die "KafkaUser secret $NS/$u missing — run: make install-app-kafka"
done
((PROCESSOR_MAX_REPLICAS <= APP_KAFKA_INGEST_PARTITIONS)) || die "PROCESSOR_MAX_REPLICAS > APP_KAFKA_INGEST_PARTITIONS: extra pods would idle"
((CONSENSUS_MAX_REPLICAS <= APP_KAFKA_TUNNEL_PARTITIONS)) || die "CONSENSUS_MAX_REPLICAS > APP_KAFKA_TUNNEL_PARTITIONS: extra pods would idle"

# The consensus service used to be a TBMQ MQTT client in its own namespace.
if k get namespace iot-consensus >/dev/null 2>&1; then
  warn "removing the old MQTT-based consensus (namespace iot-consensus)"
  kubectl delete namespace iot-consensus --wait=true --timeout="${WAIT_TIMEOUT}s" | sed 's/^/    /'
fi

# --- images: docker build → k3s containerd -------------------------------------------
PROCESSOR_IMAGE_REF=$(processor_image_ref)
CONSENSUS_IMAGE_REF=$(consensus_image_ref)
export PROCESSOR_IMAGE_REF CONSENSUS_IMAGE_REF
import_app_image "$PROCESSOR_IMAGE_REF" "$APPS_DIR/telemetry-processor"
import_app_image "$CONSENSUS_IMAGE_REF" "$APPS_DIR/consensus"

# --- KEDA's SCRAM login (secret never written outside a private temp dir) ---------------
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
(
  umask 077
  k -n "$NS" get secret keda -o jsonpath='{.data.password}' | base64 -d >"$tmp/password"
  k -n "$NS" get secret app-kafka-cluster-ca-cert -o jsonpath='{.data.ca\.crt}' | base64 -d >"$tmp/ca"
)
k -n "$NS" create secret generic keda-app-kafka \
  --from-literal=sasl=scram_sha512 --from-literal=username=keda --from-literal=tls=enable \
  --from-file=password="$tmp/password" --from-file=ca="$tmp/ca" \
  --dry-run=client -o yaml | k apply -f - | sed 's/^/    /'
rm -rf "$tmp"

# --- manifests ---------------------------------------------------------------------------
OTEL_ENDPOINT=""
[[ "$OBSERVABILITY_ENABLED" == true ]] && OTEL_ENDPOINT=http://otel-collector.monitoring.svc:4318
export OTEL_ENDPOINT
OUT="$GENERATED_DIR/processing"
mkdir -p "$OUT"
for f in keda-auth telemetry-processor consensus; do
  render "$DEPLOY_DIR/processing/$f.yaml.tpl" >"$OUT/$f.yaml"
done
kube_apply "$OUT/keda-auth.yaml"
kube_apply "$OUT/telemetry-processor.yaml"
kube_apply "$OUT/consensus.yaml"
# consensus scales on VictoriaMetrics (its committed Kafka offsets trail on purpose); without the
# observability layer it runs a fixed replica count.
if [[ "$OBSERVABILITY_ENABLED" == true ]]; then
  render "$DEPLOY_DIR/processing/consensus-scaler.yaml.tpl" >"$OUT/consensus-scaler.yaml"
  kube_apply "$OUT/consensus-scaler.yaml"
else
  k -n "$NS" delete scaledobject consensus --ignore-not-found >/dev/null
  k -n "$NS" scale deploy/consensus --replicas="$CONSENSUS_MIN_REPLICAS" >/dev/null
  warn "OBSERVABILITY_ENABLED=false: consensus fixed at $CONSENSUS_MIN_REPLICAS replica(s), no autoscaling"
fi

info "waiting for rollouts (ready = consumer group joined)"
if ! rollout "$NS" deploy/telemetry-processor >/dev/null; then
  k -n "$NS" logs -l app=telemetry-processor --tail 20 --prefix >&2 || true
  die "telemetry-processor not ready"
fi
ok "telemetry-processor ready (image $PROCESSOR_IMAGE_REF)"
if ! rollout "$NS" deploy/consensus >/dev/null; then
  k -n "$NS" logs -l app=consensus --tail 20 --prefix >&2 || true
  die "consensus not ready"
fi
ok "consensus ready (image $CONSENSUS_IMAGE_REF)"

kubectl -n "$NS" wait scaledobject/telemetry-processor --for=condition=Ready --timeout=120s >/dev/null ||
  die "ScaledObject telemetry-processor not ready: kubectl -n $NS describe scaledobject telemetry-processor"
ok "KEDA: telemetry-processor scales on Kafka lag (${PROCESSOR_LAG_THRESHOLD}/pod)"
if [[ "$OBSERVABILITY_ENABLED" == true ]]; then
  # The query only works once VictoriaMetrics scrapes consensus (install-observability runs after this).
  if kubectl -n "$NS" wait scaledobject/consensus --for=condition=Ready --timeout=60s >/dev/null 2>&1; then
    ok "KEDA: consensus scales on unread detections via VictoriaMetrics (${CONSENSUS_LAG_THRESHOLD}/pod)"
  else
    warn "ScaledObject consensus not Ready yet — expected until make install-observability has run"
  fi
fi
[[ -n "$OTEL_ENDPOINT" ]] && info "traces: $OTEL_ENDPOINT (sampling $TRACES_SAMPLE_RATIO)"
info "outputs: processor -> iot.detections, iot.sensor-health, iot.detections.rejected; consensus -> iot.vehicles, iot.traffic (make logs-consensus)"

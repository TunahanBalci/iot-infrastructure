#!/usr/bin/env bash
# Start the cluster and every workload in dependency order. Safe to re-run.
#   k3s → API → Cilium → nodes → cert-manager / Envoy Gateway / operators → Postgres (CNPG),
#   Kafka (Strimzi), Valkey, app Kafka → TBMQ → integration executor → storage → telemetry-processor,
#   consensus (KEDA resumed) → (optional) simulator
source "$(dirname "$0")/lib.sh"
require kubectl jq

NS=$TBMQ_NAMESPACE

step "up: k3s"
if ! has k3s; then die "k3s not installed — run: make install"; fi
if systemctl is-active --quiet k3s; then
  skip "k3s already running"
else
  info "starting k3s (sudo)"
  as_root systemctl start k3s
  ok "k3s started"
fi
wait_api
ok "API server ready"

step "up: networking"
k -n kube-system get ds cilium >/dev/null 2>&1 || die "Cilium not installed — run: make install"
rollout kube-system ds/cilium >/dev/null
rollout kube-system deploy/cilium-operator >/dev/null
ok "cilium ready"
ensure_nodes_ready
ok "nodes Ready"

step "up: platform"
for d in cert-manager cert-manager-webhook cert-manager-cainjector; do rollout cert-manager "deploy/$d" >/dev/null; done
ok "cert-manager ready"
rollout envoy-gateway-system deploy/envoy-gateway >/dev/null
k -n envoy-gateway-system wait --for=condition=Programmed gateway/edge --timeout="${WAIT_TIMEOUT}s" >/dev/null
ok "envoy gateway ready (edge on $EDGE_LB_IP)"
rollout strimzi-system deploy/strimzi-cluster-operator >/dev/null
rollout cnpg-system "$(k -n cnpg-system get deploy -l app.kubernetes.io/name=cloudnative-pg -o name)" >/dev/null
rollout reloader deploy/reloader-reloader >/dev/null
for d in keda-operator keda-operator-metrics-apiserver; do rollout keda "deploy/$d" >/dev/null; done
ok "operators ready (Strimzi, CloudNativePG, Reloader, KEDA)"

# scale_to <kind/name> <replicas>  — only touches workloads that differ
scale_to() {
  local current
  current=$(k -n "$NS" get "$1" -o jsonpath='{.spec.replicas}')
  if [[ "$current" != "$2" ]]; then
    k -n "$NS" scale "$1" --replicas="$2" >/dev/null
    info "scaled $1 $current → $2"
  fi
}

step "up: TBMQ data services"
k -n "$NS" get clusters.postgresql.cnpg.io/tbmq-db kafka/tbmq-kafka statefulset/tbmq-valkey >/dev/null ||
  die "data services missing — run: make install-tbmq-deps"
# Postgres: end CloudNativePG hibernation (instances come back on their PVCs).
if [[ "$(k -n "$NS" get clusters.postgresql.cnpg.io tbmq-db -o jsonpath='{.metadata.annotations.cnpg\.io/hibernation}')" == on ]]; then
  k -n "$NS" annotate clusters.postgresql.cnpg.io tbmq-db cnpg.io/hibernation=off --overwrite >/dev/null
  info "postgres: hibernation off"
fi
strimzi_start "$NS" tbmq-kafka
strimzi_start iot-pipeline app-kafka
scale_to statefulset/tbmq-valkey 1
retry "$WAIT_TIMEOUT" 5 postgres_instances_ready || die "postgres not ready: kubectl -n $NS describe clusters.postgresql.cnpg.io tbmq-db"
retry "$WAIT_TIMEOUT" 5 kafka_pods_ready || die "kafka not ready: kubectl -n $NS describe kafka tbmq-kafka"
rollout "$NS" statefulset/tbmq-valkey >/dev/null
ok "postgres, kafka, valkey ready"
if k -n iot-pipeline get kafka/app-kafka >/dev/null 2>&1; then
  retry "$WAIT_TIMEOUT" 5 strimzi_pods_ready iot-pipeline app-kafka "$APP_KAFKA_REPLICAS" ||
    die "app-kafka not ready: kubectl -n iot-pipeline describe kafka app-kafka"
  ok "app-kafka ready"
fi

step "up: TBMQ"
scale_to statefulset/tbmq "$TBMQ_REPLICAS"
rollout "$NS" statefulset/tbmq >/dev/null
ok "tbmq ready ($TBMQ_REPLICAS replicas)"
keda_resume "$NS" tbmq-integration-executor
scale_to statefulset/tbmq-integration-executor "$TBMQ_IE_MIN_REPLICAS"
rollout "$NS" statefulset/tbmq-integration-executor >/dev/null
ok "integration executor ready (KEDA resumed)"

if k get namespace iot-storage >/dev/null 2>&1; then
  step "up: storage"
  for w in seaweedfs clickhouse; do
    if k -n iot-storage get "statefulset/$w" >/dev/null 2>&1; then
      k -n iot-storage scale "statefulset/$w" --replicas=1 >/dev/null
      rollout iot-storage "statefulset/$w" >/dev/null
      ok "$w ready"
    fi
  done
fi

if k -n iot-pipeline get deploy/consensus >/dev/null 2>&1; then
  step "up: stream processing (KEDA resumed)"
  for d in telemetry-processor consensus; do
    keda_resume iot-pipeline "$d"
    retry "$WAIT_TIMEOUT" 5 deploy_has_ready iot-pipeline "$d" ||
      die "$d not ready: kubectl -n iot-pipeline describe deploy $d"
    ok "$d ready"
  done
fi

if [[ "${SIM:-false}" == true ]]; then
  "$ROOT_DIR/scripts/sim.sh" up
fi

"$ROOT_DIR/scripts/endpoints.sh"

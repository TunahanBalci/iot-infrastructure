#!/usr/bin/env bash
# Stop workloads in reverse dependency order (data on PVCs is kept; observability keeps running).
#   scripts/down.sh          # scale TBMQ stack to 0, cluster keeps running
#   scripts/down.sh --stop   # additionally stop k3s and all its containers
source "$(dirname "$0")/lib.sh"

NS=$TBMQ_NAMESPACE
STOP_K3S=false
[[ "${1:-}" == --stop ]] && STOP_K3S=true

if has docker && docker ps -q --filter "name=^${SIM_CONTAINER}\$" | grep -q .; then
  "$ROOT_DIR/scripts/sim.sh" down
fi

if systemctl is-active --quiet k3s && api_ready; then
  # Processing first: consumers commit and save consensus geometry while Kafka is still up.
  step "down: stream processing → 0 (KEDA paused)"
  keda_pause iot-pipeline telemetry-processor
  keda_pause iot-pipeline consensus
  k -n iot-pipeline wait --for=delete pod -l 'app in (telemetry-processor,consensus)' --timeout="${WAIT_TIMEOUT}s" >/dev/null 2>&1 ||
    warn "processing pods still terminating"
  ok "telemetry-processor, consensus at 0"

  if k get namespace iot-storage >/dev/null 2>&1; then
    step "down: storage → 0 replicas"
    for w in statefulset/clickhouse statefulset/seaweedfs; do
      if k -n iot-storage get "$w" >/dev/null 2>&1; then
        k -n iot-storage scale "$w" --replicas=0 >/dev/null
        ok "$w scaled to 0"
      fi
    done
  fi

  step "down: TBMQ stack → 0 replicas"
  keda_pause "$NS" tbmq-integration-executor
  for w in statefulset/tbmq-integration-executor statefulset/tbmq; do
    if k -n "$NS" get "$w" >/dev/null 2>&1; then
      k -n "$NS" scale "$w" --replicas=0 >/dev/null
      k -n "$NS" rollout status "$w" --timeout="${WAIT_TIMEOUT}s" >/dev/null 2>&1 || true
      ok "$w scaled to 0"
    fi
  done
  if k -n "$NS" get statefulset/tbmq-valkey >/dev/null 2>&1; then
    k -n "$NS" scale statefulset/tbmq-valkey --replicas=0 >/dev/null
    ok "statefulset/tbmq-valkey scaled to 0"
  fi
  strimzi_stop "$NS" tbmq-kafka
  strimzi_stop iot-pipeline app-kafka
  # CloudNativePG declarative hibernation: pods removed, PVCs kept.
  if k -n "$NS" get clusters.postgresql.cnpg.io/tbmq-db >/dev/null 2>&1; then
    k -n "$NS" annotate clusters.postgresql.cnpg.io tbmq-db cnpg.io/hibernation=on --overwrite >/dev/null
    ok "postgres tbmq-db hibernated"
  fi
  info "waiting for pods to terminate (observability keeps running)"
  for ns in "$NS" iot-pipeline iot-storage; do
    k get namespace "$ns" >/dev/null 2>&1 || continue
    k -n "$ns" wait --for=delete pod --all --timeout="${WAIT_TIMEOUT}s" >/dev/null 2>&1 || warn "some pods in $ns still terminating"
  done
else
  skip "k3s not running — workloads already down"
fi

if $STOP_K3S; then
  step "down: stopping k3s (sudo)"
  if systemctl is-active --quiet k3s; then
    as_root systemctl stop k3s
    ok "k3s service stopped"
  fi
  # systemctl stop leaves pod containers running (KillMode=process).
  if [[ -x /usr/local/bin/k3s-killall.sh ]]; then
    as_root /usr/local/bin/k3s-killall.sh >/dev/null 2>&1
    ok "k3s containers stopped (k3s-killall.sh)"
  fi
  info "start again with: make up"
fi

#!/usr/bin/env bash
# Step 90 — observability in namespace monitoring: VictoriaMetrics (VMSingle, vmagent, vmalert, Alertmanager,
# Grafana, kube-state-metrics, node-exporter), VictoriaLogs + Vector, VictoriaTraces, OpenTelemetry Collector;
# scrape targets, alert rules and IoT dashboards from deploy/observability. Skipped unless
# OBSERVABILITY_ENABLED=true.
# Idempotent: CRDs via server-side apply, helm upgrade --install per chart, kustomize | kubectl apply --server-side.
source "$(dirname "$0")/../lib.sh"

NS=monitoring
VM_REPO=https://victoriametrics.github.io/helm-charts
OTEL_REPO=https://open-telemetry.github.io/opentelemetry-helm-charts
OBS_DIR="$DEPLOY_DIR/observability"

step "90 observability: VictoriaMetrics ${OBS_METRICS_RETENTION} / VictoriaLogs ${OBS_LOGS_RETENTION} / VictoriaTraces ${OBS_TRACES_RETENTION}, Grafana, OTel Collector"
if [[ "$OBSERVABILITY_ENABLED" != true ]]; then
  skip "OBSERVABILITY_ENABLED=$OBSERVABILITY_ENABLED — nothing to do"
  exit 0
fi
require kubectl helm jq openssl sha256sum envsubst
wait_api

# Storage size values are only accepted as Kubernetes quantities (the charts take GiB numbers for
# the disk-usage caps: 80% of the PVC).
gib() { # gib <quantity Mi|Gi> -> whole GiB (min 1)
  local q=$1 n
  case "$q" in
    *Gi) n=${q%Gi} ;;
    *Mi) n=$(( ${q%Mi} / 1024 )) ;;
    *) die "unsupported storage quantity '$q' (use Mi or Gi)" ;;
  esac
  echo $(( n * 8 / 10 > 0 ? n * 8 / 10 : 1 ))
}

k create namespace "$NS" --dry-run=client -o yaml | k apply -f - >/dev/null
k label namespace "$NS" app.kubernetes.io/part-of=iot-infrastructure --overwrite >/dev/null

# --- VictoriaMetrics operator CRDs ---------------------------------------------------------------------
# Helm never upgrades chart CRDs: apply them server-side (they exceed the client-side annotation limit).
info "applying VictoriaMetrics operator CRDs (victoria-metrics-k8s-stack $VM_K8S_STACK_CHART_VERSION)"
helm show crds victoria-metrics-k8s-stack --repo "$VM_REPO" --version "$VM_K8S_STACK_CHART_VERSION" |
  k apply --server-side --force-conflicts -f - >/dev/null
for crd in vmsingles vmagents vmalerts vmalertmanagers vmrules vmpodscrapes vmservicescrapes vmstaticscrapes vmnodescrapes; do
  wait_crd "$crd.operator.victoriametrics.com"
done
ok "VictoriaMetrics CRDs established"

# --- secrets: Grafana admin, ClickHouse datasource password ------------------------------------------
mkdir -p "$STATE_DIR"
pw_file="$STATE_DIR/grafana-admin-password"
if [[ -n "$GRAFANA_ADMIN_PASSWORD" ]]; then
  (umask 077 && printf '%s' "$GRAFANA_ADMIN_PASSWORD" >"$pw_file")
elif [[ ! -s "$pw_file" ]]; then
  existing=$(k -n "$NS" get secret grafana-admin -o jsonpath='{.data.admin-password}' 2>/dev/null | base64 -d || true)
  (umask 077 && if [[ -n "$existing" ]]; then printf '%s' "$existing"; else openssl rand -hex 16 | tr -d '\n'; fi >"$pw_file")
else
  # --from-file keeps every byte, so a trailing newline would become part of the password and nobody
  # could type it. Older runs wrote one: drop it (Grafana keeps no state, the roll below recreates admin).
  grafana_pw=$(<"$pw_file")
  (umask 077 && printf '%s' "$grafana_pw" >"$pw_file")
fi
k -n "$NS" create secret generic grafana-admin --from-literal=admin-user=admin --from-file=admin-password="$pw_file" \
  --dry-run=client -o yaml | k apply -f - >/dev/null
ok "Grafana admin password in .state/grafana-admin-password (secret $NS/grafana-admin)"

# Grafana reads the ClickHouse password from env (secret key "password"); copy the storage layer's
# secret. Missing (STORAGE_ENABLED=false or storage not installed yet): the datasource stays unusable.
ch_checksum=none
if ch_data=$(k -n iot-storage get secret clickhouse-grafana -o json 2>/dev/null | jq -c '.data // {}'); then
  jq -n --argjson data "$ch_data" --arg ns "$NS" '{apiVersion: "v1", kind: "Secret", type: "Opaque",
      metadata: {name: "clickhouse-grafana", namespace: $ns, labels: {"app.kubernetes.io/part-of": "iot-infrastructure"}},
      data: $data}' | k apply -f - >/dev/null
  jq -e 'has("password")' <<<"$ch_data" >/dev/null || warn "secret iot-storage/clickhouse-grafana has no key 'password' — ClickHouse datasource will not authenticate"
  ch_checksum=$(sha256sum <<<"$ch_data" | cut -c1-16)
  ok "copied secret iot-storage/clickhouse-grafana (ClickHouse datasource user grafana)"
else
  skip "secret iot-storage/clickhouse-grafana not found — ClickHouse datasource unusable until make install-storage and a re-run of this step"
fi

# --- victoria-metrics-k8s-stack ----------------------------------------------------------------------
# Datasources are provisioned by an init container and passwords are read at start: roll Grafana when the
# values, the ClickHouse secret or the admin password change.
ds_checksum=$( { cat "$OBS_DIR/victoria-metrics-k8s-stack.yaml" "$pw_file"; echo "$ch_checksum"; } | sha256sum | cut -c1-16)
# Uninstalling this release removes the operator and its VMSingle/VMAgent/... CRs at the same time, and
# the finalizer the operator puts on those CRs then has nobody left to clear it: the uninstall waits
# forever. Delete the CRs first, while the operator still runs.
if helm_never_deployed vm "$NS"; then
  k -n "$NS" delete vmsingle,vmagent,vmalert,vmalertmanager --all --ignore-not-found --timeout=120s >/dev/null 2>&1 ||
    k -n "$NS" get vmsingle,vmagent,vmalert,vmalertmanager -o name 2>/dev/null |
      xargs -r -I{} k -n "$NS" patch {} --type=merge -p '{"metadata":{"finalizers":null}}' >/dev/null 2>&1 || true
  helm uninstall vm -n "$NS" --wait --timeout "${WAIT_TIMEOUT}s" >/dev/null 2>&1 || true
fi
info "helm upgrade --install vm victoria-metrics-k8s-stack $VM_K8S_STACK_CHART_VERSION (dashboards/rules sync-job needs GitHub access)"
helm upgrade --install vm victoria-metrics-k8s-stack \
  --repo "$VM_REPO" --version "$VM_K8S_STACK_CHART_VERSION" \
  --namespace "$NS" --skip-crds \
  -f "$OBS_DIR/victoria-metrics-k8s-stack.yaml" \
  --set-string vmsingle.spec.retentionPeriod="$OBS_METRICS_RETENTION" \
  --set-string vmsingle.spec.storage.resources.requests.storage="$OBS_METRICS_STORAGE" \
  --set-string "grafana.podAnnotations.checksum/datasources=$ds_checksum" \
  --wait --timeout "${WAIT_TIMEOUT}s" >/dev/null
ok "release vm deployed (revision $(helm_revision vm "$NS"))"

cr_operational() { # cr_operational <kind/name>
  [[ "$(k -n "$NS" get "$1" -o jsonpath='{.status.updateStatus}' 2>/dev/null)" == operational ]]
}
for cr in vmsingle/vm vmagent/vm vmalert/vm vmalertmanager/vm; do
  retry "$WAIT_TIMEOUT" 5 cr_operational "$cr" ||
    die "$cr not operational: kubectl -n $NS describe $cr; kubectl -n $NS logs deploy/vm-victoria-metrics-operator"
done
ok "VMSingle, vmagent, vmalert, Alertmanager operational"

# --- VictoriaLogs + Vector, VictoriaTraces, OpenTelemetry Collector ---------------------------------
sts_recreate_if_storage_changed "$NS" vlogs-server "${OBS_LOGS_STORAGE}"
helm_clear_failed_install vlogs "$NS"
info "helm upgrade --install vlogs victoria-logs-single $VICTORIA_LOGS_CHART_VERSION (+ Vector DaemonSet)"
helm upgrade --install vlogs victoria-logs-single \
  --repo "$VM_REPO" --version "$VICTORIA_LOGS_CHART_VERSION" \
  --namespace "$NS" \
  -f "$OBS_DIR/victoria-logs-single.yaml" \
  --set-string server.retentionPeriod="$OBS_LOGS_RETENTION" \
  --set-string server.persistentVolume.size="$OBS_LOGS_STORAGE" \
  --set server.retentionDiskSpaceUsage="$(gib "$OBS_LOGS_STORAGE")" \
  --wait --timeout "${WAIT_TIMEOUT}s" >/dev/null
ok "VictoriaLogs and Vector ready"

sts_recreate_if_storage_changed "$NS" vtraces-server "${OBS_TRACES_STORAGE}"
helm_clear_failed_install vtraces "$NS"
info "helm upgrade --install vtraces victoria-traces-single $VICTORIA_TRACES_CHART_VERSION"
helm upgrade --install vtraces victoria-traces-single \
  --repo "$VM_REPO" --version "$VICTORIA_TRACES_CHART_VERSION" \
  --namespace "$NS" \
  -f "$OBS_DIR/victoria-traces-single.yaml" \
  --set-string server.retentionPeriod="$OBS_TRACES_RETENTION" \
  --set-string server.persistentVolume.size="$OBS_TRACES_STORAGE" \
  --set server.retentionDiskSpaceUsage="$(gib "$OBS_TRACES_STORAGE")" \
  --wait --timeout "${WAIT_TIMEOUT}s" >/dev/null
ok "VictoriaTraces ready"

helm_clear_failed_install otel-collector "$NS"
info "helm upgrade --install otel-collector opentelemetry-collector $OTEL_COLLECTOR_CHART_VERSION"
helm upgrade --install otel-collector opentelemetry-collector \
  --repo "$OTEL_REPO" --version "$OTEL_COLLECTOR_CHART_VERSION" \
  --namespace "$NS" \
  -f "$OBS_DIR/otel-collector.yaml" \
  --wait --timeout "${WAIT_TIMEOUT}s" >/dev/null
ok "OTel Collector ready (otel-collector.$NS.svc: OTLP gRPC 4317, HTTP 4318)"

# --- scrape targets, alert rules, dashboards ---------------------------------------------------------
OUT="$GENERATED_DIR/observability"
mkdir -p "$OUT"
# The simulator target carries the node address (it runs on the host, outside the cluster), so it is
# rendered from a template instead of being kustomized.
{ k kustomize "$OBS_DIR"; echo '---'; render "$OBS_DIR/simulator-scrape.yaml.tpl"; } >"$OUT/rendered.yaml"
# The operator's validating webhook can refuse requests for a few seconds after it starts.
retry 120 5 kubectl apply --server-side --dry-run=server -f "$OUT/rendered.yaml" || true
k apply --server-side --force-conflicts -f "$OUT/rendered.yaml" | sed 's/^/    /'
ok "scrape targets, VMRule iot-platform and IoT dashboards applied"
info "simulator scraped on ${K3S_NODE_IP}:${SIM_METRICS_PORT} while it runs (make sim-up); its target is down otherwise, by design"

# Dashboards and scrape objects removed from deploy/observability (the chart-managed ones are not labelled).
current=$(grep -E '^  name: ' "$OUT/rendered.yaml" | awk '{print $2}' | sort -u)
for kind in configmap vmpodscrape vmstaticscrape vmrule; do
  sel=app.kubernetes.io/part-of=iot-infrastructure
  [[ $kind == configmap ]] && sel="$sel,grafana_dashboard=1"
  for name in $(k -n "$NS" get "$kind" -l "$sel" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    grep -qx "$name" <<<"$current" || { k -n "$NS" delete "$kind" "$name" >/dev/null; ok "removed stale $kind/$name"; }
  done
done

grafana_ready() { [[ "$(k -n "$NS" get deploy vm-grafana -o jsonpath='{.status.readyReplicas}')" == 1 ]]; }
retry 300 5 grafana_ready || warn "Grafana not ready yet (plugins are downloaded at start): kubectl -n $NS logs deploy/vm-grafana -c grafana"

# --- summary -------------------------------------------------------------------------------------------
cat <<EOF

    ${C_BOLD}Observability UIs (port-forward, then open the URL):${C_RESET}
    Grafana        kubectl -n $NS port-forward svc/vm-grafana 3000:80              http://localhost:3000
                   login admin / \$(cat .state/grafana-admin-password)   dashboards: folder "IoT"
    vmalert        kubectl -n $NS port-forward svc/vmalert-vm 8880:8080            http://localhost:8880/vmalert/groups
    Alertmanager   kubectl -n $NS port-forward svc/vmalertmanager-vm 9093:9093     http://localhost:9093
    VictoriaMetrics kubectl -n $NS port-forward svc/vmsingle-vm 8428:8428          http://localhost:8428/vmui
    vmagent targets kubectl -n $NS port-forward svc/vmagent-vm 8429:8429           http://localhost:8429/targets
    VictoriaLogs   kubectl -n $NS port-forward svc/vlogs-server 9428:9428          http://localhost:9428/select/vmui
    VictoriaTraces kubectl -n $NS port-forward svc/vtraces-server 10428:10428      http://localhost:10428/select/vmui (or Grafana Explore)
    OTLP endpoint  http://otel-collector.$NS.svc:4318 (HTTP), otel-collector.$NS.svc:4317 (gRPC)
EOF

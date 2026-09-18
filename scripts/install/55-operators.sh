#!/usr/bin/env bash
# Step 55 — operators: Strimzi (Kafka), CloudNativePG (Postgres), Reloader (restart on renewed
# certificates), KEDA (workload autoscaling on Kafka lag).
# Idempotent: CRDs via server-side apply, helm upgrade --install.
source "$(dirname "$0")/../lib.sh"
require kubectl helm

step "55 operators: Strimzi $STRIMZI_VERSION, CloudNativePG chart $CNPG_CHART_VERSION, Reloader chart $RELOADER_CHART_VERSION, KEDA $KEDA_VERSION"
wait_api

# --- Strimzi --------------------------------------------------------------------
# Helm never upgrades chart crds/, so apply them explicitly (server-side: they exceed the
# client-side last-applied annotation limit).
STRIMZI_CHART=oci://quay.io/strimzi-helm/strimzi-kafka-operator
info "applying Strimzi CRDs ($STRIMZI_VERSION)"
# helm show crds concatenates the CRD files without document separators.
helm show crds "$STRIMZI_CHART" --version "$STRIMZI_VERSION" 2> >(grep -vE '^(Pulled|Digest):' >&2 || true) |
  awk '/^apiVersion: / && NR > 1 { print "---" } { print }' |
  k apply --server-side --force-conflicts -f - >/dev/null
for crd in kafkas.kafka.strimzi.io kafkanodepools.kafka.strimzi.io; do wait_crd "$crd"; done
ok "Strimzi CRDs established"

helm_clear_failed_install strimzi strimzi-system
info "helm upgrade --install strimzi $STRIMZI_VERSION"
helm upgrade --install strimzi "$STRIMZI_CHART" \
  --version "$STRIMZI_VERSION" \
  --namespace strimzi-system --create-namespace \
  --skip-crds \
  -f "$DEPLOY_DIR/operators/strimzi-values.yaml" \
  --wait --timeout "${WAIT_TIMEOUT}s" >/dev/null 2> >(grep -vE '^(Pulled|Digest):' >&2 || true)
rollout strimzi-system deploy/strimzi-cluster-operator >/dev/null
ok "strimzi-cluster-operator ready (watches all namespaces)"

# --- CloudNativePG --------------------------------------------------------------
# The chart templates its CRDs (crds.create), so helm upgrades them.
helm_clear_failed_install cnpg cnpg-system
info "helm upgrade --install cnpg $CNPG_CHART_VERSION"
helm upgrade --install cnpg cloudnative-pg \
  --repo https://cloudnative-pg.github.io/charts \
  --version "$CNPG_CHART_VERSION" \
  --namespace cnpg-system --create-namespace \
  -f "$DEPLOY_DIR/operators/cnpg-values.yaml" \
  --wait --timeout "${WAIT_TIMEOUT}s" >/dev/null
wait_crd clusters.postgresql.cnpg.io
cnpg_deploy=$(k -n cnpg-system get deploy -l app.kubernetes.io/name=cloudnative-pg -o name)
[[ -n "$cnpg_deploy" ]] || die "CloudNativePG operator deployment not found in cnpg-system"
rollout cnpg-system "$cnpg_deploy" >/dev/null
ok "CloudNativePG operator ready ($cnpg_deploy)"

# --- Reloader -------------------------------------------------------------------
helm_clear_failed_install reloader reloader
info "helm upgrade --install reloader $RELOADER_CHART_VERSION"
helm upgrade --install reloader reloader \
  --repo https://stakater.github.io/stakater-charts \
  --version "$RELOADER_CHART_VERSION" \
  --namespace reloader --create-namespace \
  -f "$DEPLOY_DIR/operators/reloader-values.yaml" \
  --wait --timeout "${WAIT_TIMEOUT}s" >/dev/null
rollout reloader deploy/reloader-reloader >/dev/null
ok "Reloader ready (rolls annotated workloads when their secrets change)"

# --- KEDA -----------------------------------------------------------------------
helm_clear_failed_install keda keda
info "helm upgrade --install keda $KEDA_VERSION"
helm upgrade --install keda keda \
  --repo https://kedacore.github.io/charts \
  --version "$KEDA_VERSION" \
  --namespace keda --create-namespace \
  -f "$DEPLOY_DIR/operators/keda-values.yaml" \
  --wait --timeout "${WAIT_TIMEOUT}s" >/dev/null
wait_crd scaledobjects.keda.sh
for d in keda-operator keda-operator-metrics-apiserver keda-admission-webhooks; do rollout keda "deploy/$d" >/dev/null; done
ok "KEDA ready (ScaledObjects scale consumers on Kafka lag)"

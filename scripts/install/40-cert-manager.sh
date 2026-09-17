#!/usr/bin/env bash
# Step 40 — cert-manager plus local CA ClusterIssuers: iot-ca (servers), iot-device-ca (MQTT clients).
# Idempotent: helm upgrade --install + kubectl apply.
source "$(dirname "$0")/../lib.sh"
require kubectl helm envsubst

step "40 cert-manager: TLS certificates"
wait_api

helm_clear_failed_install cert-manager cert-manager
info "helm upgrade --install cert-manager $CERT_MANAGER_VERSION"
helm upgrade --install cert-manager oci://quay.io/jetstack/charts/cert-manager \
  --version "$CERT_MANAGER_VERSION" \
  --namespace cert-manager --create-namespace \
  --set crds.enabled=true \
  --wait --timeout "${WAIT_TIMEOUT}s" >/dev/null 2> >(grep -vE '^(Pulled|Digest):' >&2 || true)
ok "cert-manager release deployed"

rollout cert-manager deploy/cert-manager-webhook >/dev/null
ok "cert-manager webhook ready"

mkdir -p "$GENERATED_DIR"
render "$DEPLOY_DIR/cert-manager/issuers.yaml.tpl" >"$GENERATED_DIR/issuers.yaml"
# The webhook can refuse requests for a few seconds after becoming Ready.
retry 120 5 kubectl apply -f "$GENERATED_DIR/issuers.yaml" || k apply -f "$GENERATED_DIR/issuers.yaml"
ok "issuers applied"

k -n cert-manager wait --for=condition=Ready certificate/iot-root-ca --timeout="${WAIT_TIMEOUT}s" >/dev/null
k wait --for=condition=Ready clusterissuer/iot-ca --timeout="${WAIT_TIMEOUT}s" >/dev/null
ok "ClusterIssuer iot-ca ready (root CA secret cert-manager/iot-root-ca)"
k -n cert-manager wait --for=condition=Ready certificate/iot-device-ca --timeout="${WAIT_TIMEOUT}s" >/dev/null
k wait --for=condition=Ready clusterissuer/iot-device-ca --timeout="${WAIT_TIMEOUT}s" >/dev/null
ok "ClusterIssuer iot-device-ca ready (device client CA secret cert-manager/iot-device-ca)"

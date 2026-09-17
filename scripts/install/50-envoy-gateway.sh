#!/usr/bin/env bash
# Step 50 — Gateway API CRDs, Envoy Gateway controller, edge Gateway on EDGE_LB_IP.
# Idempotent: server-side apply for CRDs, helm upgrade --install, kubectl apply.
source "$(dirname "$0")/../lib.sh"
require kubectl helm envsubst jq

step "50 envoy-gateway: L7 edge on ${EDGE_LB_IP}"
wait_api
crd_exists certificates.cert-manager.io || die "cert-manager missing — run: make install-cert-manager"

# Gateway API + Envoy Gateway CRDs. Server-side apply with force-conflicts takes
# ownership from whoever installed older Gateway API CRDs (e.g. k3s traefik).
info "applying Gateway API + Envoy Gateway CRDs ($ENVOY_GATEWAY_VERSION)"
helm template eg-crds oci://docker.io/envoyproxy/gateway-crds-helm \
  --version "$ENVOY_GATEWAY_VERSION" \
  --set crds.gatewayAPI.enabled=true \
  --set crds.gatewayAPI.channel=standard \
  --set crds.envoyGateway.enabled=true 2> >(grep -vE '^(Pulled|Digest):' >&2 || true) |
  k apply --server-side --force-conflicts -f - >/dev/null
for crd in gateways.gateway.networking.k8s.io httproutes.gateway.networking.k8s.io \
           envoyproxies.gateway.envoyproxy.io backendtrafficpolicies.gateway.envoyproxy.io; do
  wait_crd "$crd"
done
ok "CRDs established (Gateway API $(k get crd gateways.gateway.networking.k8s.io -o jsonpath='{.metadata.annotations.gateway\.networking\.k8s\.io/bundle-version}'))"

helm_clear_failed_install eg envoy-gateway-system
info "helm upgrade --install eg $ENVOY_GATEWAY_VERSION"
helm upgrade --install eg oci://docker.io/envoyproxy/gateway-helm \
  --version "$ENVOY_GATEWAY_VERSION" \
  --namespace envoy-gateway-system --create-namespace \
  --skip-crds \
  --wait --timeout "${WAIT_TIMEOUT}s" >/dev/null 2> >(grep -vE '^(Pulled|Digest):' >&2 || true)
rollout envoy-gateway-system deploy/envoy-gateway >/dev/null
ok "envoy-gateway controller ready"

nodes=$(k get nodes --no-headers | wc -l)
export ENVOY_REPLICAS=$((nodes > 1 ? 2 : 1))
mkdir -p "$GENERATED_DIR"
render "$DEPLOY_DIR/envoy-gateway/edge.yaml.tpl" >"$GENERATED_DIR/edge.yaml"
k apply -f "$GENERATED_DIR/edge.yaml" | sed 's/^/    /'

k -n envoy-gateway-system wait --for=condition=Ready certificate/edge-tls --timeout="${WAIT_TIMEOUT}s" >/dev/null
ok "edge TLS certificate issued"
k wait --for=condition=Accepted gatewayclass/envoy --timeout="${WAIT_TIMEOUT}s" >/dev/null
ok "GatewayClass envoy accepted"
k -n envoy-gateway-system wait --for=condition=Programmed gateway/edge --timeout="${WAIT_TIMEOUT}s" >/dev/null
addr=$(k -n envoy-gateway-system get gateway edge -o jsonpath='{.status.addresses[0].value}')
[[ "$addr" == "$EDGE_LB_IP" ]] || die "gateway address is '$addr', expected $EDGE_LB_IP"
ok "Gateway edge programmed on $addr (${ENVOY_REPLICAS} envoy replica(s))"

# The controller propagates allocateLoadBalancerNodePorts=false from the EnvoyProxy; then drop
# NodePorts allocated before that.
envoy_svc=$(k -n envoy-gateway-system get svc -l gateway.envoyproxy.io/owning-gateway-name=edge -o jsonpath='{.items[0].metadata.name}')
node_ports_disabled() {
  [[ "$(k -n envoy-gateway-system get svc "$envoy_svc" -o jsonpath='{.spec.allocateLoadBalancerNodePorts}')" == false ]]
}
retry 120 2 node_ports_disabled || die "service $envoy_svc still allocates NodePorts"
release_lb_node_ports envoy-gateway-system "$envoy_svc"
ok "edge service $envoy_svc: LB IP only, no NodePorts"

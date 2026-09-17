#!/usr/bin/env bash
# Step 20 — Cilium CNI with kube-proxy replacement, Maglev LB, LB-IPAM, Hubble.
# Idempotent: helm upgrade --install; agents restart only when the release changed.
source "$(dirname "$0")/../lib.sh"
require kubectl helm jq cilium

step "20 cilium: CNI + kube-proxy replacement + L4 load balancer"
wait_api

# Maglev hash seed must be identical on every node and stable across upgrades.
mkdir -p "$STATE_DIR"
SEED_FILE="$STATE_DIR/maglev-hash-seed"
if [[ ! -s "$SEED_FILE" ]]; then
  existing=$(k -n kube-system get cm cilium-config -o jsonpath='{.data.bpf-lb-maglev-hash-seed}' 2>/dev/null || true)
  if [[ -n "$existing" ]]; then
    printf '%s' "$existing" >"$SEED_FILE"
    ok "reusing Maglev hash seed from running cluster"
  else
    head -c12 /dev/urandom | base64 -w0 >"$SEED_FILE"
    ok "generated Maglev hash seed ($SEED_FILE)"
  fi
fi

args=(
  --namespace kube-system
  --version "$CILIUM_VERSION"
  --repo https://helm.cilium.io
  -f "$DEPLOY_DIR/cilium/values.yaml"
  --set-string k8sServiceHost="$K8S_SERVICE_HOST"
  --set-string k8sServicePort="$K8S_SERVICE_PORT"
  --set routingMode="$CILIUM_ROUTING_MODE"
  --set loadBalancer.mode="$CILIUM_LB_MODE"
  --set loadBalancer.acceleration="$CILIUM_LB_ACCELERATION"
  --set operator.replicas="$CILIUM_OPERATOR_REPLICAS"
  --set-string maglev.hashSeed="$(cat "$SEED_FILE")"
)
if [[ "$CILIUM_ROUTING_MODE" == native ]]; then
  args+=(--set ipv4NativeRoutingCIDR="$K3S_CLUSTER_CIDR" --set autoDirectNodeRoutes=true)
fi
[[ -n "$CILIUM_DEVICES" ]] && args+=(--set-string devices="$CILIUM_DEVICES")

# helm upgrade always bumps the revision (and regenerates Hubble certs), so
# only run it when the desired release differs from what was last applied.
desired_hash=$( { printf '%s\n' "${args[@]}"; cat "$DEPLOY_DIR/cilium/values.yaml"; } | sha256sum | cut -d' ' -f1)
HASH_FILE="$STATE_DIR/cilium-release.sha256"
release_status=$(helm status cilium -n kube-system -o json 2>/dev/null | jq -r '.info.status' || echo missing)
chart_version=$(helm list -n kube-system -f '^cilium$' -o json 2>/dev/null | jq -r '.[0].chart // ""' || true)

if [[ "$release_status" == deployed && "$chart_version" == "cilium-$CILIUM_VERSION" \
      && -f "$HASH_FILE" && "$(cat "$HASH_FILE")" == "$desired_hash" ]]; then
  skip "cilium $CILIUM_VERSION release up to date"
else
  info "helm upgrade --install cilium $CILIUM_VERSION (release status: $release_status)"
  helm upgrade --install cilium cilium "${args[@]}" >/dev/null
  printf '%s' "$desired_hash" >"$HASH_FILE"
  ok "cilium release applied (revision $(helm_revision cilium kube-system))"
fi

# Order matters: agents and operator run on the host network and tolerate the
# not-ready taint; hubble/CoreDNS can't schedule until nodes are Ready. Waiting
# on full `cilium status` first would deadlock.
info "waiting for cilium agents and operator (image pulls can take a few minutes)"
rollout kube-system ds/cilium >/dev/null
rollout kube-system deploy/cilium-operator >/dev/null
ok "cilium agents and operator ready"

info "waiting for nodes to become Ready"
ensure_nodes_ready
ok "all nodes Ready"

# Pods started before Cilium (old flannel IPs, or none) are unmanaged: no
# CiliumEndpoint, no connectivity. Recreate the ones owned by a controller.
wait_crd ciliumendpoints.cilium.io

# A CiliumEndpoint left over from an earlier incarnation of a pod (different IP, node address of the
# day) blocks the agent: "sync-to-k8s-ciliumendpoint ... cannot take ownership of CEP that is not
# local". The agent rebuilds the CEP from its own endpoint state once the stale object is gone.
mapfile -t stale_cep < <(
  join -j 1 -o 1.1,1.2,2.2 \
    <(k get ciliumendpoints -A -o json | jq -r '.items[]
        | select(.status.networking.addressing[0].ipv4 != null)
        | "\(.metadata.namespace)/\(.metadata.name) \(.status.networking.addressing[0].ipv4)"' | sort) \
    <(k get pods -A -o json | jq -r '.items[]
        | select(.spec.hostNetwork != true and .status.podIP != null)
        | "\(.metadata.namespace)/\(.metadata.name) \(.status.podIP)"' | sort) |
    awk '$2 != $3 { print $1 }'
)
for p in "${stale_cep[@]}"; do
  k -n "${p%/*}" delete ciliumendpoint "${p#*/}" >/dev/null
  ok "removed stale CiliumEndpoint $p (pod has a different IP now)"
done
mapfile -t unmanaged < <(
  comm -23 \
    <(k get pods -A -o json | jq -r '.items[]
        | select(.spec.hostNetwork != true and .status.phase == "Running")
        | "\(.metadata.namespace)/\(.metadata.name)"' | sort) \
    <(k get ciliumendpoints -A -o json | jq -r '.items[] | "\(.metadata.namespace)/\(.metadata.name)"' | sort)
)
if ((${#unmanaged[@]} == 0)); then
  skip "no unmanaged pods"
else
  for p in "${unmanaged[@]}"; do
    ns=${p%/*} name=${p#*/}
    if [[ "$(k -n "$ns" get pod "$name" -o jsonpath='{.metadata.ownerReferences[0].kind}')" != "" ]]; then
      k -n "$ns" delete pod "$name" --wait=false >/dev/null
      ok "recreating unmanaged pod $p"
    else
      warn "unmanaged bare pod $p has no controller — delete it manually"
    fi
  done
fi

# CoreDNS forwards to the resolver the host had when its pod started. On a laptop that upstream is
# gone after a network change, and nothing in the cluster can resolve an external name any more.
if deploy_has_ready kube-system coredns; then
  if cluster_dns_resolves_externally; then
    ok "cluster DNS resolves $DNS_PROBE_NAME"
  else
    info "cluster DNS cannot resolve $DNS_PROBE_NAME (stale upstream): restarting CoreDNS"
    k -n kube-system rollout restart deploy/coredns >/dev/null
    rollout kube-system deploy/coredns >/dev/null
    retry 60 5 cluster_dns_resolves_externally &&
      ok "cluster DNS resolves $DNS_PROBE_NAME after the restart" ||
      warn "cluster DNS still cannot resolve $DNS_PROBE_NAME — check the host resolver and CoreDNS logs"
  fi
fi

info "waiting for hubble and overall cilium health"
if rollout kube-system deploy/hubble-relay >/dev/null 2>&1 && cilium status --wait --wait-duration 180s >/dev/null 2>&1; then
  ok "cilium status healthy (hubble included)"
else
  warn "cilium core is up but status is not fully healthy yet — check: cilium status"
fi

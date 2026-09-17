#!/usr/bin/env bash
# Step 10 — k3s server configured for Cilium (no flannel, kube-proxy, traefik, servicelb).
# Idempotent: installs only when missing or version differs; restarts only when config changed.
source "$(dirname "$0")/../lib.sh"

step "10 k3s: cluster (node IP ${K3S_NODE_IP} on ${K3S_NODE_IP_IFACE})"

[[ -n "$K3S_NODE_IP" && -n "$K3S_NODE_IP_IFACE" ]] || die "K3S_NODE_IP and K3S_NODE_IP_IFACE must be set"
DROPIN_DIR=/etc/rancher/k3s/config.yaml.d
DROPIN="$DROPIN_DIR/50-iot-infra.yaml"
mkdir -p "$GENERATED_DIR"
render "$DEPLOY_DIR/k3s/50-iot-infra.yaml.tpl" >"$GENERATED_DIR/k3s-50-iot-infra.yaml"
render "$DEPLOY_DIR/k3s/iot-node-ip.service.tpl" >"$GENERATED_DIR/iot-node-ip.service"

node_internal_ip() {
  k get node -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null
}
previous_node_ip=""
if systemctl is-active --quiet k3s && api_ready; then previous_node_ip=$(node_internal_ip); fi

# --- stable node address: dummy interface, created before k3s starts ---
config_changed=false
units_changed=false
install_if_changed() { # install_if_changed <src> <dest>
  if [[ -f "$2" ]] && cmp -s "$1" "$2"; then return 1; fi
  as_root mkdir -p "$(dirname "$2")"
  as_root install -m 0644 "$1" "$2"
}
if install_if_changed "$GENERATED_DIR/iot-node-ip.service" /etc/systemd/system/iot-node-ip.service; then
  info "wrote /etc/systemd/system/iot-node-ip.service (sudo)"
  units_changed=true
fi
if install_if_changed "$DEPLOY_DIR/k3s/k3s-iot-node-ip.conf" /etc/systemd/system/k3s.service.d/10-iot-node-ip.conf; then
  info "wrote /etc/systemd/system/k3s.service.d/10-iot-node-ip.conf (sudo)"
  units_changed=true
  config_changed=true
fi
if $units_changed; then
  as_root systemctl daemon-reload
fi
if $units_changed || ! systemctl is-active --quiet iot-node-ip; then
  as_root systemctl enable iot-node-ip.service >/dev/null 2>&1
  as_root systemctl restart iot-node-ip.service
fi
ip -4 -o addr show dev "$K3S_NODE_IP_IFACE" 2>/dev/null | grep -q " $K3S_NODE_IP/32 " ||
  die "$K3S_NODE_IP not on $K3S_NODE_IP_IFACE: systemctl status iot-node-ip"
ok "node address $K3S_NODE_IP on $K3S_NODE_IP_IFACE (iot-node-ip.service)"

# --- config drop-in (k3s merges /etc/rancher/k3s/config.yaml.d/*.yaml) ---
if [[ -f "$DROPIN" ]] && cmp -s "$DROPIN" "$GENERATED_DIR/k3s-50-iot-infra.yaml"; then
  skip "config drop-in $DROPIN up to date"
else
  info "writing $DROPIN (sudo)"
  as_root mkdir -p "$DROPIN_DIR"
  as_root install -m 0644 "$GENERATED_DIR/k3s-50-iot-infra.yaml" "$DROPIN"
  config_changed=true
  ok "config drop-in written"
fi

# --- binary / service ---
installed_version=""
has k3s && installed_version=$(k3s --version 2>/dev/null | awk 'NR==1{print $3}')

if [[ -z "$installed_version" ]] || [[ -n "$K3S_VERSION" && "$installed_version" != "$K3S_VERSION" ]]; then
  # Flags come from the config drop-in written above; scripts/setup-k3s.sh adds the same ones.
  info "installing k3s ${K3S_VERSION:-latest stable} via scripts/setup-k3s.sh (sudo)"
  INSTALL_K3S_VERSION="$K3S_VERSION" "$ROOT_DIR/scripts/setup-k3s.sh"
  ok "k3s $(k3s --version | awk 'NR==1{print $3}') installed"
  config_changed=false # fresh install already started with the new config
else
  skip "k3s $installed_version already installed"
fi

if ! systemctl is-active --quiet k3s; then
  info "starting k3s (sudo)"
  as_root systemctl enable --now k3s
  config_changed=false
elif $config_changed; then
  info "restarting k3s to apply config (sudo)"
  as_root systemctl restart k3s
fi
ok "k3s service active"

# --- kubeconfig ---
if [[ ! -s "$KUBECONFIG" ]]; then
  info "copying /etc/rancher/k3s/k3s.yaml → $KUBECONFIG (sudo)"
  mkdir -p "$(dirname "$KUBECONFIG")"
  as_root cat /etc/rancher/k3s/k3s.yaml >"$KUBECONFIG"
  chmod 600 "$KUBECONFIG"
  ok "kubeconfig written"
else
  skip "kubeconfig $KUBECONFIG exists"
fi

info "waiting for API server"
wait_api
ok "API server ready"

node_ip_pinned() { [[ "$(node_internal_ip)" == "$K3S_NODE_IP" ]]; }
retry "$WAIT_TIMEOUT" 3 node_ip_pinned ||
  die "node InternalIP is '$(node_internal_ip)', expected $K3S_NODE_IP"
ok "node InternalIP $K3S_NODE_IP"
# Cilium reads the node address at startup: restart the agents when it moved.
if [[ -n "$previous_node_ip" && "$previous_node_ip" != "$K3S_NODE_IP" ]] && k -n kube-system get ds cilium >/dev/null 2>&1; then
  info "node IP moved $previous_node_ip → $K3S_NODE_IP: restarting cilium agents"
  k -n kube-system rollout restart ds/cilium >/dev/null
  rollout kube-system ds/cilium >/dev/null
  ok "cilium agents restarted"
fi

# A pod keeps the addresses it was created with: kubelet never rewrites them. After the node IP moved,
# host-network pods still publish the old one (stale scrape targets, CiliumEndpoints Cilium refuses to
# own), so recreate the ones a controller will bring back.
mapfile -t stale_ip < <(k get pods -A -o json | jq -r --arg ip "$K3S_NODE_IP" '.items[]
  | select(.status.phase == "Running" and ((.metadata.ownerReferences // []) | length) > 0)
  | select((.status.hostIP // $ip) != $ip or (.spec.hostNetwork == true and (.status.podIP // $ip) != $ip))
  | "\(.metadata.namespace)/\(.metadata.name)"')
for p in "${stale_ip[@]}"; do
  k -n "${p%/*}" delete pod "${p#*/}" --wait=false >/dev/null
  ok "recreating $p (still on the previous node address)"
done

# --- leftovers from a previous flannel/traefik install ---
# A stale cni0 bridge keeps a kernel route for the pod CIDR, which hijacks
# host→pod traffic (kubelet probes) once Cilium owns pod networking.
for link in cni0 flannel.1 flannel-v6.1; do
  if ip link show "$link" >/dev/null 2>&1; then
    info "removing leftover flannel interface $link (sudo)"
    as_root ip link delete "$link"
    ok "removed $link"
  fi
done

if ! k -n kube-system get deploy traefik >/dev/null 2>&1 && k -n kube-system get svc traefik >/dev/null 2>&1; then
  info "removing orphaned traefik Service (traefik is disabled)"
  k -n kube-system delete svc traefik --wait=false >/dev/null
fi

# LoadBalancer Services carry the finalizer service.kubernetes.io/load-balancer-cleanup,
# removed by servicelb (klipper). With servicelb disabled, deleted Services stay
# Terminating forever — and Cilium LB-IPAM still hands them pool IPs.
mapfile -t stuck < <(k get svc -A -o json | jq -r '.items[]
  | select(.metadata.deletionTimestamp != null and ((.metadata.finalizers // []) | index("service.kubernetes.io/load-balancer-cleanup")))
  | "\(.metadata.namespace)/\(.metadata.name)"')
for s in "${stuck[@]}"; do
  k -n "${s%/*}" patch svc "${s#*/}" --type=json \
    -p '[{"op":"remove","path":"/metadata/finalizers"}]' >/dev/null
  ok "released Service $s stuck Terminating (orphaned load-balancer-cleanup finalizer)"
done

info "node will report NotReady until Cilium is installed (next step)"

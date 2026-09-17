#!/usr/bin/env bash
# Step 30 — Cilium LB-IPAM pool and (optional) L2 announcement policy.
# Idempotent: kubectl apply; the L2 policy is removed again when L2_ANNOUNCE=false.
source "$(dirname "$0")/../lib.sh"
require kubectl envsubst

step "30 lb-ipam: LoadBalancer IP pool ${LB_POOL_START}-${LB_POOL_STOP}"
wait_api
wait_crd ciliumloadbalancerippools.cilium.io
wait_crd ciliuml2announcementpolicies.cilium.io

mkdir -p "$GENERATED_DIR"
render "$DEPLOY_DIR/lb-ipam/pool.yaml.tpl" >"$GENERATED_DIR/lb-pool.yaml"
k apply -f "$GENERATED_DIR/lb-pool.yaml" | sed 's/^/    /'

if [[ "$L2_ANNOUNCE" == true ]]; then
  L2_INTERFACES_YAML=$(for re in $L2_INTERFACES; do printf '    - "%s"\n' "$re"; done)
  export L2_INTERFACES_YAML
  render "$DEPLOY_DIR/lb-ipam/l2-policy.yaml.tpl" >"$GENERATED_DIR/l2-policy.yaml"
  k apply -f "$GENERATED_DIR/l2-policy.yaml" | sed 's/^/    /'
  ok "L2 announcements enabled on: $L2_INTERFACES"
else
  if k get ciliuml2announcementpolicy edge-l2 >/dev/null 2>&1; then
    k delete ciliuml2announcementpolicy edge-l2 | sed 's/^/    /'
  fi
  skip "L2 announcements disabled — LB IPs reachable from this host only (set L2_ANNOUNCE=true for LAN)"
fi

sleep 2
conflict=$(k get ciliumloadbalancerippool edge-pool -o jsonpath='{.status.conditions[?(@.type=="cilium.io/PoolConflict")].status}')
[[ "$conflict" != True ]] || die "pool edge-pool overlaps another pool: kubectl get ciliumloadbalancerippool edge-pool -o yaml"
ok "pool edge-pool has no conflicts"

for ip in "$EDGE_LB_IP" "$MQTT_LB_IP"; do
  python3 - "$LB_POOL_START" "$LB_POOL_STOP" "$ip" <<'EOF' || die "$ip is outside ${LB_POOL_START}-${LB_POOL_STOP}"
import ipaddress, sys
lo, hi, ip = (ipaddress.ip_address(a) for a in sys.argv[1:])
sys.exit(0 if lo <= ip <= hi else 1)
EOF
done
ok "EDGE_LB_IP=$EDGE_LB_IP and MQTT_LB_IP=$MQTT_LB_IP inside pool"

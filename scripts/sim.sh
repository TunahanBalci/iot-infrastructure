#!/usr/bin/env bash
# Tunnel sensor simulator as a Docker container on the host network, publishing to TBMQ
# through the Cilium L4 LoadBalancer IP (same path as real devices): MQTTS 8883, one
# connection and one client certificate (CN = tunnel id) per tunnel.
#   scripts/sim.sh up | down | logs | status
#   MODE=generator LOAD=50000 scripts/sim.sh up    # synthetic tunnels at that total msg/s
source "$(dirname "$0")/lib.sh"
require docker

cmd=${1:-status}
MODE=${MODE:-$SIM_MODE}
LOAD=${LOAD:-}
[[ $MODE == profiles || $MODE == generator ]] || die "MODE must be profiles or generator (got '$MODE')"

running() { docker ps -q --filter "name=^${SIM_CONTAINER}\$" | grep -q .; }

case "$cmd" in
  up)
    if [[ $MODE == profiles ]]; then
      [[ -f $SIM_PROFILES_FILE ]] || die "profiles file not found: $SIM_PROFILES_FILE"
      tunnels=$(grep -c '^[[:space:]]*-\?[[:space:]]*tunnel_id:' "$SIM_PROFILES_FILE")
      ((tunnels > 0)) || die "no tunnels defined in $SIM_PROFILES_FILE"
    else
      tunnels=$SIM_TUNNELS
    fi
    step "simulator ($MODE): $tunnels tunnels, $((tunnels * 3)) devices (one mTLS connection per tunnel) → mqtts://$MQTT_LB_IP:8883"
    src_hash=$(app_source_hash "$APPS_DIR/simulator")
    built_hash=$(docker image inspect -f '{{index .Config.Labels "iot.source-hash"}}' "$SIM_IMAGE" 2>/dev/null || true)
    if [[ "${SIM_REBUILD:-false}" == true || "$built_hash" != "$src_hash" ]]; then
      info "building $SIM_IMAGE (sources $src_hash)"
      docker build -q --label "iot.source-hash=$src_hash" -t "$SIM_IMAGE" "$APPS_DIR/simulator" >/dev/null
      ok "image $SIM_IMAGE built"
    else
      skip "image $SIM_IMAGE up to date"
    fi
    # Fails on an invalid config or profiles file before anything is started.
    docker run --rm "$SIM_IMAGE" --print-config >/dev/null || die "invalid simulator configuration"
    mkdir -p "$DEVICE_CERTS_DIR"
    stamp=$(mktemp "$DEVICE_CERTS_DIR/.issue-stamp.XXXXXX")
    if [[ $MODE == profiles ]]; then
      MODE=profiles "$ROOT_DIR/scripts/device-certs.sh"
    else
      MODE=generator "$ROOT_DIR/scripts/device-certs.sh" "$SIM_TUNNELS"
    fi
    reissued=$(find "$DEVICE_CERTS_DIR" -maxdepth 1 -name '*.pem' -newer "$stamp" | wc -l)
    rm -f "$stamp"
    if running; then
      # The simulator loads its client certificates once per connection setup.
      if ((reissued > 0)); then
        info "$reissued certificate(s) re-issued: restarting $SIM_CONTAINER"
        docker rm -f "$SIM_CONTAINER" >/dev/null
      else
        skip "container $SIM_CONTAINER already running (make sim-down first to apply other changes)"
        exit 0
      fi
    fi
    docker rm -f "$SIM_CONTAINER" >/dev/null 2>&1 || true
    env_args=(
      -e "SIM__MQTT__HOST=$MQTT_LB_IP"
      -e "SIM__MQTT__PORT=8883"
      -e "SIM__TOPOLOGY__MODE=$MODE"
      -e "SIM__TRAFFIC__MSGS_PER_DEVICE_S=$SIM_MSGS_PER_DEVICE_S"
      -e "SIM__SIMULATION__WORKERS=$SIM_WORKERS"
      -e "SIM__MQTT__CONNECTION_MODE=per_tunnel"
      -e "SIM__MQTT__TLS__ENABLED=true"
      -e "SIM__MQTT__TLS__CA_CERTS=/certs/server-ca.crt"
      -e "SIM__MQTT__TLS__CERTFILE=/certs/{tunnel_id}.pem"
      -e "SIM__MQTT__TLS__KEYFILE=/certs/{tunnel_id}.key"
      # Scraped by the cluster (static target) on the host-local node address, not on the Wi-Fi one.
      -e "SIM__SERVICE__HTTP_PORT=$SIM_METRICS_PORT"
      -e "SIM__SERVICE__HTTP_BIND=$K3S_NODE_IP"
    )
    if [[ $MODE == generator ]]; then
      env_args+=(-e "SIM__TOPOLOGY__TUNNELS=$SIM_TUNNELS")
    fi
    # LOAD is a total across every device; the simulator divides it by tunnels * 3.
    if [[ -n $LOAD ]]; then
      env_args+=(-e "SIM__TRAFFIC__TARGET_TOTAL_MSGS_S=$LOAD")
    fi
    # Host uid: the private keys are 0600 files owned by the user who issued them.
    docker run -d --name "$SIM_CONTAINER" --network host --restart unless-stopped \
      --user "$(id -u):$(id -g)" -v "$DEVICE_CERTS_DIR:/certs:ro" \
      "${env_args[@]}" "$SIM_IMAGE" >/dev/null
    sleep 3
    running || { docker logs --tail 20 "$SIM_CONTAINER" >&2; die "simulator exited"; }
    ok "simulator running (make sim-logs); metrics http://$K3S_NODE_IP:$SIM_METRICS_PORT/metrics"
    info "simulated time: curl -XPOST http://$K3S_NODE_IP:$SIM_METRICS_PORT/time -d '{\"set\": \"2026-01-01T18:00:00\"}'"
    info "               curl -XPOST http://$K3S_NODE_IP:$SIM_METRICS_PORT/time/resync"
    ;;
  down)
    step "simulator: stop"
    if docker ps -aq --filter "name=^${SIM_CONTAINER}\$" | grep -q .; then
      docker rm -f "$SIM_CONTAINER" >/dev/null
      ok "container $SIM_CONTAINER removed"
    else
      skip "not running"
    fi
    ;;
  logs)
    exec docker logs -f --tail 100 "$SIM_CONTAINER"
    ;;
  status)
    if running; then
      ok "simulator running"
      docker logs --tail 5 "$SIM_CONTAINER" 2>&1 | sed 's/^/      /'
    else
      info "simulator not running"
    fi
    ;;
  *) die "usage: $0 up|down|logs|status" ;;
esac

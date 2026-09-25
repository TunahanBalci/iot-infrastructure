#!/usr/bin/env bash
# Print how to reach everything.
source "$(dirname "$0")/lib.sh"

CA="$GENERATED_DIR/iot-root-ca.crt"
if api_ready && k -n cert-manager get secret iot-root-ca >/dev/null 2>&1; then
  mkdir -p "$GENERATED_DIR"
  k -n cert-manager get secret iot-root-ca -o jsonpath='{.data.ca\.crt}' | base64 -d >"$CA"
fi

cat <<EOF

${C_BOLD}Endpoints${C_RESET}
  ${C_CYAN}L7 · Envoy Gateway (${EDGE_LB_IP})${C_RESET}
    TBMQ UI / REST     http://${EDGE_LB_IP}/   https://${EDGE_LB_IP}/
    admin login        ${TBMQ_ADMIN_USER}   password: .state/tbmq-admin-password (or TBMQ_ADMIN_PASSWORD)
  ${C_CYAN}L4 · Cilium LoadBalancer (${MQTT_LB_IP}) — client certificate required${C_RESET}
    MQTTS              mqtts://${MQTT_LB_IP}:8883
    device certs       ${DEVICE_CERTS_DIR}/<tunnel>.pem|.key   (make device-certs; CN = tunnel id)
    watch events       make consensus-watch   (iot-viewer certificate, subscribe-only)
    ACL rules          deploy/tbmq/device-credentials.json.tpl

  ${C_CYAN}Pipeline (iot-pipeline, app Kafka app-kafka-kafka-bootstrap:9093, TLS + SCRAM)${C_RESET}
    ingest             $((TBMQ_IE_SHARDS * 3)) TBMQ integrations app-kafka-ingest-<position>[-<shard>] → iot.mqtt.ingest
    processing         telemetry-processor → iot.detections   consensus → iot.vehicles iot.traffic iot.sensor-health
    watch a topic      make watch-topic TOPIC=iot.vehicles
    scaling            kubectl get scaledobject -A    (KEDA on consumer-group lag)

  ${C_CYAN}Storage (${STORAGE_NAMESPACE}, STORAGE_ENABLED=${STORAGE_ENABLED})${C_RESET}
    ClickHouse SQL     make clickhouse-client     (database iot: detections, vehicles_dedup, vehicles_1m/1h, traffic, sensor_health)
    cold tier          SeaweedFS S3 bucket clickhouse-cold (parts move by TTL)

  ${C_CYAN}Observability (${MONITORING_NAMESPACE}, OBSERVABILITY_ENABLED=${OBSERVABILITY_ENABLED}) — port-forward, then open${C_RESET}
    Grafana            kubectl -n monitoring port-forward svc/vm-grafana 3000:80          http://localhost:3000  (admin / .state/grafana-admin-password)
    vmui (metrics)     kubectl -n monitoring port-forward svc/vmsingle-vm 8428:8428       http://localhost:8428/vmui
    alerts             kubectl -n monitoring port-forward svc/vmalert-vm 8880:8080        http://localhost:8880
    logs               kubectl -n monitoring port-forward svc/vlogs-server 9428:9428      http://localhost:9428/select/vmui
    traces             kubectl -n monitoring port-forward svc/vtraces-server 10428:10428  http://localhost:10428/select/vmui

  ${C_CYAN}TBMQ data services (in-cluster, ${TBMQ_NAMESPACE})${C_RESET}
    Kafka (Strimzi)    tbmq-kafka-kafka-bootstrap:9093   (mTLS, KafkaUser tbmq-broker; TBMQ pods only)
    Postgres (CNPG)    tbmq-db-rw:5432   (secret tbmq-db-app)
    Valkey             tbmq-valkey:6379  (secret tbmq-valkey)

  CA certificate       ${CA}
  Hosts (optional)     ${EDGE_LB_IP} tbmq.${DOMAIN}   ${MQTT_LB_IP} mqtt.${DOMAIN}
  Hubble UI            cilium hubble ui          (or: make hubble-ui)
  Flow logs            hubble observe --port 8883   (needs: cilium hubble port-forward &)
EOF
if [[ "$L2_ANNOUNCE" != true ]]; then
  printf '\n  %sLB IPs are host-local (L2_ANNOUNCE=false) and no NodePorts are allocated: reachable from this machine only.%s\n' "$C_DIM" "$C_RESET"
fi

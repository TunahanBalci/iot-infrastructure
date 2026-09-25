#!/usr/bin/env bash
# Verify every layer, step by step. Read-only: never changes the cluster.
#   scripts/verify.sh                 # all steps
#   scripts/verify.sh cilium tbmq     # selected steps
# Steps: prereqs k3s cilium lb-ipam cert-manager envoy-gateway operators tbmq-deps app-kafka tbmq processing
#        storage observability e2e
source "$(dirname "$0")/lib.sh"
trap - ERR
set +e

ALL_STEPS=(prereqs k3s cilium lb-ipam cert-manager envoy-gateway operators tbmq-deps app-kafka tbmq processing storage observability e2e)
NS=$TBMQ_NAMESPACE
PROBE="$ROOT_DIR/scripts/probe.py"

# --- predicates (run by check/soft_check; print a reason on failure) ---------
eq() { [[ "$1" == "$2" ]] || { echo "got '$1', want '$2'"; return 1; }; }
ge() { ((${1:-0} >= $2)) || { echo "got '$1', want >= $2"; return 1; }; }
absent() { ! "$@" >/dev/null 2>&1; }

replicas_ready() { # <ns> <kind> <name> [expected-replicas]
  local s
  s=$(k -n "$1" get "$2" "$3" -o jsonpath='{.status.readyReplicas}/{.spec.replicas}' 2>&1) || { echo "$s"; return 1; }
  [[ "${s%/*}" == "${s#*/}" && "${s#*/}" != 0 ]] || { echo "ready $s"; return 1; }
  [[ -z "${4:-}" || "${s#*/}" == "$4" ]] || { echo "replicas ${s#*/}, want $4"; return 1; }
}
ds_ready() { # <ns> <name>
  local s
  s=$(k -n "$1" get ds "$2" -o jsonpath='{.status.numberReady}/{.status.desiredNumberScheduled}' 2>&1) || { echo "$s"; return 1; }
  [[ "${s%/*}" == "${s#*/}" && "${s#*/}" != 0 ]] || { echo "ready $s"; return 1; }
}
condition_true() { # <ns|-> <kind/name> <condition>
  local nsflag=()
  [[ "$1" != - ]] && nsflag=(-n "$1")
  eq "$(k "${nsflag[@]}" get "$2" -o jsonpath="{.status.conditions[?(@.type==\"$3\")].status}" 2>&1)" True
}
helm_deployed() { # <release> <ns> <version>
  local j
  j=$(helm list -n "$2" -f "^$1\$" -o json 2>&1) || { echo "$j"; return 1; }
  eq "$(jq -r '.[0].status // "missing"' <<<"$j")" deployed || return 1
  eq "$(jq -r '.[0].chart // ""' <<<"$j" | sed -E 's/^.*-v?([0-9]+\.[0-9]+\.[0-9]+.*)$/\1/')" "${3#v}"
}
cilium_cfg() { eq "$(k -n kube-system get cm cilium-config -o jsonpath="{.data.$1}" 2>&1)" "$2"; }

nodes_ready() {
  local bad
  bad=$(k get nodes --no-headers 2>&1 | awk '$2 != "Ready" {print $1" "$2}')
  [[ -z "$bad" ]] || { echo "$bad"; return 1; }
}
pods_managed_by_cilium() {
  local unmanaged
  unmanaged=$(comm -23 \
    <(k get pods -A -o json | jq -r '.items[] | select(.spec.hostNetwork != true and .status.phase == "Running") | "\(.metadata.namespace)/\(.metadata.name)"' | sort) \
    <(k get ciliumendpoints -A -o json | jq -r '.items[] | "\(.metadata.namespace)/\(.metadata.name)"' | sort))
  [[ -z "$unmanaged" ]] || { echo "unmanaged: $unmanaged"; return 1; }
}
pool_conflict_free() {
  local s
  s=$(k get ciliumloadbalancerippool edge-pool -o jsonpath='{.status.conditions[?(@.type=="cilium.io/PoolConflict")].status}' 2>&1) || { echo "$s"; return 1; }
  [[ "$s" != True ]] || { echo "PoolConflict=True"; return 1; }
}
no_pending_lb() {
  local pending
  pending=$(k get svc -A -o json | jq -r '.items[] | select(.spec.type == "LoadBalancer" and ((.status.loadBalancer.ingress // []) | length) == 0) | "\(.metadata.namespace)/\(.metadata.name)"')
  [[ -z "$pending" ]] || { echo "pending: $pending"; return 1; }
}
no_stuck_lb_finalizer() {
  local stuck
  stuck=$(k get svc -A -o json | jq -r '.items[] | select(.metadata.deletionTimestamp != null) | "\(.metadata.namespace)/\(.metadata.name)"')
  [[ -z "$stuck" ]] || { echo "terminating: $stuck (make install-k3s releases them)"; return 1; }
}
envoy_data_plane_ready() {
  k -n envoy-gateway-system get deploy -l gateway.envoyproxy.io/owning-gateway-name=edge -o json |
    jq -e '.items | length > 0 and all(.status.readyReplicas == .spec.replicas)' >/dev/null
}
route_accepted() { # <name>
  k -n "$NS" get httproute "$1" -o json |
    jq -e '[.status.parents[]?.conditions[]? | select(.type == "Accepted" or .type == "ResolvedRefs") | .status] | length == 2 and all(. == "True")' >/dev/null
}
lb_backends() { # <ip:port> <expected>  — counts active backends in Cilium's service table
  local n
  n=$(k -n kube-system exec ds/cilium -c cilium-agent -- cilium-dbg service list 2>/dev/null |
    awk -v fe="$1" '/^[0-9]+ / { blk = index($0, fe) > 0 } blk && /=>/ && /active/ { n++ } END { print n + 0 }')
  eq "$n" "$2"
}
schema_installed() {
  eq "$(tbmq_db_query "select to_regclass('public.tb_schema_settings') is not null" 2>&1)" t
}
valkey_cli() { # valkey_cli [-a] <args...>  — -a authenticates with the pod's password
  local auth=()
  if [[ "$1" == -a ]]; then auth=(-a '"$VALKEY_PASSWORD"'); shift; fi
  k -n "$NS" exec statefulset/tbmq-valkey -c valkey -- sh -c "valkey-cli --no-auth-warning ${auth[*]} $*"
}
valkey_requires_auth() {
  local out
  out=$(valkey_cli ping 2>&1)
  [[ "$out" == *NOAUTH* ]] || { echo "unauthenticated ping answered: $out"; return 1; }
}
valkey_aof_on() { eq "$(valkey_cli -a config get appendonly 2>&1 | tail -n1)" yes; }
legacy_deps_absent() {
  local w found=()
  for w in deploy/postgres statefulset/tbmq-kafka deploy/tbmq-valkey; do
    if k -n "$NS" get "$w" >/dev/null 2>&1; then found+=("$w"); fi
  done
  ((${#found[@]} == 0)) || { echo "still present: ${found[*]} (make install-tbmq-deps LEGACY_DEPS_DELETE=true)"; return 1; }
}
no_lb_node_ports() {
  local with
  with=$(k get svc -A -o json | jq -r '.items[] | select(.spec.type == "LoadBalancer" and any(.spec.ports[]; .nodePort != null))
    | "\(.metadata.namespace)/\(.metadata.name)"')
  [[ -z "$with" ]] || { echo "NodePorts allocated: $with"; return 1; }
}
node_ip_on_iface() { [[ "$(ip -4 -o addr show dev "$K3S_NODE_IP_IFACE" 2>&1)" == *" $K3S_NODE_IP/32 "* ]]; }
kafka_listener_mtls() {
  eq "$(k -n "$NS" get kafka tbmq-kafka -o jsonpath='{range .spec.kafka.listeners[*]}{.port}/{.tls}/{.authentication.type};{end}' 2>&1)" "9093/true/tls;"
}
kafka_network_policy() { # tbmq and its integration executors publish/consume; KEDA reads the group lag
  k -n "$NS" get networkpolicy tbmq-kafka-network-policy-kafka -o json 2>&1 |
    jq -e '[.spec.ingress[] | select(any(.ports[]?; .port == 9093)) | .from[]?.podSelector.matchLabels.app]
           | sort == ["keda-operator", "tbmq", "tbmq-integration-executor"]' >/dev/null
}
reloader_annotated() { # <statefulset>
  eq "$(k -n "$NS" get sts "$1" -o jsonpath='{.metadata.annotations.reloader\.stakater\.com/auto}' 2>&1)" true
}
admin_hardened() {
  local out
  out=$(python3 "$ROOT_DIR/scripts/tbmq_credentials.py" check-admin --url "http://$EDGE_LB_IP" 2>&1) || { echo "$out"; return 1; }
}
lb_ports() { eq "$(k -n "$NS" get svc tbmq-mqtt -o jsonpath='{.spec.ports[*].port}' 2>&1)" 8883; }
x509_auth_configured() {
  local out
  out=$(python3 "$ROOT_DIR/scripts/tbmq_credentials.py" check-x509 --url "http://$EDGE_LB_IP" --file "$GENERATED_DIR/tbmq/device-credentials.json" 2>&1) ||
    { echo "$out"; return 1; }
}
http_status() { # <expected> <curl args...>
  local want=$1; shift
  eq "$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$@")" "$want"
}
ws_status_in() { # <regex> <probe ws args...>
  local re=$1 out; shift
  out=$(python3 "$PROBE" ws "$@")
  [[ "$out" =~ $re ]] || { echo "$out"; return 1; }
}

# pod_metric_sum <namespace> <label-selector> <metric> — sum over matching pods of a /metrics value
# (all label sets of the metric), read through the API server proxy.
pod_metric_sum() {
  local pods pod v sum=0
  pods=$(k -n "$1" get pods -l "$2" --field-selector=status.phase=Running -o jsonpath='{.items[*].metadata.name}' 2>/dev/null)
  [[ -n "$pods" ]] || { echo "no running pods for $2"; return 1; }
  for pod in $pods; do
    # Plain kubectl, not k(): --request-timeout appends ?timeout=... to the URL, and the pod-proxy
    # subresource answers that with "the server could not find the requested resource". `timeout`
    # keeps the call bounded.
    v=$(timeout 30 kubectl get --raw "/api/v1/namespaces/$1/pods/$pod:8080/proxy/metrics" 2>/dev/null |
      awk -v m="$3" '$1 == m || index($1, m "{") == 1 { s += $2; found = 1 } END { if (found) printf "%d", s; else exit 1 }') ||
      { echo "$pod: $3 not found"; return 1; }
    sum=$((sum + v))
  done
  echo "$sum"
}
metric_positive() { # metric_positive <namespace> <selector> <metric>
  local v
  v=$(pod_metric_sum "$@") || { echo "$v"; return 1; }
  ((v > 0)) || { echo "$3 = 0"; return 1; }
}
scaledobject_ready() { condition_true "$1" "scaledobject/$2" Ready; }
kafka_topics_ready() {
  local notready
  notready=$(k -n iot-pipeline get kafkatopic -l strimzi.io/cluster=app-kafka -o json 2>&1 |
    jq -r '.items[] | select(([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 0) | .metadata.name') ||
    { echo "$notready"; return 1; }
  [[ -z "$notready" ]] || { echo "not ready: $notready"; return 1; }
}
# Partitions are the parallelism ceiling of every consumer group. Kafka can only grow them, so a topic
# left at an older, smaller count silently caps how far KEDA can scale the pipeline out.
kafka_topic_partitions() { # kafka_topic_partitions <topic> <expected>
  local n
  n=$(k -n iot-pipeline get kafkatopic "$1" -o jsonpath='{.spec.partitions}' 2>&1) || { echo "$n"; return 1; }
  ge "$n" "$2"
}

kafka_users_ready() {
  local u
  for u in tbmq-ie telemetry-processor consensus clickhouse keda pipeline-viewer; do
    condition_true iot-pipeline "kafkauser/$u" Ready >/dev/null || { echo "KafkaUser $u not Ready"; return 1; }
  done
}
app_kafka_listener() {
  eq "$(k -n iot-pipeline get kafka app-kafka -o jsonpath='{range .spec.kafka.listeners[*]}{.port}/{.tls}/{.authentication.type};{end}{.spec.kafka.authorization.type}' 2>&1)" \
    "9093/true/scram-sha-512;simple"
}
integrations_configured() { # the TBMQ_IE_SHARDS x 3 integrations of the configured shard layout, and no others
  local out position shard suffix password ca names=()
  password=$(k -n iot-pipeline get secret tbmq-ie -o jsonpath='{.data.password}' | base64 -d)
  ca=$(k -n iot-pipeline get secret app-kafka-cluster-ca-cert -o jsonpath='{.data.ca\.crt}' | base64 -d)
  for position in start middle end; do
    for ((shard = 0; shard < TBMQ_IE_SHARDS; shard++)); do
      suffix=""
      ((TBMQ_IE_SHARDS == 1)) || suffix="-$shard"
      names+=("app-kafka-ingest-$position$suffix")
      out=$(APP_KAFKA_USERNAME=tbmq-ie APP_KAFKA_PASSWORD="$password" APP_KAFKA_CA_PEM="$ca" \
        python3 "$ROOT_DIR/scripts/tbmq_credentials.py" check-integration --url "http://$EDGE_LB_IP" \
          --file "$GENERATED_DIR/tbmq/app-kafka-integration-$position$suffix.json" 2>&1) ||
        { echo "$position$suffix: $out"; return 1; }
    done
  done
  python3 "$ROOT_DIR/scripts/tbmq_credentials.py" check-integrations --url "http://$EDGE_LB_IP" \
    --prefix app-kafka-ingest- "${names[@]}"
}
consumer_lag_below() { # consumer_lag_below <group> <max> — from KEDA's view (HPA external metric)
  local hpa v
  hpa=$(k -n iot-pipeline get hpa -o json | jq -r --arg so "keda-hpa-$1" '.items[] | select(.metadata.name == $so) |
    [.status.currentMetrics[]?.external.current.averageValue // empty] | first // empty')
  [[ -n "$hpa" ]] || { echo "no HPA metric yet for $1"; return 1; }
  v=$(numfmt --from=si "${hpa%m}" 2>/dev/null || echo "${hpa%m}")
  [[ "$hpa" == *m ]] && v=$((v / 1000))
  ((v <= $2)) || { echo "lag per pod $v > $2"; return 1; }
}

# probe_rc <want-exit> <probe args...> — probe.py exit: 0 accepted, 2 refused by broker, 1 no broker/TLS
probe_rc() {
  local want=$1 out rc=0; shift
  out=$(python3 "$PROBE" "$@" 2>&1) || rc=$?
  ((rc == want)) || { echo "exit $rc, want $want: $out"; return 1; }
}
probe_rc_not() {
  local unwanted=$1 out rc=0; shift
  out=$(python3 "$PROBE" "$@" 2>&1) || rc=$?
  ((rc != unwanted)) || { echo "exit $rc: $out"; return 1; }
}

# --- steps -------------------------------------------------------------------
verify_prereqs() {
  step "verify prereqs"
  for c in curl jq envsubst openssl python3 kubectl helm cilium hubble; do check "$c installed" has "$c"; done
  soft_check "docker installed (simulator only)" has docker
  soft_check "helm $HELM_VERSION" eq "$(helm version --template '{{.Version}}' 2>/dev/null)" "$HELM_VERSION"
  soft_check "cilium-cli $CILIUM_CLI_VERSION" eq "$(cilium version --client 2>/dev/null | awk '/cilium-cli:/{print $2}')" "$CILIUM_CLI_VERSION"
  # Watchers (config reloaders, Vector, Grafana) fail with "too many open files" below these.
  soft_check "fs.inotify.max_user_instances >= 512" ge "$(sysctl -n fs.inotify.max_user_instances 2>/dev/null || echo 0)" 512
  soft_check "fs.inotify.max_user_watches >= 262144" ge "$(sysctl -n fs.inotify.max_user_watches 2>/dev/null || echo 0)" 262144
}

verify_k3s() {
  step "verify k3s"
  check "k3s binary installed" has k3s
  [[ -n "$K3S_VERSION" ]] && check "k3s $K3S_VERSION" eq "$(k3s --version 2>/dev/null | awk 'NR==1{print $3}')" "$K3S_VERSION"
  check "k3s service active" systemctl is-active --quiet k3s
  mkdir -p "$GENERATED_DIR"
  render "$DEPLOY_DIR/k3s/50-iot-infra.yaml.tpl" >"$GENERATED_DIR/k3s-50-iot-infra.yaml"
  check "config drop-in: no flannel/kube-proxy/traefik/servicelb" \
    diff -u "$GENERATED_DIR/k3s-50-iot-infra.yaml" /etc/rancher/k3s/config.yaml.d/50-iot-infra.yaml
  check "kubeconfig reaches API ($KUBECONFIG)" api_ready
  check "all nodes Ready" nodes_ready
  check "stable node address $K3S_NODE_IP on $K3S_NODE_IP_IFACE" node_ip_on_iface
  check "iot-node-ip.service active" systemctl is-active --quiet iot-node-ip
  check "node InternalIP $K3S_NODE_IP" eq "$(k get node -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>&1)" "$K3S_NODE_IP"
  check "API server advertises $K3S_NODE_IP (kubernetes endpoints)" eq \
    "$(k get endpointslice -n default kubernetes -o jsonpath='{.endpoints[*].addresses[*]}' 2>&1)" "$K3S_NODE_IP"
  check "CoreDNS ready" replicas_ready kube-system deploy coredns
  # CoreDNS keeps the upstream resolver its pod started with: after a network change it answers
  # cluster names and nothing else, which breaks every in-cluster download.
  soft_check "cluster DNS resolves $DNS_PROBE_NAME (CoreDNS upstream current)" cluster_dns_resolves_externally
  check "no leftover flannel bridge cni0" absent ip link show cni0
  check "traefik not deployed" absent k -n kube-system get deploy traefik
  check "servicelb not deployed" eq "$(k get ds -A -o name 2>/dev/null | grep -c svclb)" 0
}

verify_cilium() {
  step "verify cilium"
  check "helm release cilium $CILIUM_VERSION deployed" helm_deployed cilium kube-system "$CILIUM_VERSION"
  check "cilium agents ready" ds_ready kube-system cilium
  check "cilium-operator ready" replicas_ready kube-system deploy cilium-operator
  check "cilium status healthy" cilium status --wait --wait-duration 20s
  check "kube-proxy replacement enabled" cilium_cfg kube-proxy-replacement true
  check "API endpoint $K8S_SERVICE_HOST:$K8S_SERVICE_PORT" eq \
    "$(k -n kube-system get ds cilium -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="KUBERNETES_SERVICE_HOST")].value}:{.spec.template.spec.containers[0].env[?(@.name=="KUBERNETES_SERVICE_PORT")].value}')" \
    "$K8S_SERVICE_HOST:$K8S_SERVICE_PORT"
  check "LB algorithm maglev" cilium_cfg bpf-lb-algorithm maglev
  check "LB mode $CILIUM_LB_MODE" cilium_cfg bpf-lb-mode "$CILIUM_LB_MODE"
  check "LB-IPAM enabled" cilium_cfg enable-lb-ipam true
  check "Cilium Gateway API off (Envoy Gateway owns it)" absent cilium_cfg enable-gateway-api true
  check "all pods managed by Cilium" pods_managed_by_cilium
  soft_check "hubble-relay ready" replicas_ready kube-system deploy hubble-relay
}

verify_lb_ipam() {
  step "verify lb-ipam"
  check "pool edge-pool exists" k get ciliumloadbalancerippool edge-pool
  check "pool has no conflicts" pool_conflict_free
  check "pool range $LB_POOL_START-$LB_POOL_STOP" eq \
    "$(k get ciliumloadbalancerippool edge-pool -o jsonpath='{.spec.blocks[0].start}-{.spec.blocks[0].stop}' 2>&1)" "$LB_POOL_START-$LB_POOL_STOP"
  if [[ "$L2_ANNOUNCE" == true ]]; then
    check "L2 announcement policy present" k get ciliuml2announcementpolicy edge-l2
    soft_check "L2 leases held" eq "$(k -n kube-system get lease -o name | grep -c l2announce)" 2
  else
    check "no L2 policy (L2_ANNOUNCE=false, host-local LB IPs)" absent k get ciliuml2announcementpolicy edge-l2
  fi
  check "no LoadBalancer service stuck <pending>" no_pending_lb
  check "no Service stuck Terminating on LB finalizer" no_stuck_lb_finalizer
  check "no LoadBalancer Service allocates NodePorts (nothing on the Wi-Fi IP)" no_lb_node_ports
}

verify_cert_manager() {
  step "verify cert-manager"
  check "helm release cert-manager $CERT_MANAGER_VERSION deployed" helm_deployed cert-manager cert-manager "$CERT_MANAGER_VERSION"
  for d in cert-manager cert-manager-webhook cert-manager-cainjector; do
    check "$d ready" replicas_ready cert-manager deploy "$d"
  done
  check "root CA certificate Ready" condition_true cert-manager certificate/iot-root-ca Ready
  check "ClusterIssuer iot-ca Ready" condition_true - clusterissuer/iot-ca Ready
}

verify_envoy_gateway() {
  step "verify envoy-gateway"
  check "helm release eg $ENVOY_GATEWAY_VERSION deployed" helm_deployed eg envoy-gateway-system "$ENVOY_GATEWAY_VERSION"
  check "Gateway API CRDs installed" crd_exists httproutes.gateway.networking.k8s.io
  check "envoy-gateway controller ready" replicas_ready envoy-gateway-system deploy envoy-gateway
  check "GatewayClass envoy Accepted" condition_true - gatewayclass/envoy Accepted
  check "Gateway edge Programmed" condition_true envoy-gateway-system gateway/edge Programmed
  check "Gateway address $EDGE_LB_IP" eq "$(k -n envoy-gateway-system get gateway edge -o jsonpath='{.status.addresses[0].value}' 2>&1)" "$EDGE_LB_IP"
  check "edge TLS certificate Ready" condition_true envoy-gateway-system certificate/edge-tls Ready
  check "envoy data plane ready" envoy_data_plane_ready
}

verify_operators() {
  step "verify operators"
  check "helm release strimzi $STRIMZI_VERSION deployed" helm_deployed strimzi strimzi-system "$STRIMZI_VERSION"
  check "strimzi-cluster-operator ready" replicas_ready strimzi-system deploy strimzi-cluster-operator
  check "Strimzi CRD kafkas.kafka.strimzi.io (v1)" eq \
    "$(k get crd kafkas.kafka.strimzi.io -o jsonpath='{.spec.versions[?(@.storage==true)].name}' 2>&1)" v1
  check "helm release cnpg $CNPG_CHART_VERSION deployed" helm_deployed cnpg cnpg-system "$CNPG_CHART_VERSION"
  check "CloudNativePG operator ready" replicas_ready cnpg-system deploy \
    "$(k -n cnpg-system get deploy -l app.kubernetes.io/name=cloudnative-pg -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
  check "helm release reloader $RELOADER_CHART_VERSION deployed" helm_deployed reloader reloader "$RELOADER_CHART_VERSION"
  check "Reloader ready" replicas_ready reloader deploy reloader-reloader
  check "helm release keda $KEDA_VERSION deployed" helm_deployed keda keda "$KEDA_VERSION"
  for d in keda-operator keda-operator-metrics-apiserver keda-admission-webhooks; do
    check "$d ready" replicas_ready keda deploy "$d"
  done
}

verify_tbmq_deps() {
  step "verify tbmq-deps"
  check "Kafka tbmq-kafka Ready (Strimzi)" condition_true "$NS" kafka/tbmq-kafka Ready
  check "Kafka version $KAFKA_VERSION" eq "$(k -n "$NS" get kafka tbmq-kafka -o jsonpath='{.status.kafkaVersion}' 2>&1)" "$KAFKA_VERSION"
  check "Kafka pods ready ($KAFKA_REPLICAS)" kafka_pods_ready
  check "Kafka listener: only 9093, TLS, client certificates" kafka_listener_mtls
  check "Kafka NetworkPolicy: 9093 only from tbmq, integration executor and KEDA pods" kafka_network_policy
  check "KafkaUser tbmq-broker Ready (client certificate)" condition_true "$NS" kafkauser/tbmq-broker Ready
  check "Postgres tbmq-db Ready (CloudNativePG)" condition_true "$NS" clusters.postgresql.cnpg.io/tbmq-db Ready
  check "Postgres instances ready ($POSTGRES_INSTANCES)" postgres_instances_ready
  check "valkey ready" replicas_ready "$NS" sts tbmq-valkey 1
  check "valkey requires a password" valkey_requires_auth
  check "valkey AOF persistence on" valkey_aof_on
  check "TBMQ database schema installed" schema_installed
  check "old upstream Postgres/Kafka/Valkey workloads removed" legacy_deps_absent
}

verify_tbmq() {
  step "verify tbmq"
  check "tbmq ready ($TBMQ_REPLICAS replicas)" replicas_ready "$NS" sts tbmq "$TBMQ_REPLICAS"
  check "integration executor ready ($TBMQ_IE_REPLICAS)" replicas_ready "$NS" sts tbmq-integration-executor "$TBMQ_IE_REPLICAS"
  check "MQTTS certificate Ready" condition_true "$NS" certificate/tbmq-mqtt-tls Ready
  check "service tbmq is ClusterIP (no NodePorts)" eq "$(k -n "$NS" get svc tbmq -o jsonpath='{.spec.type}' 2>&1)" ClusterIP
  check "service tbmq-mqtt on $MQTT_LB_IP" eq "$(k -n "$NS" get svc tbmq-mqtt -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>&1)" "$MQTT_LB_IP"
  check "service tbmq-mqtt exposes only 8883" lb_ports
  soft_check "Cilium L4 table: $TBMQ_REPLICAS active backends for $MQTT_LB_IP:8883" lb_backends "$MQTT_LB_IP:8883" "$TBMQ_REPLICAS"
  check "HTTPRoute tbmq-ui accepted" route_accepted tbmq-ui
  check "MQTT over WebSocket route removed" absent k -n "$NS" get httproute tbmq-mqtt-ws
  check "certificates only: X.509 required, MQTT Basic disabled, credentials iot-devices match" x509_auth_configured
  check "admin: generated password, default rejected, default-key JWT rejected" admin_hardened
  check "Reloader restarts tbmq on secret changes" reloader_annotated tbmq
  check "Reloader restarts integration executor on secret changes" reloader_annotated tbmq-integration-executor
  soft_check "tbmq consumer batching (TBMQ_MSG_CONSUMER_CONFIG)" eq \
    "$(k -n "$NS" get sts tbmq -o jsonpath='{.spec.template.spec.containers[?(@.name=="server")].env[?(@.name=="TB_KAFKA_MSG_ALL_ADDITIONAL_CONSUMER_CONFIG")].value}' 2>&1)" "$TBMQ_MSG_CONSUMER_CONFIG"
  soft_check "integration executor consumer batching (TBMQ_IE_MSG_CONSUMER_CONFIG)" eq \
    "$(k -n "$NS" get sts tbmq-integration-executor -o jsonpath='{.spec.template.spec.containers[?(@.name=="server")].env[?(@.name=="TB_KAFKA_IE_MSG_ADDITIONAL_CONSUMER_CONFIG")].value}' 2>&1)" "$TBMQ_IE_MSG_CONSUMER_CONFIG"
  check "$((TBMQ_IE_SHARDS * 3)) integrations app-kafka-ingest-* configured and enabled, no leftovers (→ iot.mqtt.ingest)" integrations_configured
  check "ScaledObject tbmq-integration-executor Ready (KEDA on internal Kafka lag)" scaledobject_ready "$NS" tbmq-integration-executor
  check "KafkaUser keda-tbmq Ready" condition_true "$NS" kafkauser/keda-tbmq Ready
  soft_check "legacy nginx Ingress removed" absent k -n "$NS" get ingress tbmq-ingress
}

verify_app_kafka() {
  step "verify app-kafka"
  check "Kafka app-kafka Ready (Strimzi)" condition_true iot-pipeline kafka/app-kafka Ready
  check "app-kafka pods ready ($APP_KAFKA_REPLICAS)" strimzi_pods_ready iot-pipeline app-kafka "$APP_KAFKA_REPLICAS"
  check "listener 9093: TLS + SCRAM-SHA-512, simple authorization (ACLs)" app_kafka_listener
  check "NetworkPolicy generated for app-kafka" k -n iot-pipeline get networkpolicy app-kafka-network-policy-kafka
  check "topics Ready" kafka_topics_ready
  check "iot.mqtt.ingest has >= $APP_KAFKA_INGEST_PARTITIONS partitions (processor scale-out ceiling)" \
    kafka_topic_partitions iot.mqtt.ingest "$APP_KAFKA_INGEST_PARTITIONS"
  check "iot.detections has >= $APP_KAFKA_TUNNEL_PARTITIONS partitions (consensus scale-out ceiling)" \
    kafka_topic_partitions iot.detections "$APP_KAFKA_TUNNEL_PARTITIONS"
  check "SCRAM users Ready (tbmq-ie telemetry-processor consensus clickhouse keda pipeline-viewer)" kafka_users_ready
}

verify_processing() {
  step "verify processing"
  check "telemetry-processor has ready pods" deploy_has_ready iot-pipeline telemetry-processor
  check "ScaledObject telemetry-processor Ready (KEDA, Kafka lag)" scaledobject_ready iot-pipeline telemetry-processor
  check "consensus has ready pods" deploy_has_ready iot-pipeline consensus
  if [[ "$OBSERVABILITY_ENABLED" == true ]]; then
    check "ScaledObject consensus Ready (KEDA, unread detections via VictoriaMetrics)" scaledobject_ready iot-pipeline consensus
  else
    skip "consensus autoscaling needs OBSERVABILITY_ENABLED=true (fixed replicas)"
  fi
  soft_check "telemetry-processor runs current apps/telemetry-processor/ sources (else: make install-processing)" eq \
    "$(k -n iot-pipeline get deploy telemetry-processor -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null)" "$(processor_image_ref)"
  soft_check "consensus runs current apps/consensus/ sources (else: make install-processing)" eq \
    "$(k -n iot-pipeline get deploy consensus -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null)" "$(consensus_image_ref)"
  check "keda-app-kafka login secret present" k -n iot-pipeline get secret keda-app-kafka
  soft_check "telemetry-processor consuming envelopes (needs: make sim-up)" metric_positive iot-pipeline app=telemetry-processor telemetry_processor_consumed_total
  soft_check "consensus receiving detections" metric_positive iot-pipeline app=consensus consensus_received_total
  soft_check "consensus emitting vehicle events (tunnels calibrate within ~3 min of traffic)" metric_positive iot-pipeline app=consensus consensus_vehicles_total
  soft_check "telemetry-processor lag per pod <= $PROCESSOR_LAG_THRESHOLD" consumer_lag_below telemetry-processor "$PROCESSOR_LAG_THRESHOLD"
  [[ "$OBSERVABILITY_ENABLED" == true ]] &&
    soft_check "consensus unread detections per pod <= $CONSENSUS_LAG_THRESHOLD" consumer_lag_below consensus "$CONSENSUS_LAG_THRESHOLD"
}

ch_q() { k -n "$STORAGE_NAMESPACE" exec clickhouse-0 -c clickhouse -- clickhouse-client -q "$1"; }
# One consumer per Kafka table, except iot.detections which runs CLICKHOUSE_KAFKA_DETECTIONS_CONSUMERS.
# Every one of them must hold partitions, or that table is not consuming.
storage_consumers_expected() { echo $((4 + CLICKHOUSE_KAFKA_DETECTIONS_CONSUMERS)); }
storage_consumers_assigned() {
  local n want
  want=$(storage_consumers_expected)
  n=$(ch_q "SELECT concat(toString(countIf(length(assignments.partition_id) > 0)), '/', toString(count())) FROM system.kafka_consumers WHERE database = 'iot'" 2>&1) ||
    { echo "$n"; return 1; }
  eq "$n" "$want/$want"
}
storage_no_recent_kafka_errors() {
  eq "$(ch_q "SELECT countIf(length(exceptions.time) > 0 AND arrayMax(exceptions.time) > now() - INTERVAL 10 MINUTE) FROM system.kafka_consumers WHERE database = 'iot'" 2>&1)" 0
}
storage_recent_rows() { [[ "$(ch_q "SELECT count() FROM iot.detections WHERE ts > now() - INTERVAL 10 MINUTE" 2>&1)" -gt 0 ]]; }
storage_policy_ok() {
  eq "$(ch_q "SELECT arrayStringConcat(groupArray(volume_name || '=' || arrayStringConcat(disks, ',')), ' ') FROM (SELECT * FROM system.storage_policies WHERE policy_name = 'tiered' ORDER BY volume_priority)" 2>&1)" "hot=default cold=s3_cold_cache"
}
storage_schema_present() {
  eq "$(ch_q "SELECT count() FROM system.tables WHERE database = 'iot' AND name IN ('detections','detections_rejected','vehicles','vehicles_1m','vehicles_1h','traffic','sensor_health','tunnel_profiles','tunnel_rules','speeding_alerts','restricted_alerts','detections_kafka','vehicles_kafka','traffic_kafka','sensor_health_kafka','detections_rejected_kafka')" 2>&1)" 16
}
# The tunnels of apps/simulator/config/tunnels.yaml must be in ClickHouse, or the alert views
# (which join against them) silently return nothing.
storage_profiles_loaded() {
  local want
  want=$(grep -c '^[[:space:]]*-\?[[:space:]]*tunnel_id:' "$SIM_PROFILES_FILE")
  eq "$(ch_q "SELECT count() FROM iot.tunnel_profiles FINAL" 2>&1)" "$want"
}
storage_bucket_exists() {
  k -n "$STORAGE_NAMESPACE" exec seaweedfs-0 -- sh -c "echo s3.bucket.list | weed shell -master=127.0.0.1:9333" 2>/dev/null |
    grep -qE '^[[:space:]]*clickhouse-cold[[:space:]]'
}
# "| grep -q" closes the pipe on the first match, so kubectl dies of SIGPIPE and pipefail fails the
# check even when the output was right: capture first, match after. wget itself exits 8 on the 403.
storage_s3_anonymous_denied() {
  local out
  out=$(k -n "$STORAGE_NAMESPACE" exec clickhouse-0 -c clickhouse -- sh -c \
    "wget -S -O /dev/null http://seaweedfs.$STORAGE_NAMESPACE.svc:8333/clickhouse-cold/ 2>&1" || true)
  [[ "$out" == *" 403 "* ]] || { printf '%s\n' "$out" | tail -n 3; return 1; }
}
storage_default_localhost_only() {
  eq "$(ch_q "SELECT arrayStringConcat(host_names) || '|' || toString(length(host_ip)) FROM system.users WHERE name = 'default'" 2>&1)" "localhost|0"
}
storage_grafana_readonly() {
  ! k -n "$STORAGE_NAMESPACE" exec -i clickhouse-0 -c clickhouse -- sh -c \
    'CLICKHOUSE_PASSWORD=$(cat) clickhouse-client --host clickhouse --user grafana -q "CREATE TABLE iot.verify_probe (x UInt8) ENGINE = Memory"' \
    <"$STATE_DIR/clickhouse-grafana-password" >/dev/null 2>&1
}
storage_metrics() {
  local out
  out=$(k -n "$STORAGE_NAMESPACE" exec clickhouse-0 -c clickhouse -- wget -qO- http://127.0.0.1:9363/metrics 2>&1) ||
    { printf '%s\n' "$out" | tail -n 2; return 1; }
  [[ $'\n'"$out" == *$'\n''ClickHouseMetrics_KafkaConsumers '* ]] || { echo "no ClickHouseMetrics_KafkaConsumers in /metrics"; return 1; }
}

verify_storage() {
  step "verify storage"
  if [[ "$STORAGE_ENABLED" != true ]]; then skip "STORAGE_ENABLED=$STORAGE_ENABLED"; return; fi
  local NS_S=$STORAGE_NAMESPACE
  check "seaweedfs ready" replicas_ready "$NS_S" sts seaweedfs 1
  check "clickhouse ready" replicas_ready "$NS_S" sts clickhouse 1
  check "clickhouse image $CLICKHOUSE_IMAGE" eq "$(k -n "$NS_S" get sts clickhouse -o jsonpath='{.spec.template.spec.containers[0].image}' 2>&1)" "$CLICKHOUSE_IMAGE"
  check "bucket clickhouse-cold exists" storage_bucket_exists
  check "S3 API refuses unsigned requests" storage_s3_anonymous_denied
  check "storage policy tiered: hot=default, cold=s3_cold_cache" storage_policy_ok
  check "database iot: tables, alert views and Kafka tables present" storage_schema_present
  check "tunnel profiles loaded from $(basename "${SIM_PROFILES_FILE:-tunnels.yaml}")" storage_profiles_loaded
  check "$(storage_consumers_expected) Kafka consumers have partitions (SASL_SSL as clickhouse)" storage_consumers_assigned
  check "user default: localhost only" storage_default_localhost_only
  check "user grafana: read-only" storage_grafana_readonly
  check "Prometheus endpoint :9363" storage_metrics
  check "Reloader restarts clickhouse on secret changes" eq "$(k -n "$NS_S" get sts clickhouse -o jsonpath='{.metadata.annotations.reloader\.stakater\.com/auto}' 2>&1)" true
  soft_check "no Kafka consumer exceptions in the last 10 min" storage_no_recent_kafka_errors
  soft_check "detections ingested in the last 10 min (needs pipeline traffic)" storage_recent_rows
}

vm_query() { # vm_query <promql> -> JSON result array, through the API server service proxy (no port-forward)
  k get --raw "/api/v1/namespaces/monitoring/services/vmsingle-vm:8428/proxy/api/v1/query?query=$(jq -rn --arg q "$1" '$q|@uri')" |
    jq -c '.data.result'
}
vm_targets_up() { # the simulator target (make sim-up, on the host) is down by design
  local out down
  out=$(vm_query 'up{job!="simulator"} == 0') || { echo "VictoriaMetrics query failed"; return 1; }
  down=$(jq -r '.[] | "\(.metric.job) \(.metric.namespace // "")/\(.metric.pod // .metric.instance)"' <<<"$out")
  [[ -z "$down" ]] || { echo "down: $(tr '\n' ' ' <<<"$down")"; return 1; }
}
vm_series_at_least() { # <promql> <n>
  local n
  n=$(vm_query "$1" | jq 'length') || return 1
  ((n >= $2)) || { echo "$n series for $1, want >= $2"; return 1; }
}
cr_operational() { eq "$(k -n monitoring get "$1" -o jsonpath='{.status.updateStatus}' 2>&1)" operational; }
vm_metrics_fresh() { # newest scraped sample a query can see: 10s scrape + 5s -search.latencyOffset
  local age
  age=$(vm_query 'time() - max(timestamp(up{job=~"tbmq|telemetry-processor|consensus|kafka|clickhouse"}))' | jq -r '.[0].value[1] // "999"')
  awk -v a="$age" 'BEGIN { exit !(a < 20) }' || { echo "newest sample is ${age}s old, want < 20s"; return 1; }
}
grafana_datasources_healthy() {
  local uid out bad=""
  for uid in VictoriaMetrics VictoriaLogs VictoriaTraces ClickHouse; do
    out=$(k -n monitoring exec deploy/vm-grafana -c grafana -- sh -c \
      "curl -s -u \"admin:\$GF_SECURITY_ADMIN_PASSWORD\" http://localhost:3000/api/datasources/uid/$uid/health" 2>&1)
    jq -e '.status == "OK"' <<<"$out" >/dev/null 2>&1 || bad="$bad $uid"
  done
  [[ -z "$bad" ]] || { echo "unhealthy:$bad"; return 1; }
}

# The dashboards sidecar and the home-dashboard path must agree: a wrong path leaves the landing page
# with an HTTP 500 while every dashboard is present in the IoT folder.
grafana_home_dashboard() {
  local out
  out=$(k -n monitoring exec deploy/vm-grafana -c grafana -- sh -c \
    "curl -s -o /dev/null -w '%{http_code}' -u \"admin:\$GF_SECURITY_ADMIN_PASSWORD\" http://localhost:3000/api/dashboards/home" 2>&1)
  eq "$out" 200
}

verify_observability() {
  step "verify observability"
  if [[ "$OBSERVABILITY_ENABLED" != true ]]; then
    skip "OBSERVABILITY_ENABLED=$OBSERVABILITY_ENABLED"
    return
  fi
  local M=monitoring cr
  check "helm release vm (victoria-metrics-k8s-stack $VM_K8S_STACK_CHART_VERSION) deployed" helm_deployed vm "$M" "$VM_K8S_STACK_CHART_VERSION"
  check "helm release vlogs (victoria-logs-single $VICTORIA_LOGS_CHART_VERSION) deployed" helm_deployed vlogs "$M" "$VICTORIA_LOGS_CHART_VERSION"
  check "helm release vtraces (victoria-traces-single $VICTORIA_TRACES_CHART_VERSION) deployed" helm_deployed vtraces "$M" "$VICTORIA_TRACES_CHART_VERSION"
  check "helm release otel-collector ($OTEL_COLLECTOR_CHART_VERSION) deployed" helm_deployed otel-collector "$M" "$OTEL_COLLECTOR_CHART_VERSION"
  check "VictoriaMetrics operator ready" replicas_ready "$M" deploy vm-victoria-metrics-operator
  for cr in vmsingle/vm vmagent/vm vmalert/vm vmalertmanager/vm; do
    check "$cr operational" cr_operational "$cr"
  done
  check "Grafana ready" replicas_ready "$M" deploy vm-grafana 1
  check "kube-state-metrics ready" replicas_ready "$M" deploy vm-kube-state-metrics
  check "node-exporter ready" ds_ready "$M" vm-prometheus-node-exporter
  check "VictoriaLogs ready" replicas_ready "$M" sts vlogs-server 1
  check "Vector log agents ready" ds_ready "$M" vlogs-vector
  check "VictoriaTraces ready" replicas_ready "$M" sts vtraces-server 1
  check "OTel Collector ready (otel-collector.$M.svc:4317/4318)" replicas_ready "$M" deploy otel-collector 1
  check "VMRule iot-platform present" k -n "$M" get vmrule iot-platform
  check "IoT dashboards present (6)" eq "$(k -n "$M" get cm -l grafana_dashboard=1,app.kubernetes.io/part-of=iot-infrastructure --no-headers 2>/dev/null | wc -l | tr -d ' ')" 6
  check "Grafana alert rules present (ClickHouse tunnel alerts)" \
    eq "$(k -n "$M" get cm -l grafana_alert=1 --no-headers 2>/dev/null | wc -l | tr -d ' ')" 1
  check "vmagent: no scrape target down (simulator excluded)" vm_targets_up
  check "metrics are queryable within 20s of the scrape" vm_metrics_fresh
  check "VMSingle -search.latencyOffset=5s" eq \
    "$(k -n monitoring get vmsingle vm -o jsonpath='{.spec.extraArgs.search\.latencyOffset}' 2>&1)" 5s
  check "pipeline scraped every 10s" eq \
    "$(k -n monitoring get vmpodscrape iot-pipeline -o jsonpath='{.spec.podMetricsEndpoints[0].scrape_interval}' 2>&1)" 10s
  soft_check "TBMQ broker metrics scraped" vm_series_at_least 'connectedSessions{job="tbmq"}' 1
  soft_check "Kafka broker metrics for tbmq-kafka and app-kafka" vm_series_at_least 'count by (strimzi_io_cluster) (up{job="kafka"})' 2
  soft_check "Kafka Exporter metrics for both clusters" vm_series_at_least 'count by (namespace) (up{job="kafka-exporter"})' 2
  soft_check "telemetry-processor and consensus scraped" vm_series_at_least 'count by (job) (up{job=~"telemetry-processor|consensus"})' 2
  soft_check "simulator target present (up/down depending on make sim-up)" k -n monitoring get vmstaticscrape simulator
  soft_check "Grafana datasources healthy (VictoriaMetrics, VictoriaLogs, VictoriaTraces, ClickHouse)" grafana_datasources_healthy
  soft_check "Grafana home dashboard (IoT pipeline overview) loads" grafana_home_dashboard
  soft_check "no critical alert firing" eq "$(vm_query 'count(ALERTS{alertstate="firing",severity="critical"})' 2>/dev/null | jq -r '.[0].value[1] // "0"')" 0
}

verify_e2e() {
  step "verify e2e (real traffic through LB IPs)"
  local ca="$GENERATED_DIR/iot-root-ca.crt" d=$DEVICE_CERTS_DIR
  mkdir -p "$GENERATED_DIR"
  k -n cert-manager get secret iot-root-ca -o jsonpath='{.data.ca\.crt}' 2>/dev/null | base64 -d >"$ca"
  local mqtts=("$MQTT_LB_IP" 8883 --tls --ca "$ca")
  local device=(--cert "$d/T000000.pem" --key "$d/T000000.key" --client-id tunnel-sim-T000000)

  check "L7 http://$EDGE_LB_IP/ → TBMQ UI" http_status 200 "http://$EDGE_LB_IP/"
  check "L7 https://$EDGE_LB_IP/ (cert verified by iot-ca)" http_status 200 --cacert "$ca" "https://$EDGE_LB_IP/"
  check "L7 UI live-update WebSocket /api/ws routed" ws_status_in ' (101|401)( |$)' "$EDGE_LB_IP" 80 /api/ws
  check "L4 mqtt://$MQTT_LB_IP:1883 not exposed" probe_rc 1 mqtt "$MQTT_LB_IP" 1883
  if ! "$ROOT_DIR/scripts/device-certs.sh" 1 >/dev/null 2>&1; then
    check "device certificate T000000 (scripts/device-certs.sh)" false
    return
  fi
  check "L4 mqtts://$MQTT_LB_IP:8883 without client certificate refused" probe_rc_not 0 mqtt "${mqtts[@]}"
  check "L4 mqtts with device cert T000000 → CONNACK 0, publish to own status topic" \
    probe_rc 0 mqtt "${mqtts[@]}" "${device[@]}" --publish simulator/tunnel-sim-T000000/status
  check "ACL: T000000 cannot publish tunnels/T000001/... (other tunnel)" \
    probe_rc 2 mqtt "${mqtts[@]}" "${device[@]}" --publish tunnels/T000001/sensors/start/detections
  check "L4 mqtts with viewer cert (subscribe-only) → CONNACK 0" \
    probe_rc 0 mqtt "${mqtts[@]}" --cert "$d/iot-viewer.pem" --key "$d/iot-viewer.key"
}

# --- main --------------------------------------------------------------------
steps=("$@")
((${#steps[@]})) || steps=("${ALL_STEPS[@]}")
for s in "${steps[@]}"; do
  fn="verify_${s//-/_}"
  declare -F "$fn" >/dev/null || die "unknown step '$s' (valid: ${ALL_STEPS[*]})"
  if [[ "$s" != prereqs && "$s" != k3s ]] && ! api_ready; then
    step "verify $s"
    check "Kubernetes API reachable" false
    continue
  fi
  "$fn"
done
checks_summary

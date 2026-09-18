#!/usr/bin/env bash
# Step 60 — TBMQ data services: Strimzi Kafka (mTLS), CloudNativePG Postgres, Valkey (AOF, password),
# and the one-time TBMQ database schema install.
# Idempotent: kustomize build | kubectl apply; the schema install runs only when the database is empty.
source "$(dirname "$0")/../lib.sh"
require kubectl openssl

NS=$TBMQ_NAMESPACE
step "60 tbmq-deps: Kafka x${KAFKA_REPLICAS} (Strimzi), Postgres x${POSTGRES_INSTANCES} (CNPG), Valkey in $NS"
wait_api
for crd in kafkas.kafka.strimzi.io kafkanodepools.kafka.strimzi.io kafkausers.kafka.strimzi.io clusters.postgresql.cnpg.io; do
  crd_exists "$crd" || die "CRD $crd missing — run: make install-operators"
done

# --- old upstream dependencies (Postgres Deployment, Kafka StatefulSet, Valkey Deployment) ---
legacy=()
for w in deploy/postgres statefulset/tbmq-kafka deploy/tbmq-valkey; do
  if k -n "$NS" get "$w" >/dev/null 2>&1; then legacy+=("$w"); fi
done
if ((${#legacy[@]})); then
  [[ "$LEGACY_DEPS_DELETE" == true ]] ||
    die "old TBMQ dependencies found: ${legacy[*]}. Replacing them DELETES the TBMQ database (MQTT credentials, users, settings) and all Kafka data. To proceed: make install-tbmq-deps LEGACY_DEPS_DELETE=true"
  warn "LEGACY_DEPS_DELETE=true: deleting ${legacy[*]} and their volumes"
  for w in statefulset/tbmq-integration-executor statefulset/tbmq; do
    if k -n "$NS" get "$w" >/dev/null 2>&1; then k -n "$NS" scale "$w" --replicas=0 >/dev/null; fi
  done
  kubectl -n "$NS" delete deploy/postgres deploy/tbmq-valkey statefulset/tbmq-kafka pod/tb-db-setup \
    svc/tbmq-database svc/tbmq-kafka svc/tbmq-kafka-headless svc/tbmq-valkey \
    --ignore-not-found --wait=true --timeout="${WAIT_TIMEOUT}s" | sed 's/^/    /'
  mapfile -t old_pvcs < <(k -n "$NS" get pvc -o name | grep -E '^persistentvolumeclaim/(postgres-pv-claim|kafka-data-tbmq-kafka-[0-9]+)$' || true)
  if ((${#old_pvcs[@]})); then
    kubectl -n "$NS" delete "${old_pvcs[@]}" --wait=true --timeout="${WAIT_TIMEOUT}s" | sed 's/^/    /'
  fi
  ok "old dependencies removed; TBMQ is scaled to 0 until: make install-tbmq"
fi

# --- namespace + Valkey password ------------------------------------------------
k apply -f "$ROOT_DIR/$TBMQ_MANIFESTS_DIR/tbmq-namespace.yml" >/dev/null
mkdir -p "$STATE_DIR" "$GENERATED_DIR"
pw_file="$STATE_DIR/tbmq-valkey-password"
if [[ ! -s "$pw_file" ]]; then
  existing=$(k -n "$NS" get secret tbmq-valkey -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)
  (umask 077 && if [[ -n "$existing" ]]; then printf '%s' "$existing"; else openssl rand -hex 24 | tr -d '\n'; fi >"$pw_file")
fi
# The Secret is never written to disk outside .state/.
k -n "$NS" create secret generic tbmq-valkey --from-file=password="$pw_file" \
  --dry-run=client -o yaml | k apply -f - | sed 's/^/    /'

# --- Kafka, Postgres, Valkey ------------------------------------------------------
KAFKA_REPLICATION_FACTOR=$((KAFKA_REPLICAS < 3 ? KAFKA_REPLICAS : 3))
KAFKA_MIN_ISR=$((KAFKA_REPLICATION_FACTOR > 1 ? KAFKA_REPLICATION_FACTOR - 1 : 1))
export KAFKA_REPLICATION_FACTOR KAFKA_MIN_ISR

OUT="$GENERATED_DIR/tbmq-deps"
mkdir -p "$OUT"
for f in kustomization kafka postgres valkey; do
  render "$DEPLOY_DIR/tbmq-deps/$f.yaml.tpl" >"$OUT/$f.yaml"
done
k kustomize --load-restrictor=LoadRestrictionsNone "$OUT" >"$OUT/rendered.yaml"
k -n "$NS" apply -f "$DEPLOY_DIR/observability/strimzi-kafka-metrics.yaml" | sed 's/^/    /'  # Kafka metricsConfig
# Operator admission webhooks can refuse requests right after the operators start.
retry 120 5 kubectl apply --dry-run=server -f "$OUT/rendered.yaml" || true
kube_apply "$OUT/rendered.yaml"

info "waiting for Kafka, Postgres and Valkey (first start pulls images: a few minutes)"
kubectl -n "$NS" wait kafka/tbmq-kafka --for=condition=Ready --timeout="${WAIT_TIMEOUT}s" >/dev/null ||
  die "Kafka not ready: kubectl -n $NS describe kafka tbmq-kafka"
ok "Kafka tbmq-kafka ready (${KAFKA_REPLICAS} node(s), RF ${KAFKA_REPLICATION_FACTOR}, min ISR ${KAFKA_MIN_ISR}, mTLS listener 9093)"
kubectl -n "$NS" wait kafkauser/tbmq-broker --for=condition=Ready --timeout="${WAIT_TIMEOUT}s" >/dev/null ||
  die "KafkaUser tbmq-broker not ready: kubectl -n $NS describe kafkauser tbmq-broker"
ok "KafkaUser tbmq-broker ready (client certificate in secret tbmq-broker)"
kubectl -n "$NS" wait clusters.postgresql.cnpg.io/tbmq-db --for=condition=Ready --timeout="${WAIT_TIMEOUT}s" >/dev/null ||
  die "Postgres not ready: kubectl -n $NS describe clusters.postgresql.cnpg.io tbmq-db"
ok "Postgres tbmq-db ready (${POSTGRES_INSTANCES} instance(s), primary $(tbmq_db_primary))"
rollout "$NS" statefulset/tbmq-valkey >/dev/null
ok "Valkey tbmq-valkey ready (AOF on PVC, password auth)"

# --- database schema (upstream k8s-install-tbmq.sh logic, made re-runnable) ---
schema_installed() {
  [[ "$(tbmq_db_query "select to_regclass('public.tb_schema_settings') is not null" 2>/dev/null)" == t ]]
}

if schema_installed; then
  skip "TBMQ database schema already installed"
else
  info "installing TBMQ database schema (one-time)"
  SETUP="$GENERATED_DIR/tbmq-db-setup"
  mkdir -p "$SETUP"
  render "$DEPLOY_DIR/tbmq-deps/db-setup/kustomization.yaml.tpl" >"$SETUP/kustomization.yaml"
  k kustomize --load-restrictor=LoadRestrictionsNone "$SETUP" >"$SETUP/rendered.yaml"
  k -n "$NS" delete pod tb-db-setup --ignore-not-found --wait=true >/dev/null
  k apply -f "$SETUP/rendered.yaml" >/dev/null
  kubectl -n "$NS" wait --for=condition=Ready pod/tb-db-setup --timeout="${WAIT_TIMEOUT}s" >/dev/null
  k -n "$NS" exec tb-db-setup -- sh -c 'export INSTALL_TB=true; start-tb-mqtt-broker.sh; touch /tmp/install-finished;'
  k -n "$NS" delete pod tb-db-setup --wait=false >/dev/null
  schema_installed || die "schema install finished but tb_schema_settings table is missing"
  ok "TBMQ database schema installed"
fi

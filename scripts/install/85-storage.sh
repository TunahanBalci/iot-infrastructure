#!/usr/bin/env bash
# Step 85 — storage layer: ClickHouse (hot/warm; Kafka engine ingestion from the app Kafka) and
# SeaweedFS (S3 cold tier) in STORAGE_NAMESPACE. Skipped unless STORAGE_ENABLED=true.
# Idempotent: credentials are generated once into .state/, manifests go through kustomize +
# kube_apply, the bucket is created only when missing, and the ClickHouse schema is re-applied
# with IF NOT EXISTS; TTL and Kafka stream objects only change when their rendered SQL changed.
source "$(dirname "$0")/../lib.sh"
require kubectl envsubst python3 openssl jq sha256sum

if [[ "${STORAGE_ENABLED:-false}" != true ]]; then
  step "85 storage: skipped (STORAGE_ENABLED=${STORAGE_ENABLED:-false})"
  exit 0
fi

NS=$STORAGE_NAMESPACE
BUCKET=clickhouse-cold
step "85 storage: ClickHouse + SeaweedFS (S3 bucket $BUCKET) in $NS ← app Kafka $CLICKHOUSE_KAFKA_BOOTSTRAP"
wait_api
[[ "$CLICKHOUSE_TTL_UNIT" =~ ^(MINUTE|HOUR|DAY)$ ]] || die "CLICKHOUSE_TTL_UNIT must be MINUTE, HOUR or DAY (got '$CLICKHOUSE_TTL_UNIT')"

# --- pipeline prerequisites: KafkaUser clickhouse and the app Kafka cluster CA -----------------
user_secret=$(k -n "$PIPELINE_NAMESPACE" get secret clickhouse -o json 2>/dev/null) ||
  die "secret $PIPELINE_NAMESPACE/clickhouse (KafkaUser clickhouse) missing — install the pipeline first (app Kafka, KafkaUsers)"
ca_secret=$(k -n "$PIPELINE_NAMESPACE" get secret app-kafka-cluster-ca-cert -o json 2>/dev/null) ||
  die "secret $PIPELINE_NAMESPACE/app-kafka-cluster-ca-cert missing — app Kafka not ready yet"

# --- namespace + credentials (.state/, reused from the cluster if .state/ was lost) ---------------
mkdir -p "$STATE_DIR" "$GENERATED_DIR"
OUT="$GENERATED_DIR/storage"
rm -rf "$OUT"
mkdir -p "$OUT"
render "$DEPLOY_DIR/storage/namespace.yaml.tpl" | k apply -f - | sed 's/^/    /'

state_secret() { # state_secret <state-file> <secret> <key> <random-bytes>
  local file="$STATE_DIR/$1" existing
  if [[ ! -s "$file" ]]; then
    existing=$(k -n "$NS" get secret "$2" -o jsonpath="{.data.$3}" 2>/dev/null | base64 -d || true)
    (umask 077 && if [[ -n "$existing" ]]; then printf '%s' "$existing"; else openssl rand -hex "$4" | tr -d '\n'; fi >"$file")
  fi
  printf '%s' "$file"
}
s3_access=$(state_secret seaweedfs-s3-access-key seaweedfs-s3 access-key 10)
s3_secret=$(state_secret seaweedfs-s3-secret-key seaweedfs-s3 secret-key 20)
ch_default=$(state_secret clickhouse-default-password clickhouse-users default-password 16)
ch_grafana=$(state_secret clickhouse-grafana-password clickhouse-grafana password 16)
sha256() { sha256sum "$1" | cut -c1-64; }

# Secrets are built in memory and piped to kubectl: nothing is written outside .state/.
s3_identities() {
  python3 - "$s3_access" "$s3_secret" "$BUCKET" <<'EOF'
import json, sys
access, secret, bucket = (open(sys.argv[1]).read(), open(sys.argv[2]).read(), sys.argv[3])
print(json.dumps({"identities": [{
    "name": "clickhouse",
    "credentials": [{"accessKey": access, "secretKey": secret}],
    "actions": [f"{a}:{bucket}" for a in ("Read", "Write", "List", "Tagging")],
}]}))
EOF
}
k -n "$NS" create secret generic seaweedfs-s3 \
  --from-file=access-key="$s3_access" --from-file=secret-key="$s3_secret" --from-file=s3.json=<(s3_identities) \
  --dry-run=client -o yaml | k apply -f - | sed 's/^/    /'
k -n "$NS" create secret generic clickhouse-users \
  --from-file=default-password="$ch_default" \
  --from-literal=default-password-sha256="$(sha256 "$ch_default")" \
  --from-literal=grafana-password-sha256="$(sha256 "$ch_grafana")" \
  --dry-run=client -o yaml | k apply -f - | sed 's/^/    /'
# For Grafana (the monitoring step reads .state/clickhouse-grafana-password or copies this Secret).
k -n "$NS" create secret generic clickhouse-grafana \
  --from-literal=username=grafana --from-file=password="$ch_grafana" \
  --dry-run=client -o yaml | k apply -f - | sed 's/^/    /'
# KafkaUser password + cluster CA from the pipeline namespace (base64 data copied as is).
jq -n --argjson u "$user_secret" --argjson c "$ca_secret" --arg ns "$NS" '{
  apiVersion: "v1", kind: "Secret", type: "Opaque",
  metadata: {name: "clickhouse-kafka", namespace: $ns, labels: {"app.kubernetes.io/part-of": "iot-infrastructure"}},
  data: {password: $u.data.password, "ca.crt": $c.data["ca.crt"]}}' | k apply -f - | sed 's/^/    /'
ok "credentials: .state/{seaweedfs-s3-*,clickhouse-default-password,clickhouse-grafana-password}; Kafka CA + password copied from $PIPELINE_NAMESPACE"

# --- render: *.tpl → .generated/storage/ (suffix dropped), other files copied ------------------------
to_mib() { # 256Mi / 1Gi → MiB
  case $1 in *Gi) echo $(( ${1%Gi} * 1024 )) ;; *Mi) echo "${1%Mi}" ;; *) die "memory quantity '$1': use Mi or Gi" ;; esac
}
SEAWEEDFS_GOMEMLIMIT="$(( $(to_mib "$SEAWEEDFS_MEMORY_LIMIT") * 80 / 100 ))MiB"
CLICKHOUSE_S3_ENDPOINT="http://seaweedfs.$NS.svc:8333/$BUCKET/"
export SEAWEEDFS_GOMEMLIMIT CLICKHOUSE_S3_ENDPOINT
while IFS= read -r f; do
  mkdir -p "$OUT/$(dirname "$f")"
  if [[ "$f" == *.tpl ]]; then render "$DEPLOY_DIR/storage/$f" >"$OUT/${f%.tpl}"; else cp "$DEPLOY_DIR/storage/$f" "$OUT/$f"; fi
done < <(cd "$DEPLOY_DIR/storage" && find . -type f | sed 's|^\./||' | sort)
k kustomize "$OUT" >"$OUT/rendered.yaml"
kube_apply "$OUT/rendered.yaml"

# --- SeaweedFS + bucket -------------------------------------------------------------------------
info "waiting for SeaweedFS (first start pulls the image)"
rollout "$NS" statefulset/seaweedfs >/dev/null
weed_shell() { kubectl -n "$NS" exec seaweedfs-0 -c seaweedfs -- sh -c "echo '$1' | weed shell -master=127.0.0.1:9333"; }
if weed_shell s3.bucket.list 2>/dev/null | grep -qE "^[[:space:]]*$BUCKET[[:space:]]"; then
  skip "bucket $BUCKET exists"
else
  weed_shell "s3.bucket.create -name $BUCKET" | sed 's/^/    /'
  weed_shell s3.bucket.list | grep -qE "^[[:space:]]*$BUCKET[[:space:]]" || die "bucket $BUCKET not created: kubectl -n $NS logs seaweedfs-0"
  ok "bucket $BUCKET created"
fi
ok "SeaweedFS ready (S3 http://seaweedfs.$NS.svc:8333, identity clickhouse limited to $BUCKET)"

# --- ClickHouse ---------------------------------------------------------------------------------
info "waiting for ClickHouse"
if ! rollout "$NS" statefulset/clickhouse >/dev/null; then
  k -n "$NS" logs clickhouse-0 --tail 20 >&2 || true
  die "ClickHouse not ready: kubectl -n $NS describe pod clickhouse-0"
fi
ok "ClickHouse ready ($CLICKHOUSE_IMAGE, memory limit $CLICKHOUSE_MEMORY_LIMIT)"

# clickhouse-client inside the pod: user default over localhost, password from the pod env.
ch() { kubectl -n "$NS" exec clickhouse-0 -c clickhouse -- clickhouse-client "$@"; }
ch_file() { kubectl -n "$NS" exec -i clickhouse-0 -c clickhouse -- clickhouse-client --multiquery <"$1"; }
applied_checksum() { ch -q "SELECT checksum FROM iot.schema_state FINAL WHERE name = '$1'"; }

# S3 disk end to end (credentials, bucket permissions): write, read back and drop a probe part.
ch --multiquery -q "
  DROP TABLE IF EXISTS default.storage_s3_probe SYNC;
  CREATE TABLE default.storage_s3_probe (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS disk = 's3_cold';
  INSERT INTO default.storage_s3_probe VALUES (1);
  SELECT throwIf(count() != 1, 'S3 probe read failed') FROM default.storage_s3_probe FORMAT Null;
  DROP TABLE default.storage_s3_probe SYNC;" ||
  die "ClickHouse cannot write to the S3 disk (bucket $BUCKET): kubectl -n $NS logs clickhouse-0 | grep -i s3"
ok "S3 disk s3_cold: probe part written to and read from SeaweedFS"

# --- schema -------------------------------------------------------------------------------------
S="$OUT/schema"
ch_file "$S/10-tables.sql"
ok "database iot: tables present"

# 20-ttl: tiering/retention intervals. MODIFY TTL only when the rendered file changed.
sum=$(sha256 "$S/20-ttl.sql")
if [[ "$(applied_checksum 20-ttl)" == "$sum" ]]; then
  skip "TTL unchanged"
else
  ch_file "$S/20-ttl.sql"
  ch -q "INSERT INTO iot.schema_state (name, checksum) VALUES ('20-ttl', '$sum')"
  ok "TTL applied: detections cold ${CLICKHOUSE_DETECTIONS_COLD_AFTER}/delete ${CLICKHOUSE_DETECTIONS_DELETE_AFTER}, vehicles ${CLICKHOUSE_VEHICLES_COLD_AFTER}/${CLICKHOUSE_VEHICLES_DELETE_AFTER}, 1h rollup ${CLICKHOUSE_ROLLUP_1H_COLD_AFTER}/${CLICKHOUSE_ROLLUP_1H_DELETE_AFTER} (${CLICKHOUSE_TTL_UNIT})"
fi

ch_file "$S/30-views.sql"

# 50-profiles: the tunnels of the simulator's profile file (names, speed limits, access rules) as
# rows, so the alert views judge traffic by exactly what the devices simulate. Re-applied when the
# profile file changes (ReplacingMergeTree: a re-applied tunnel replaces its row).
python3 "$ROOT_DIR/scripts/render_profiles_sql.py" "$SIM_PROFILES_FILE" >"$S/50-profiles.sql"
sum=$(sha256 "$S/50-profiles.sql")
if [[ "$(applied_checksum 50-profiles)" == "$sum" ]]; then
  skip "tunnel profiles unchanged"
else
  ch_file "$S/50-profiles.sql"
  ch -q "INSERT INTO iot.schema_state (name, checksum) VALUES ('50-profiles', '$sum')"
  ok "tunnel profiles and access rules loaded from $(basename "$SIM_PROFILES_FILE")"
fi

# 40-streams: Kafka tables + materialized views (no data). When the rendered file changed, drop
# them all and recreate. Kafka tables go first: dropping one stops its consumers; a block not yet
# written is discarded uncommitted and read again by the new tables (offsets live in Kafka).
sum=$(sha256 "$S/40-streams.sql")
if [[ "$(applied_checksum 40-streams)" == "$sum" ]]; then
  skip "Kafka streams unchanged"
else
  for engine in Kafka MaterializedView Null EmbeddedRocksDB; do
    for t in $(ch -q "SELECT name FROM system.tables WHERE database = 'iot' AND engine = '$engine' ORDER BY name"); do
      ch -q "DROP TABLE IF EXISTS iot.\`$t\` SYNC"
      info "dropped iot.$t ($engine)"
    done
  done
  ch_file "$S/40-streams.sql"
  ch -q "INSERT INTO iot.schema_state (name, checksum) VALUES ('40-streams', '$sum')"
  ok "Kafka streams created (groups clickhouse-detections, -rejected, -vehicles, -traffic, -sensor-health)"
fi

# --- Kafka consumers ----------------------------------------------------------------------------
consumers_assigned() {
  [[ "$(ch -q "SELECT countIf(length(assignments.partition_id) > 0) FROM system.kafka_consumers WHERE database = 'iot'")" -ge 5 ]]
}
if retry 120 5 consumers_assigned; then
  ok "5 Kafka consumers have partition assignments ($CLICKHOUSE_KAFKA_BOOTSTRAP, SASL_SSL as clickhouse)"
else
  ch -q "SELECT table, length(assignments.partition_id) AS partitions, substring(arrayElement(exceptions.text, -1), 1, 200) AS last_error
         FROM system.kafka_consumers WHERE database = 'iot' FORMAT PrettyCompactMonoBlock" 2>&1 | sed 's/^/    /' >&2 || true
  warn "Kafka consumers without assignment: topics/ACLs for KafkaUser clickhouse, NetworkPolicy of app-kafka (admit $NS pods app=clickhouse)"
fi

info "SQL:      kubectl -n $NS exec -it clickhouse-0 -- clickhouse-client"
info "Grafana:  clickhouse.$NS.svc:9000 (native) / :8123 (HTTP), user grafana, password Secret $NS/clickhouse-grafana"
info "metrics:  clickhouse.$NS.svc:9363/metrics, seaweedfs.$NS.svc:9327/metrics"

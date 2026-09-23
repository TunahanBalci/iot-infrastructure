#!/usr/bin/env bash
# Drop every row whose tunnel is not one of the configured profile tunnels (iot.tunnel_profiles).
#
# `make sim-up MODE=generator` writes thousands of synthetic T0000NN tunnels into the same
# tables as the deployment. They outnumber the real rows ~250:1, which swamps the tunnel
# variable and the device tables in Grafana. Deleting 86M rows one by one is slower than
# keeping the few that matter: copy them aside, swap the table in, drop the old one.
#
#   scripts/clickhouse-prune.sh            # show what would go
#   scripts/clickhouse-prune.sh --apply    # do it

set -Eeuo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_config

APPLY=false
[[ "${1:-}" == "--apply" ]] && APPLY=true

# tunnel_id columns only; the rest key on event_id or have no tunnel at all.
TABLES=(detections sensor_health traffic vehicles vehicles_1m vehicles_1h)
KEEP="tunnel_id IN (SELECT tunnel_id FROM iot.tunnel_profiles FINAL)"

ch() { kubectl -n "$STORAGE_NAMESPACE" exec -i clickhouse-0 -c clickhouse -- clickhouse-client "$@"; }

info "profile tunnels: $(ch -q 'SELECT groupArray(tunnel_id) FROM iot.tunnel_profiles FINAL')"
for t in "${TABLES[@]}"; do
  read -r keep purge < <(ch -q "SELECT countIf($KEEP), countIf(NOT ($KEEP)) FROM iot.$t FORMAT TSV")
  printf '  %-14s keep %10s   purge %10s\n' "$t" "$keep" "$purge"
  $APPLY || continue
  [[ "$purge" == "0" ]] && continue
  # EXCHANGE is atomic, so the materialized views keep writing into whichever table holds
  # the name; at most the few rows inserted during the copy are lost.
  ch -q "CREATE TABLE iot.${t}__keep AS iot.$t"
  ch -q "INSERT INTO iot.${t}__keep SELECT * FROM iot.$t WHERE $KEEP"
  ch -q "EXCHANGE TABLES iot.$t AND iot.${t}__keep"
  ch -q "DROP TABLE iot.${t}__keep SYNC"
  ok "$t pruned to $(ch -q "SELECT count() FROM iot.$t FORMAT TSV") rows"
done

$APPLY || warn "dry run; re-run with --apply to delete the rows above"

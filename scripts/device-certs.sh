#!/usr/bin/env bash
# MQTT client certificates for mutual TLS on 8883, signed by the device CA (cert-manager/iot-device-ca).
#   scripts/device-certs.sh                     # the tunnels of SIM_PROFILES_FILE (MODE=profiles)
#   scripts/device-certs.sh COUNT [OFFSET]      # generated tunnels T000000.. (MODE=generator)
# Writes to DEVICE_CERTS_DIR:
#   T000000.pem / T000000.key ...  one per tunnel: CN = tunnel id; certificate followed by the device CA
#   iot-viewer.pem / .key          subscribe-only client (make consensus-watch)
#   server-ca.crt                  CA of the TBMQ server certificate (iot-root-ca), to verify the broker
# Idempotent: certificates are kept until they expire within 30 days or the device CA changes.
# The CA private key is only held in a temporary directory while signing.
source "$(dirname "$0")/lib.sh"
require kubectl openssl xargs

MODE=${MODE:-$SIM_MODE}
COUNT=${1:-$SIM_TUNNELS}
OFFSET=${2:-0}

# Tunnel ids: the curated profiles, or the simulator's topology.tunnel_id_format (T{index:06d}).
tunnel_ids() {
  if [[ $# -eq 0 && $MODE == profiles ]]; then
    [[ -f $SIM_PROFILES_FILE ]] || die "profiles file not found: $SIM_PROFILES_FILE"
    sed -n 's/^[[:space:]]*-\?[[:space:]]*tunnel_id:[[:space:]]*\([A-Za-z0-9_-]\+\).*/\1/p' "$SIM_PROFILES_FILE"
  else
    for ((i = OFFSET; i < OFFSET + COUNT; i++)); do printf 'T%06d\n' "$i"; done
  fi
}
DIR=$DEVICE_CERTS_DIR
MARKER="$DIR/.device-ca-fingerprint"

[[ "$(openssl version | awk '{print $2}')" == [3-9]* ]] || die "OpenSSL 3 or newer required (req -CA)"
api_ready || die "Kubernetes API not reachable (the device CA is the secret cert-manager/iot-device-ca)"

mkdir -p "$DIR"
chmod 700 "$DIR"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
(
  umask 077
  k -n cert-manager get secret iot-device-ca -o jsonpath='{.data.tls\.crt}' | base64 -d >"$work/ca.crt"
  k -n cert-manager get secret iot-device-ca -o jsonpath='{.data.tls\.key}' | base64 -d >"$work/ca.key"
)
[[ -s "$work/ca.crt" && -s "$work/ca.key" ]] || die "secret cert-manager/iot-device-ca missing — run: make install-cert-manager"
k -n cert-manager get secret iot-root-ca -o jsonpath='{.data.ca\.crt}' | base64 -d >"$DIR/server-ca.crt"

# A new device CA invalidates every certificate in the directory.
fingerprint=$(openssl x509 -in "$work/ca.crt" -noout -fingerprint -sha256 | cut -d= -f2)
if [[ ! -f "$MARKER" ]]; then
  if find "$DIR" -maxdepth 1 \( -name '*.pem' -o -name '*.key' \) | grep -q .; then
    die "$DIR contains certificates but no $MARKER — not a device-certs directory, refusing to touch it"
  fi
  echo "$fingerprint" >"$MARKER"
elif [[ "$(<"$MARKER")" != "$fingerprint" ]]; then
  warn "device CA changed: re-issuing all certificates in $DIR"
  find "$DIR" -maxdepth 1 \( -name '*.pem' -o -name '*.key' \) ! -name 'server-ca.*' -delete
  echo "$fingerprint" >"$MARKER"
fi

issue() { # issue <cn>  — prints "issued" when a certificate was (re)created
  local cn=$1 tmp
  if [[ -s "$DIR/$cn.pem" && -s "$DIR/$cn.key" ]] &&
     openssl x509 -in "$DIR/$cn.pem" -noout -checkend 2592000 >/dev/null; then
    return 0
  fi
  tmp="$DIR/.$cn.tmp"
  (
    umask 077
    openssl req -x509 -new -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
      -keyout "$tmp.key" -subj "/CN=$cn" -days "$DEVICE_CERT_DAYS" -CA "$CA_CRT" -CAkey "$CA_KEY" \
      -addext basicConstraints=critical,CA:FALSE -addext keyUsage=critical,digitalSignature \
      -addext extendedKeyUsage=clientAuth -out "$tmp.crt" 2>/dev/null
    # Clients send the whole file: TBMQ matches the CA's CN in the presented chain.
    cat "$tmp.crt" "$CA_CRT" >"$tmp.pem"
  )
  mv "$tmp.key" "$DIR/$cn.key"
  mv "$tmp.pem" "$DIR/$cn.pem"
  rm -f "$tmp.crt"
  echo issued
}
export -f issue
export DIR DEVICE_CERT_DAYS CA_CRT="$work/ca.crt" CA_KEY="$work/ca.key"

ids=$(tunnel_ids "$@")
issued=$(
  { echo iot-viewer; [[ -n $ids ]] && echo "$ids"; } |
    xargs -P "$(nproc)" -n 64 bash -ec 'for cn; do issue "$cn"; done' _ | grep -c issued || true
)
total=$(find "$DIR" -maxdepth 1 -name '*.pem' | wc -l)
count=$([[ -n $ids ]] && echo "$ids" | wc -l || echo 0)
ok "device certificates: ${issued} issued, ${total} in $DIR (${count} tunnels + iot-viewer)"

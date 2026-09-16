#!/usr/bin/env bash
# Step 00 — host prerequisites and CLI tools (helm, cilium, hubble).
# Idempotent: tools already at the pinned version are left alone.
source "$(dirname "$0")/../lib.sh"

step "00 prereqs: host tools"

# System packages we don't install automatically (need the distro package manager).
missing=()
for c in curl jq envsubst openssl python3 tar sha256sum systemctl; do has "$c" || missing+=("$c"); done
if ((${#missing[@]})); then
  die "install with your package manager first: ${missing[*]}  (Debian/Ubuntu: sudo apt install curl jq gettext-base openssl python3 tar coreutils)"
fi
ok "system tools present (curl jq envsubst openssl python3)"

has docker && ok "docker present (needed only for make sim-up)" || warn "docker missing — make sim-up will not work"

# Config reloaders, Vector, Grafana and the operators all watch files. The distro default of 128
# inotify instances per user runs out on a node this dense, and watchers then fail to start with
# "too many open files" — which looks like an application bug, not a host limit.
inotify_ok() { (($(sysctl -n "fs.inotify.$1" 2>/dev/null || echo 0) >= $2)); }
if inotify_ok max_user_instances 512 && inotify_ok max_user_watches 262144; then
  ok "inotify limits sufficient (max_user_instances $(sysctl -n fs.inotify.max_user_instances))"
else
  warn "inotify limits are low (instances $(sysctl -n fs.inotify.max_user_instances 2>/dev/null), watches $(sysctl -n fs.inotify.max_user_watches 2>/dev/null)) — raise them:
      printf 'fs.inotify.max_user_instances=512\\nfs.inotify.max_user_watches=524288\\n' | sudo tee /etc/sysctl.d/99-iot-infra.conf && sudo sysctl --system"
fi

mkdir -p "$TOOLS_BIN_DIR"
case "$(uname -m)" in
  x86_64 | amd64) ARCH=amd64 ;;
  aarch64 | arm64) ARCH=arm64 ;;
  *) die "unsupported architecture $(uname -m)" ;;
esac
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# fetch_verified <url> <sha256-url> <dest>
fetch_verified() {
  curl -fsSL --retry 3 -o "$3" "$1"
  local want
  want=$(curl -fsSL --retry 3 "$2" | awk '{print $1}')
  echo "$want  $3" | sha256sum -c --quiet - || die "checksum mismatch for $1"
}

# --- helm ---
if has helm && [[ "$(helm version --template '{{.Version}}' 2>/dev/null)" == "$HELM_VERSION" ]]; then
  skip "helm $HELM_VERSION already installed"
elif has helm; then
  ok "helm $(helm version --template '{{.Version}}') present (pinned $HELM_VERSION; keeping existing)"
else
  info "installing helm $HELM_VERSION → $TOOLS_BIN_DIR"
  f="helm-$HELM_VERSION-linux-$ARCH.tar.gz"
  fetch_verified "https://get.helm.sh/$f" "https://get.helm.sh/$f.sha256sum" "$TMP/$f"
  tar -xzf "$TMP/$f" -C "$TMP"
  install -m 0755 "$TMP/linux-$ARCH/helm" "$TOOLS_BIN_DIR/helm"
  ok "helm $HELM_VERSION installed"
fi

# --- cilium-cli ---
if has cilium && cilium version --client 2>/dev/null | grep -q "cilium-cli: $CILIUM_CLI_VERSION"; then
  skip "cilium-cli $CILIUM_CLI_VERSION already installed"
else
  info "installing cilium-cli $CILIUM_CLI_VERSION → $TOOLS_BIN_DIR"
  f="cilium-linux-$ARCH.tar.gz"
  base="https://github.com/cilium/cilium-cli/releases/download/$CILIUM_CLI_VERSION"
  fetch_verified "$base/$f" "$base/$f.sha256sum" "$TMP/$f"
  tar -xzf "$TMP/$f" -C "$TMP"
  install -m 0755 "$TMP/cilium" "$TOOLS_BIN_DIR/cilium"
  ok "cilium-cli $CILIUM_CLI_VERSION installed"
fi

# --- hubble ---
if has hubble && hubble version 2>/dev/null | grep -q "${HUBBLE_VERSION#v}"; then
  skip "hubble $HUBBLE_VERSION already installed"
else
  info "installing hubble $HUBBLE_VERSION → $TOOLS_BIN_DIR"
  f="hubble-linux-$ARCH.tar.gz"
  base="https://github.com/cilium/hubble/releases/download/$HUBBLE_VERSION"
  fetch_verified "$base/$f" "$base/$f.sha256sum" "$TMP/$f"
  tar -xzf "$TMP/$f" -C "$TMP"
  install -m 0755 "$TMP/hubble" "$TOOLS_BIN_DIR/hubble"
  ok "hubble $HUBBLE_VERSION installed"
fi

case ":$PATH:" in
  *":$TOOLS_BIN_DIR:"*) ;;
  *) warn "$TOOLS_BIN_DIR is not on your shell PATH (scripts add it automatically)" ;;
esac

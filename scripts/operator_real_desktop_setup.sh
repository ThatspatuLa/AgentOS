#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$ROOT/data/operator"
ARTIFACT_DIR="$DATA_DIR/artifacts/real-desktop"
CREDENTIAL_FILE="$DATA_DIR/.rdp-credentials"
CERT_FILE="$ARTIFACT_DIR/rdp-cert.pem"
KEY_FILE="$ARTIFACT_DIR/rdp-key.pem"
RDP_PORT="${OPERATOR_RDP_PORT:-3389}"

mkdir -p "$ARTIFACT_DIR"
chmod 700 "$DATA_DIR" "$ARTIFACT_DIR"

if [[ ! -s "$CREDENTIAL_FILE" ]]; then
  RDP_USER="zenoperator"
  RDP_PASSWORD="zen-$(openssl rand -hex 6)"
  printf 'RDP_USER=%q\nRDP_PASSWORD=%q\n' "$RDP_USER" "$RDP_PASSWORD" > "$CREDENTIAL_FILE"
  chmod 600 "$CREDENTIAL_FILE"
fi

# Generated locally and used only by GNOME Remote Desktop on loopback.
if [[ ! -s "$CERT_FILE" || ! -s "$KEY_FILE" ]]; then
  openssl req -new -newkey rsa:2048 -nodes -x509 -days 3650 \
    -subj "/CN=Agent OS Local Operator" \
    -keyout "$KEY_FILE" \
    -out "$CERT_FILE" >/dev/null 2>&1
  chmod 600 "$KEY_FILE"
fi

# shellcheck disable=SC1090
source "$CREDENTIAL_FILE"

grdctl rdp set-tls-cert "$CERT_FILE"
grdctl rdp set-tls-key "$KEY_FILE"
grdctl rdp set-credentials "$RDP_USER" "$RDP_PASSWORD"
grdctl rdp set-port "$RDP_PORT"
grdctl rdp disable-port-negotiation
grdctl rdp disable-view-only
grdctl rdp enable
systemctl --user restart gnome-remote-desktop.service

echo "GNOME real-desktop sharing enabled on localhost:${RDP_PORT}."
echo "Credentials are stored in ${CREDENTIAL_FILE} (mode 600)."
grdctl status

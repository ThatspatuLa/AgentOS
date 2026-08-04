#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CREDENTIAL_FILE="$ROOT/data/operator/.rdp-credentials"
ARTIFACT_DIR="$ROOT/data/operator/artifacts/real-desktop"
PID_FILE="$ARTIFACT_DIR/freerdp.pid"
LOG_FILE="$ARTIFACT_DIR/freerdp.log"
DISPLAY_NAME="${OPERATOR_VNC_DISPLAY:-:2}"
RDP_PORT="${OPERATOR_RDP_PORT:-3389}"
RDP_SIZE="${OPERATOR_RDP_SIZE:-1440x900}"

FREERDP="$(command -v xfreerdp3 || command -v xfreerdp || true)"
if [[ -z "$FREERDP" ]]; then
  echo "FreeRDP X11 client is missing. Install package: freerdp3-x11" >&2
  exit 2
fi
if [[ ! -s "$CREDENTIAL_FILE" ]]; then
  echo "GNOME RDP credentials are missing. Run scripts/operator_real_desktop_setup.sh first." >&2
  exit 3
fi

mkdir -p "$ARTIFACT_DIR"
# shellcheck disable=SC1090
source "$CREDENTIAL_FILE"

bridge_pid_active() {
  local pid="${1:-}"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  ps -p "$pid" -o comm= 2>/dev/null | grep -qi "xfreerdp"
}

if [[ -s "$PID_FILE" ]] && bridge_pid_active "$(cat "$PID_FILE")"; then
  echo "Real desktop bridge already running."
  exit 0
fi

setsid bash -c '
  pid_file="$1"
  shift
  echo "$$" > "$pid_file"
  exec "$@"
' agent-os-rdp "$PID_FILE" env DISPLAY="$DISPLAY_NAME" "$FREERDP" \
    "/v:127.0.0.1:${RDP_PORT}" \
    "/u:${RDP_USER}" \
    "/p:${RDP_PASSWORD}" \
    /cert:ignore \
    /gfx \
    "/size:${RDP_SIZE}" \
    /network:lan \
    /audio-mode:2 \
    </dev/null \
    >"$LOG_FILE" 2>&1 &

sleep 5
if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  BRIDGE_PID="$(pgrep -f "xfreerdp.*127[.]0[.]0[.]1:${RDP_PORT}" | head -n 1 || true)"
  if [[ -n "$BRIDGE_PID" ]]; then
    echo "$BRIDGE_PID" > "$PID_FILE"
  fi
fi
if ! bridge_pid_active "$(cat "$PID_FILE")"; then
  echo "Real desktop bridge failed to start. See ${LOG_FILE}." >&2
  exit 4
fi

echo "Real GNOME desktop bridge running on VNC display ${DISPLAY_NAME} at ${RDP_SIZE}."

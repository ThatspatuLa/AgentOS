#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PASS_TXT="$ROOT/data/operator/.vnc-password"
PASS_FILE="$ROOT/data/operator/.vnc-passwd"
LOG_DIR="$ROOT/data/operator/artifacts/m7"
XSTARTUP="$LOG_DIR/xstartup"
TAILSCALE_IP="${OPERATOR_TAILSCALE_IP:-}"
VNC_DISPLAY="${OPERATOR_VNC_DISPLAY:-:2}"
VNC_PORT="${OPERATOR_VNC_PORT:-5902}"
NOVNC_PORT="${OPERATOR_NOVNC_PORT:-6080}"
GEOMETRY="${OPERATOR_VNC_GEOMETRY:-1920x1080}"

mkdir -p "$ROOT/data/operator" "$LOG_DIR"
chmod 700 "$ROOT/data/operator"

if [[ -z "$TAILSCALE_IP" ]]; then
  TAILSCALE_IP="$(tailscale ip -4 | head -n 1)"
fi

if [[ ! -s "$PASS_TXT" ]]; then
  openssl rand -base64 12 | tr -dc 'A-Za-z0-9' | head -c 8 > "$PASS_TXT"
  printf '\n' >> "$PASS_TXT"
  chmod 600 "$PASS_TXT"
fi

vncpasswd -f < "$PASS_TXT" > "$PASS_FILE"
chmod 600 "$PASS_FILE"

AGENT_OS_NOTICE="$ROOT/scripts/operator_vnc_notice.py"

cat > "$XSTARTUP" <<EOF
#!/usr/bin/env bash
unset SESSION_MANAGER
unset DBUS_SESSION_BUS_ADDRESS
if command -v xsetroot >/dev/null 2>&1; then
  xsetroot -solid '#102033'
fi
if [[ -x "$AGENT_OS_NOTICE" ]]; then
  "$AGENT_OS_NOTICE" &
fi
if command -v x-terminal-emulator >/dev/null 2>&1; then
  x-terminal-emulator &
fi
if command -v xmessage >/dev/null 2>&1; then
  xmessage -center -buttons 'OK:0' 'Agent OS M7 Observation

Connected successfully.
This is the virtual desktop observation surface.
Desktop control remains disabled.' &
fi
while true; do
  sleep 3600
done
EOF
chmod 700 "$XSTARTUP"

if tigervncserver -list | awk '{print $1}' | grep -qx "${VNC_DISPLAY#:}"; then
  echo "TigerVNC ${VNC_DISPLAY} already running."
else
  tigervncserver "$VNC_DISPLAY" \
    -localhost yes \
    -geometry "$GEOMETRY" \
    -depth 24 \
    -SecurityTypes VncAuth \
    -PasswordFile "$PASS_FILE" \
    -UseBlacklist=0 \
    -desktop "Agent OS M7" \
    -xstartup "$XSTARTUP"
fi

if [[ -x "$ROOT/scripts/operator_real_desktop_start.sh" ]]; then
  if ! "$ROOT/scripts/operator_real_desktop_start.sh"; then
    echo "Real desktop bridge is not ready; keeping the visible virtual fallback."
  fi
fi

if pgrep -f "[w]ebsockify .*${NOVNC_PORT} .*127.0.0.1:${VNC_PORT}" >/dev/null; then
  echo "noVNC websockify already running on ${NOVNC_PORT}."
else
  websockify \
    --daemon \
    --web=/usr/share/novnc \
    --log-file="$LOG_DIR/novnc.log" \
    "${TAILSCALE_IP}:${NOVNC_PORT}" \
    "127.0.0.1:${VNC_PORT}"
fi

cat <<MSG
M7 noVNC observation started.

URL:
  http://${TAILSCALE_IP}:${NOVNC_PORT}/vnc.html?host=${TAILSCALE_IP}&port=${NOVNC_PORT}&path=websockify&autoconnect=true&resize=scale&view_only=true

Password:
  $(cat "$PASS_TXT")

Notes:
  VNC listens on localhost only (${VNC_PORT}).
  noVNC is bound to Tailscale IP ${TAILSCALE_IP}:${NOVNC_PORT}.
  When the GNOME RDP bridge is active, noVNC shows your real desktop; otherwise it shows the virtual fallback.
MSG

#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TAILSCALE_IP="${OPERATOR_TAILSCALE_IP:-}"
if [[ -z "$TAILSCALE_IP" ]]; then
  TAILSCALE_IP="$(tailscale ip -4 | head -n 1)"
fi

cd "$ROOT"
exec /usr/bin/python3 agent_os_server.py --host "$TAILSCALE_IP" --port 8765

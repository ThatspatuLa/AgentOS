#!/usr/bin/env bash
set -euo pipefail

# M7 noVNC observation setup helper.
#
# Run this from a normal terminal, not from Codex, because sudo and user-session
# services need interactive access. This prepares the packages Agent OS checks
# for; final service configuration still depends on whether you want a virtual
# VNC desktop or GNOME Remote Desktop view-only observation.

echo "Installing M7 observation packages..."
sudo apt-get install -y novnc websockify tigervnc-standalone-server

cat <<'MSG'

Installed package baseline:
- novnc: browser VNC client
- websockify: WebSocket bridge for noVNC
- tigervnc-standalone-server: virtual VNC desktop backend

Next decisions:
1. Virtual desktop noVNC:
   - safer to automate
   - does not observe the already-running GNOME desktop
   - can be served through noVNC on localhost/Tailscale

2. GNOME Remote Desktop view-only:
   - observes the real GNOME Wayland desktop
   - uses RDP, not noVNC
   - requires credentials/cert setup in the real user session

Agent OS M7 status endpoint:
  /api/operator/observe/status

After install, restart Agent OS and recheck the mobile Manual tab.
MSG

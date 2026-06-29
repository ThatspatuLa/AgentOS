#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PRIVATE_ENV="${OPERATOR_DISCORD_ENV:-$HOME/.config/zennew/discord.env}"

if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

if [[ -f "$PRIVATE_ENV" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$PRIVATE_ENV"
  set +a
fi

cd "$ROOT"
exec "$ROOT/.venv/bin/python" "$ROOT/bot/main.py"

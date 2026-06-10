#!/bin/bash
# Ollama usage scraper — parses journalctl logs for per-model token usage
# Run this from generate-data.py or standalone

LOG_SINCE="${1:-24 hours ago}"

echo "=== Ollama API: Installed Models ==="
MODELS_JSON=$(curl -s http://localhost:11434/api/tags 2>/dev/null)
if [ -z "$MODELS_JSON" ]; then
  echo "ERROR: Ollama not running or not accessible"
  exit 1
fi

echo "$MODELS_JSON" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for m in data.get('models', []):
    name = m['name']
    size = m.get('size', 0)
    ctx = m.get('details', {}).get('context_length', 'unknown')
    params = m.get('details', {}).get('parameter_size', 'unknown')
    quant = m.get('details', {}).get('quantization_level', 'unknown')
    print(f'{name}|{size}|{ctx}|{params}|{quant}')
"

echo ""
echo "=== Ollama Server Logs: Request Summary ==="
journalctl -u ollama --since "$LOG_SINCE" --no-pager 2>/dev/null | \
  grep -E "POST /(api/generate|api/chat|v1/chat/completions|completion)" | \
  awk '{print $1, $2, $11, $13}' | head -50

echo ""
echo "=== Ollama Server Logs: Token Counts per Request ==="
journalctl -u ollama --since "$LOG_SINCE" --no-pager 2>/dev/null | \
  grep -E "n_tokens =|total time =" | \
  head -30

echo ""
echo "=== Ollama Server Logs: Model Load Events ==="
journalctl -u ollama --since "$LOG_SINCE" --no-pager 2>/dev/null | \
  grep -E "model loaded|starting llama-server|template selection" | \
  tail -20

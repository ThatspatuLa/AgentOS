#!/usr/bin/env python3
from pathlib import Path
from getpass import getpass
from datetime import datetime, timezone
import json
import os
import urllib.request
import urllib.error

MODEL = "openrouter/owl-alpha"

def load_env_key(path: Path, name: str) -> str:
    if not path.exists():
        return ""
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if line.startswith(name + "="):
            return line.split("=", 1)[1].strip().strip("'").strip('"')
    return ""

key = (
    os.environ.get("OPENROUTER_API_KEY", "").strip()
    or load_env_key(Path.home() / ".hermes" / ".env", "OPENROUTER_API_KEY")
    or load_env_key(Path.home() / "Projects" / "HermesRoot" / "litellm-router" / ".env.local", "OPENROUTER_API_KEY")
)

if not key:
    key = getpass("Paste OpenRouter API key: ").strip()

if not key:
    raise SystemExit("No OpenRouter API key found.")

payload = {
    "model": MODEL,
    "messages": [
        {"role": "user", "content": "Reply with exactly: OWL quota reset works."}
    ],
}

request = urllib.request.Request(
    "https://openrouter.ai/api/v1/chat/completions",
    data=json.dumps(payload).encode("utf-8"),
    headers={
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    },
    method="POST",
)

try:
    with urllib.request.urlopen(request, timeout=60) as response:
        body = response.read().decode("utf-8", errors="replace")
        data = json.loads(body)

    message = data["choices"][0]["message"]["content"]
    model = data.get("model")
    usage = data.get("usage", {})

    print("RESULT: SUCCESS")
    print("model:", model)
    print("message:", message)
    print("total_tokens:", usage.get("total_tokens"))
    print("cost:", usage.get("cost"))

except urllib.error.HTTPError as error:
    body = error.read().decode("utf-8", errors="replace")
    print("RESULT: FAILED")
    print("http_status:", error.code)

    try:
        data = json.loads(body)
        err = data.get("error", {})
        print("error_message:", err.get("message"))
        print("error_code:", err.get("code"))

        headers = ((err.get("metadata") or {}).get("headers") or {})
        if headers:
            print("rate_limit_limit:", headers.get("X-RateLimit-Limit"))
            print("rate_limit_remaining:", headers.get("X-RateLimit-Remaining"))

            reset_raw = headers.get("X-RateLimit-Reset")
            if reset_raw:
                reset_ms = int(reset_raw)
                reset_dt = datetime.fromtimestamp(reset_ms / 1000, tz=timezone.utc)
                print("rate_limit_reset_utc:", reset_dt.isoformat())
    except Exception:
        print("raw_error:", body[:1000])

except Exception as error:
    print("RESULT: FAILED")
    print("error:", repr(error))

#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
FULL="false"

if [[ "${1:-}" == "--full" ]]; then
  FULL="true"
fi

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1"
    exit 1
  fi
}

require_cmd curl
require_cmd python3
require_cmd rg

echo "Checking health endpoint..."
curl -fsS "$BASE_URL/api/config/health" >/dev/null

echo "Checking identity/config endpoint..."
ME_JSON="$(curl -fsS "$BASE_URL/api/config/me")"
python3 -c 'import json,sys; obj=json.loads(sys.stdin.read()); print("user:", obj.get("user")); print("workspace_url:", obj.get("workspace_url")); print("lakebase_configured:", obj.get("lakebase_configured")); print("lakebase_error:", obj.get("lakebase_error"))' <<< "$ME_JSON"

if [[ "$FULL" != "true" ]]; then
  echo "Smoke test passed (API reachable)."
  echo "Run with --full for end-to-end agent invocation test."
  exit 0
fi

echo "Creating project..."
PROJECT_JSON="$(curl -fsS -X POST "$BASE_URL/api/projects" -H 'Content-Type: application/json' -d '{"name":"onboarding-smoke"}')"
PROJECT_ID="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["id"])' <<< "$PROJECT_JSON")"

echo "Invoking agent..."
INVOKE_JSON="$(curl -fsS -X POST "$BASE_URL/api/invoke_agent" -H 'Content-Type: application/json' -d "{\"project_id\":\"$PROJECT_ID\",\"message\":\"Read databricks.yml and summarize target names briefly.\"}")"
EXEC_ID="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["execution_id"])' <<< "$INVOKE_JSON")"

STREAM_OUT="$(mktemp)"

echo "Streaming execution events (max 120s)..."
curl -sN --max-time 120 -X POST "$BASE_URL/api/stream_progress/$EXEC_ID" \
  -H 'Content-Type: application/json' \
  -d '{}' > "$STREAM_OUT" || true

if rg -q '"type": "error"|"type":"error"' "$STREAM_OUT"; then
  echo "Agent returned an error event:"
  rg '"type": "error"|"type":"error"' "$STREAM_OUT" | head -n 5
  exit 1
fi

if rg -q 'stream.completed|\[DONE\]|"type": "text"|"type":"text"|"type": "text_delta"|"type":"text_delta"' "$STREAM_OUT"; then
  echo "Full smoke test passed (agent responded)."
else
  echo "No completion/response event observed. Check server logs."
  exit 1
fi

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KIT_DIR="$ROOT_DIR/ai-agent/databricks-solutions-ai-dev-kit"
APP_DIR="$KIT_DIR/databricks-builder-app"
CLIENT_DIR="$APP_DIR/client"
ENV_FILE="$APP_DIR/.env.local"

BACKEND_HOST="${BACKEND_HOST:-127.0.0.1}"
BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-3000}"
BACKEND_HEALTH_URL="http://${BACKEND_HOST}:${BACKEND_PORT}/api/config/health"
BACKEND_LOG_FILE="${BACKEND_LOG_FILE:-/tmp/ai_onboarding_backend.log}"
START_FRONTEND="true"

BACKEND_PID=""
FRONTEND_PID=""

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1"
    exit 1
  fi
}

is_port_in_use() {
  local port="$1"
  if lsof -ti:"$port" >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

print_port_processes() {
  local port="$1"
  echo "Processes on port $port:"
  lsof -nP -iTCP:"$port" -sTCP:LISTEN || true
}

cleanup() {
  set +e
  if [[ -n "$FRONTEND_PID" ]]; then
    kill "$FRONTEND_PID" 2>/dev/null || true
  fi
  if [[ -n "$BACKEND_PID" ]]; then
    kill "$BACKEND_PID" 2>/dev/null || true
  fi
}

wait_for_backend() {
  local attempts=30
  local i
  for ((i=1; i<=attempts; i++)); do
    if curl -fsS "$BACKEND_HEALTH_URL" >/dev/null 2>&1; then
      return 0
    fi
    if [[ -n "$BACKEND_PID" ]] && ! kill -0 "$BACKEND_PID" 2>/dev/null; then
      return 1
    fi
    sleep 1
  done
  return 1
}

require_cmd uv
require_cmd npm
require_cmd curl
require_cmd lsof

if [[ ! -d "$APP_DIR" ]]; then
  echo "Builder app not found: $APP_DIR"
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE"
  echo "Run: ./scripts/setup_ai_onboarding_chatbot.sh --profile DEFAULT"
  exit 1
fi

if is_port_in_use "$BACKEND_PORT"; then
  echo "Port $BACKEND_PORT is already in use."
  print_port_processes "$BACKEND_PORT"
  echo "Stop that process or choose another BACKEND_PORT."
  exit 1
fi

if is_port_in_use "$FRONTEND_PORT"; then
  echo "Port $FRONTEND_PORT is already in use. Frontend start will be skipped."
  print_port_processes "$FRONTEND_PORT"
  START_FRONTEND="false"
fi

trap cleanup EXIT INT TERM

echo "Starting backend on http://${BACKEND_HOST}:${BACKEND_PORT} ..."
(
  cd "$APP_DIR"
  uv run uvicorn server.app:app --host "$BACKEND_HOST" --port "$BACKEND_PORT" --reload --reload-dir server
) >"$BACKEND_LOG_FILE" 2>&1 &
BACKEND_PID=$!

if ! wait_for_backend; then
  echo "Backend failed to start. Last logs:"
  tail -n 80 "$BACKEND_LOG_FILE" || true
  exit 1
fi

echo "Backend is healthy: $BACKEND_HEALTH_URL"

if [[ "$START_FRONTEND" == "true" ]]; then
  echo "Starting frontend on http://localhost:${FRONTEND_PORT} ..."

  if [[ ! -d "$CLIENT_DIR/node_modules" ]]; then
    echo "Frontend dependencies not found. Installing..."
    (cd "$CLIENT_DIR" && npm install)
  fi

  (
    cd "$CLIENT_DIR"
    npm run dev -- --port "$FRONTEND_PORT"
  ) &
  FRONTEND_PID=$!
else
  echo "Using existing frontend on port ${FRONTEND_PORT}."
fi

echo ""
echo "AI onboarding chatbot is running:"
echo "  Frontend: http://localhost:${FRONTEND_PORT}"
echo "  Backend : http://${BACKEND_HOST}:${BACKEND_PORT}"
echo "  Backend log: $BACKEND_LOG_FILE"
echo ""
echo "Press Ctrl+C to stop."

if [[ "$START_FRONTEND" == "true" ]]; then
  wait "$FRONTEND_PID"
else
  wait "$BACKEND_PID"
fi

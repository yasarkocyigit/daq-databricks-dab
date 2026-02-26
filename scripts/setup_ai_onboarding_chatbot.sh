#!/usr/bin/env bash
set -euo pipefail

PROFILE="DEFAULT"
INSTALL_FRONTEND="true"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:-}"
      shift 2
      ;;
    --skip-frontend)
      INSTALL_FRONTEND="false"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--profile PROFILE] [--skip-frontend]"
      exit 1
      ;;
  esac
done

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KIT_DIR="$ROOT_DIR/ai-agent/databricks-solutions-ai-dev-kit"
APP_DIR="$KIT_DIR/databricks-builder-app"
ENV_FILE="$APP_DIR/.env.local"
DB_FILE="$APP_DIR/builder_app.db"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1"
    exit 1
  fi
}

require_cmd databricks
require_cmd uv
require_cmd python3

if [[ "$INSTALL_FRONTEND" == "true" ]]; then
  require_cmd node
  require_cmd npm
fi

if [[ ! -d "$APP_DIR" ]]; then
  echo "Builder app not found: $APP_DIR"
  exit 1
fi

AUTH_JSON="$(databricks auth env --profile "$PROFILE")"
HOST="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["env"].get("DATABRICKS_HOST",""))' <<< "$AUTH_JSON")"
TOKEN="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["env"].get("DATABRICKS_TOKEN",""))' <<< "$AUTH_JSON")"

if [[ -z "$HOST" || -z "$TOKEN" ]]; then
  echo "Could not read DATABRICKS_HOST/TOKEN from profile: $PROFILE"
  echo "Run: databricks auth login --profile $PROFILE"
  exit 1
fi

cat > "$ENV_FILE" <<EOT
ENV=development
DATABRICKS_HOST=$HOST
DATABRICKS_TOKEN=$TOKEN

# Local persistence for quick MVP tests
LAKEBASE_PG_URL=sqlite+aiosqlite:///$DB_FILE
LAKEBASE_INSTANCE_NAME=
LAKEBASE_DATABASE_NAME=

# Force agent project workspace to this repository
PROJECTS_BASE_DIR=./projects
PROJECT_WORKSPACE_ROOT=$ROOT_DIR

# Skills/tooling scope for onboarding and DAB operations
ENABLED_SKILLS=databricks-spark-declarative-pipelines,databricks-asset-bundles,databricks-jobs,databricks-unity-catalog,databricks-python-sdk
SKILLS_ONLY_MODE=false

# Databricks FMAPI model settings
ANTHROPIC_MODEL=databricks-claude-sonnet-4-5
ANTHROPIC_MODEL_MINI=databricks-claude-sonnet-4-5
CLAUDE_CODE_STREAM_CLOSE_TIMEOUT=3600000
MLFLOW_TRACKING_URI=databricks
EOT

echo "Wrote $ENV_FILE"

echo "Installing backend dependencies..."
(
  cd "$APP_DIR"
  uv sync --python 3.11
  uv pip install -e "$KIT_DIR/databricks-tools-core" -e "$KIT_DIR/databricks-mcp-server" aiosqlite
)

if [[ "$INSTALL_FRONTEND" == "true" ]]; then
  echo "Installing frontend dependencies..."
  (
    cd "$APP_DIR/client"
    npm install
  )
fi

echo "Setup complete."
echo "Next: ./scripts/start_ai_onboarding_chatbot.sh"

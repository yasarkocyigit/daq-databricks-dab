# AI Onboarding Chatbot (MVP)

This setup is for testing onboarding workflows through chat using the Databricks AI Dev Kit builder app.

## 1. Setup

```bash
./scripts/setup_ai_onboarding_chatbot.sh --profile DEFAULT
```

Notes:
- Databricks auth is read from the `DEFAULT` profile.
- SQLite is used for local persistence.
- Agent working directory is pinned to this repo root via `PROJECT_WORKSPACE_ROOT`.

## 2. Start

```bash
./scripts/start_ai_onboarding_chatbot.sh
```

Services:
- Backend: `http://localhost:8000`
- Frontend: `http://localhost:3000`

Notes:
- The start script boots backend first and checks `/api/config/health` before starting frontend.
- If port `3000` is already in use, it reuses the existing frontend and starts backend only.
- Default backend log path: `/tmp/ai_onboarding_backend.log`

## 3. Quick Test

```bash
./scripts/smoke_test_ai_onboarding_chatbot.sh
```

End-to-end agent test:

```bash
./scripts/smoke_test_ai_onboarding_chatbot.sh --full
```

## 4. MVP Scope

- Chatbot + MCP tools for Databricks operations
- Conversation-driven YAML onboarding management inside this repo
- Initial goal: stable running system + smoke tests

## 5. Next Steps

After MVP validation:
- Intent-level guardrails (`create_source` / `add_table` / `update_gold`)
- PR gate + CI validation workflow
- Staging/production approval gates

## 6. Troubleshooting

If you see `[vite] http proxy error ... ECONNREFUSED 127.0.0.1:8000`:
- Cause: frontend is running but backend is not.
- Fix: start with `./scripts/start_ai_onboarding_chatbot.sh`.
- Check backend logs: `tail -n 120 /tmp/ai_onboarding_backend.log`

To test backend only:

```bash
cd ai-agent/databricks-solutions-ai-dev-kit/databricks-builder-app
uv run uvicorn server.app:app --reload --port 8000 --reload-dir server
```

## 7. Pre-Deploy Review

Before push/deploy:
- Fill placeholders in `app.yaml`:
  - `LAKEBASE_INSTANCE_NAME=<your-lakebase-instance-name>`
- Verify local/runtime files are not tracked:
  - `.claude/`, `CLAUDE.md`, `.env.local`, `builder_app.db`, `.venv/`, `projects/`

Attach Lakebase resource (Databricks CLI v0.289+):

```bash
databricks apps update <app-name> --json '{
  "resources": [{
    "name": "lakebase",
    "database": {
      "instance_name": "<your-lakebase-instance-name>",
      "database_name": "databricks_postgres",
      "permission": "CAN_CONNECT_AND_CREATE"
    }
  }]
}'
```

Deploy:

```bash
cd ai-agent/databricks-solutions-ai-dev-kit/databricks-builder-app
./scripts/deploy.sh <app-name>
```

# AI Deployment Pre-Review Checklist

Use this checklist before pushing/deploying the onboarding AI app.

## 1) Placeholder Check

File: `ai-agent/databricks-solutions-ai-dev-kit/databricks-builder-app/app.yaml`

- `LAKEBASE_INSTANCE_NAME` must be set to a real value:
  - `value: "<your-lakebase-instance-name>"`
- Commands should use placeholder app name:
  - `<app-name>`

## 2) Attach Lakebase Resource (CLI v0.289+)

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

## 3) Secret/Runtime File Check

These files must not be tracked:
- `.claude/`
- `CLAUDE.md`
- `.env.local`
- `builder_app.db`
- `.venv/`
- `projects/`
- `client/node_modules/`
- `client/out/`

Quick checks:

```bash
git ls-files | rg '(\.env|builder_app\.db|\.claude|\.databricks)'
git grep -nE 'dapi[0-9a-zA-Z-]{10,}|client_secret|password='
```

## 4) Minimal Push Scope

Recommended scope:
- `.gitignore` (root)
- `scripts/`
- `docs/AI_ONBOARDING_CHATBOT.md`
- `docs/AI_DEPLOYMENT_REVIEW_CHECKLIST.md`
- `ai-agent/databricks-solutions-ai-dev-kit/databricks-builder-app/`
- `ai-agent/databricks-solutions-ai-dev-kit/databricks-tools-core/`
- `ai-agent/databricks-solutions-ai-dev-kit/databricks-mcp-server/`
- Only active skills under `ai-agent/databricks-solutions-ai-dev-kit/databricks-skills/`

Active skills (`app.yaml`):
- `databricks-spark-declarative-pipelines`
- `databricks-asset-bundles`
- `databricks-jobs`
- `databricks-unity-catalog`
- `databricks-python-sdk`

## 5) Deploy

```bash
cd ai-agent/databricks-solutions-ai-dev-kit/databricks-builder-app
./scripts/deploy.sh <app-name>
```

Note: `deploy.sh` now fails fast if `LAKEBASE_INSTANCE_NAME` is still a placeholder.

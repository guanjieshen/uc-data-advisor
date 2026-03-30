# UC Data Advisor — Deployment Guide

Deploy a multi-agent Unity Catalog data advisor for any Databricks workspace. The setup pipeline auto-creates all infrastructure, registers agents as MLflow models on Model Serving, and generates all content from your catalog metadata.

## Prerequisites

- **Databricks workspace** with Unity Catalog enabled
- **Databricks CLI** installed and authenticated (`databricks auth login --profile <name>`)
- **Python 3.12+** with `uv` installed
- **Source data catalogs** already populated in Unity Catalog
- Deployer must have **workspace admin** or equivalent permissions to create catalogs, endpoints, service principals, and apps

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/guanjieshen/uc-data-advisor.git
cd uc-data-advisor

# 2. Create your config
cp config/advisor_config.example.yaml config/my_config.yaml
```

Edit `config/my_config.yaml` — you only need to set 2 things:

```yaml
# 1. Source catalogs (REQUIRED)
source_catalogs:
  - my_catalog_operations
  - my_catalog_analytics

# 2. Workspace connection (REQUIRED)
workspace:
  host: "https://my-workspace.cloud.databricks.com"
  profile: my-profile    # or use token/host-only auth
```

```bash
# 3. Run the full setup pipeline
uv run python -m src.setup.run --config config/my_config.yaml

# Pipeline runs: provision → grant-uc → audit → generate → register →
#   deploy-agents → grant-agent-permissions → deploy
```

The app URL is printed at the end. Run benchmarks separately (see [Benchmarks](#benchmarks) section).

## What the Setup Pipeline Does

The pipeline runs 8 steps in sequence (verify and teardown are run separately):

### Step 1: Provision Infrastructure

Creates all Databricks resources (idempotent — safe to re-run):

| Step | Resource | How Created |
|------|----------|-------------|
| 1 | **SQL Warehouse** | Auto-discovers first available, or uses `warehouse_id` from config |
| 2 | **Advisor Catalog** | `CREATE CATALOG IF NOT EXISTS {app_name}_catalog` |
| 3 | **Vector Search Endpoint** | SDK `create_endpoint()`, polls until ONLINE |
| 4 | **Databricks App** | Created via CLI, captures auto-created service principal |
| 5 | **Lakebase Instance** | SDK `create_database_instance()`, creates database + adds SP roles |
| 6 | **LLM Endpoint** | Uses pay-per-token foundation model (e.g. `databricks-claude-opus-4-6`) |
| 7 | **Genie Space** | REST API, populated with source tables in deploy stage |
| 8 | **Agent SP** | Deployer-owned SP with OAuth secret stored in Databricks secret scope |

### Step 2: Grant UC Permissions

Grants the app SP and agent SP access to all required resources:
- `USE CATALOG` + `SELECT` on each source catalog
- `ALL PRIVILEGES` on advisor catalog
- `CAN_USE` on SQL warehouse
- `CAN_RUN` on Genie Space
- Lakebase instance role + database grants

### Step 3: Audit Metadata

Walks your source catalogs via the Databricks SDK:
- Collects catalog/schema/table/column names, types, comments, owners
- Computes metadata description coverage percentage
- Stores full audit in config

### Step 4: Generate Content

From the audit, auto-generates:
- **System prompts** — per-agent prompts with organization name and data domains
- **Knowledge base** — 10+ governance FAQs plus per-catalog and per-table dynamic FAQs
- **Benchmark questions** — 8 questions across discovery, metrics, QA, and general categories
- **UI suggestions** — landing page suggestions based on notable tables
- **Genie Space table list** — source tables + materialized metric tables

### Step 5: Register Agent Models

Registers each agent (discovery, metrics, qa) as an MLflow model in Unity Catalog using the model-from-code pattern. Models are versioned and deployed in parallel.

### Step 6: Deploy Agent Endpoints

Deploys each registered model to its own Model Serving endpoint via the Databricks Agent Bricks SDK (`agents.deploy()`). Endpoints:
- Scale to zero when idle
- Authenticate via OAuth secret scope references (`{{secrets/scope/key}}`)
- Receive environment variables for Genie Space, VS indexes, and LLM endpoint

### Step 7: Grant Agent Endpoint Permissions

Waits for all agent endpoints to be READY, then grants `CAN_QUERY` to the app SP so the Databricks App can call them.

### Step 8: Deploy Artifacts + App

1. Writes metadata and knowledge base to Delta tables
2. Creates Vector Search indexes (delta sync)
3. Updates Genie Space table list
4. Generates `app.yaml` and deploys the Databricks App

## Permissions

No manual permission configuration is needed. The pipeline auto-manages two service principals:

| SP | Created By | Purpose |
|----|-----------|---------|
| **App SP** | Databricks (auto-created with app) | App runtime — calls agent endpoints, Lakebase |
| **Agent SP** | Pipeline (deployer-owned) | Model Serving auth — outbound API calls from agents |

Both SPs receive identical grants. The agent SP also gets:
- `workspace-access` entitlement (required for API access from Model Serving)
- OAuth secret stored in a Databricks secret scope

## Running Individual Steps

```bash
uv run python -m src.setup.run --config config/my_config.yaml --step provision
uv run python -m src.setup.run --config config/my_config.yaml --step grant-uc
uv run python -m src.setup.run --config config/my_config.yaml --step audit
uv run python -m src.setup.run --config config/my_config.yaml --step generate
uv run python -m src.setup.run --config config/my_config.yaml --step register
uv run python -m src.setup.run --config config/my_config.yaml --step deploy-agents
uv run python -m src.setup.run --config config/my_config.yaml --step grant-agent-permissions
uv run python -m src.setup.run --config config/my_config.yaml --step deploy
uv run python -m src.setup.run --config config/my_config.yaml --step verify
uv run python -m src.setup.run --config config/my_config.yaml --step all  # default
```

## Config Reference

Required fields:

```yaml
source_catalogs:          # List of Unity Catalog catalogs to scan
  - my_catalog
workspace:
  host: "https://..."     # Workspace URL
  profile: my-profile     # CLI profile (or use token/host-only auth)
```

Optional overrides (all have smart defaults if omitted):

```yaml
app_name: my-project-advisor           # Default: derived from catalog prefix, MUST be unique per workspace
warehouse_id: "abc123def456"           # SQL warehouse ID (auto-discovered if omitted)
advisor_catalog: my_project_catalog    # Default: {app_name}_catalog
external_location: "my-ext-loc-name"   # UC external location for catalog storage (auto-detected)
serving_model: databricks-claude-opus-4-6  # Foundation model for LLM calls
embedding_model: databricks-bge-large-en   # VS embedding model
enable_metric_views: false             # Generate metric views and materialized tables
enable_ai_gateway_guardrails: false    # Enable input safety guardrails on agent endpoints
rate_limits:                           # AI Gateway rate limits on agent endpoints (optional)
  - calls: 120
    key: user                          # "user" (per-user) or "endpoint" (shared)
    renewal_period: minute             # "minute" or "day"
include_schemas: []                    # Restrict to specific schemas
exclude_schemas: [staging, temp]       # Skip these schemas
```

## Architecture

```
User → Databricks App (FastAPI)
         ↓
    Orchestrator (LLM intent classifier)
         ↓
    ┌────┼────┐
    ↓    ↓    ↓
 Discovery  Metrics  Q&A          ← Each is a Model Serving endpoint
    ↓         ↓       ↓
 UC API    Genie   Knowledge
 + VS      Space    Base VS
    ↓         ↓       ↓
    Unity Catalog (source data)
```

- **Model Serving**: Each agent runs on its own endpoint with scale-to-zero. Agents authenticate outbound calls via OAuth M2M (secret scope).
- **Genie Space**: The Metrics agent sends natural language questions to Genie, which translates to SQL and returns results.
- **Vector Search**: Discovery agent uses a metadata VS index for semantic table search. QA agent uses a knowledge base VS index for FAQ retrieval. Both are delta sync.
- **Lakebase**: Stores conversation history and user feedback.

## Teardown

To delete all resources created by the pipeline:

```bash
uv run python -m src.setup.run --config config/my_config.yaml --step teardown
```

This deletes (in order):
1. Agent deployments and serving endpoints
2. Vector Search indexes
3. Genie spaces
4. Secret scope
5. Agent service principal
6. Databricks App
7. Lakebase instance
8. Vector Search endpoint
9. Advisor catalog (CASCADE)

The config file retains the `infrastructure` section so you can inspect what was deleted. Reset the config to redeploy:

```yaml
infrastructure: {}
generated: {}
```

## Benchmarks

The benchmark suite runs 8 questions across all 4 agent types (discovery, metrics, QA, general) and validates routing accuracy and response quality.

### Option 1: Databricks notebook (recommended)

Upload `tests/benchmark_notebook.py` to your workspace and run it on any cluster. Calls agent serving endpoints directly via the Databricks SDK — no app URL or external auth needed.

1. Import the notebook: **Workspace > Import > File > `tests/benchmark_notebook.py`**
2. Set the `config_path` widget to your uploaded config file path
3. **Run All**

Results are displayed as an interactive DataFrame table.

### Option 2: Pipeline verify step (local)

```bash
uv run python -m src.setup.run --config config/my_config.yaml --step verify
```

Calls the deployed app's HTTP API. Authenticates using the pipeline's workspace client.

### Option 3: Standalone script (local)

```bash
APP_URL="https://your-app.aws.databricksapps.com" \
  uv run python tests/benchmark.py
```

Authenticates via `databricks auth token` CLI or SDK. Set `DATABRICKS_PROFILE` or `DATABRICKS_TOKEN` if needed.

### Benchmark criteria

| Status | Meaning |
|--------|---------|
| **PASS** | Correct routing, response has content, no errors |
| **WARN** | Correct routing but response contains error/unavailable messages, or routing mismatch |
| **FAIL** | HTTP error, timeout (300s), or empty response |

## Updating After Deployment

Re-run specific steps to update:

```bash
# After adding new source tables — re-audit, regenerate, and redeploy
uv run python -m src.setup.run --config config/my_config.yaml --step audit
uv run python -m src.setup.run --config config/my_config.yaml --step generate
uv run python -m src.setup.run --config config/my_config.yaml --step deploy

# To update agent code — re-register models and redeploy endpoints
uv run python -m src.setup.run --config config/my_config.yaml --step register
uv run python -m src.setup.run --config config/my_config.yaml --step deploy-agents
```

To sync Vector Search indexes manually:
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
w.vector_search_indexes.sync_index(index_name="catalog.schema.index_name")
```

## Troubleshooting

**Warehouse not found**: Either start a warehouse in the workspace UI, or pass `warehouse_id` in your config to use a specific one.

**VS endpoint stuck provisioning**: VS endpoints can take 5-10 minutes. Re-run `--step provision` — it detects the existing endpoint.

**Agent endpoints timeout**: First request after scale-to-zero takes up to 5 minutes (cold start). The benchmark uses a 300s timeout to accommodate this.

**Genie Space errors**: Ensure the agent SP has `CAN_RUN` on the Genie space and `CAN_USE` on the SQL warehouse. Re-run `--step grant-uc` to fix.

**Knowledge base unavailable**: VS indexes take a few minutes to sync after creation. Wait and retry, or manually sync via the Catalog UI.

**App deploy fails**: Ensure the Databricks CLI is authenticated with the correct profile. Check `databricks apps get <app-name>` for status.

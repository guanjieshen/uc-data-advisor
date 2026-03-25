# UC Data Advisor — Deployment Guide

Deploy a multi-agent Unity Catalog data advisor for any Databricks workspace. The setup pipeline auto-creates all infrastructure and generates all content from your catalog metadata.

## Prerequisites

- **Databricks workspace** with Unity Catalog enabled
- **Databricks CLI** installed and authenticated (`databricks auth login --profile <name>`)
- **Python 3.12+** with `uv` installed
- **psql** (PostgreSQL client) for Lakebase grants — `brew install postgresql` or equivalent
- **Source data catalogs** already populated in Unity Catalog with table/column descriptions
- Deployer must have **workspace admin** or equivalent permissions to create catalogs, endpoints, and apps

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/guanjieshen/uc-data-advisor.git
cd uc-data-advisor

# 2. Create your config
cp config/advisor_config.example.yaml config/my_config.yaml
```

Edit `config/my_config.yaml` — you only need to set 3 things:

```yaml
source_catalogs:
  - my_catalog_operations
  - my_catalog_analytics

workspace:
  host: "https://my-workspace.cloud.databricks.com"
  profile: my-profile

app_identity:
  type: service_principal
  name: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"  # SP client ID
```

```bash
# 3. Run the setup pipeline
uv run python -m src.setup.run --config config/my_config.yaml

# 4. Verify (set APP_URL to the URL printed at end of setup)
APP_URL="https://your-app-url.aws.databricksapps.com" uv run python tests/benchmark.py
```

## What the Setup Pipeline Does

The pipeline runs 4 steps in sequence:

### Step 1: Provision Infrastructure
Creates all Databricks resources (idempotent — safe to re-run):

| Resource | How Created |
|----------|-------------|
| **SQL Warehouse** | Discovers first available serverless warehouse |
| **Advisor Catalog** | `CREATE CATALOG IF NOT EXISTS uc_data_advisor` |
| **Vector Search Endpoint** | SDK `create_endpoint()`, polls until ONLINE |
| **Lakebase Instance** | SDK `create_database_instance()`, creates database, adds SP role |
| **Serving Endpoint** | External model with AI Gateway (rate limits, usage tracking, guardrails) |
| **Secret Scope** | Stores PAT for serving endpoint proxy auth |
| **Genie Space** | REST API, populated with tables in deploy step |
| **Databricks App** | Created if not exists |

### Step 2: Audit Metadata
Walks your source catalogs via the Databricks SDK:
- Collects catalog/schema/table/column names, types, comments, owners
- Computes metadata description coverage percentage
- Stores full audit in `config.generated.audit`

### Step 3: Generate Content
From the audit, auto-generates:

| Artifact | Source | Algorithm |
|----------|--------|-----------|
| **System prompts** | Catalog/schema comments | Template with `{org_name}` and `{data_domains}` |
| **Knowledge base** | 10 static governance FAQs + per-catalog + per-table dynamic FAQs | Comment parsing |
| **Metric views** | Column type analysis | Numeric → SUM/AVG measures, String → dimensions, Timestamp → time dimensions, FK → joins |
| **Benchmark questions** | Table names + metric view measures | Discovery/metrics/QA/general patterns |
| **UI suggestions** | Notable table names | Top tables with comments |
| **Genie Space tables** | Source tables + materialized metric tables | Sorted identifiers |

### Step 4: Deploy Artifacts
- Writes metadata docs to Delta table + creates VS index
- Writes knowledge base FAQ to Delta table + creates VS index
- Creates Databricks Metric Views via SQL
- Materializes metric tables for Genie Space
- Updates Genie Space table list
- Generates `app.yaml` from infrastructure config
- Uploads app files + deploys Databricks App

## App Identity

### Service Principal (recommended)

The setup script auto-grants all permissions — zero manual steps.

```yaml
app_identity:
  type: service_principal
  name: "03e2f707-ee86-4ed4-adea-28a6e792c82f"
```

Permissions auto-granted:
- `USE CATALOG` + `SELECT` on each source catalog
- `ALL PRIVILEGES` on advisor catalog
- `CAN_QUERY` on serving endpoint
- Lakebase instance role + database grants

### User

The setup script cannot grant UC permissions to users (requires metastore admin), so it prints a SQL script of required grants and saves it to `config/.generated/required_grants.sql`.

```yaml
app_identity:
  type: user
  name: "alice@company.com"
```

After running setup, apply the grants:
```bash
cat config/.generated/required_grants.sql
# Copy and run in a SQL editor as a metastore admin
```

## Running Individual Steps

```bash
uv run python -m src.setup.run --config config/my_config.yaml --step provision
uv run python -m src.setup.run --config config/my_config.yaml --step audit
uv run python -m src.setup.run --config config/my_config.yaml --step generate
uv run python -m src.setup.run --config config/my_config.yaml --step deploy
uv run python -m src.setup.run --config config/my_config.yaml --step all  # default
```

## Optional Config Overrides

All of these have smart defaults if omitted:

```yaml
app_name: my-project-advisor           # Default: derived from catalog prefix, MUST be unique per deployment
advisor_catalog: my_project_catalog    # Default: {app_name}_catalog
external_location: "my-ext-loc-name"   # UC external location name for catalog storage (auto-detected if omitted)
serving_model: databricks-claude-opus-4-6  # Upstream foundation model
embedding_model: databricks-bge-large-en   # VS embedding model
include_schemas: []                    # Restrict to specific schemas
exclude_schemas: [staging, temp]       # Skip these schemas
```

## Metadata Quality Tips

The quality of auto-generated content depends on your catalog metadata:

| Coverage | Impact |
|----------|--------|
| **Table comments** | Drives knowledge base FAQs, benchmark questions, UI suggestions |
| **Column comments** | Enriches VS search descriptions, metric view measure names |
| **Column types** | Determines measure vs. dimension classification |
| **FK comments** (`FK to {table}`) | Enables auto-detected joins in metric views |
| **Schema comments** | Populates data domain descriptions in system prompts |

Run a metadata audit to check your coverage before deploying:
```bash
uv run python -m src.setup.run --config config/my_config.yaml --step audit
# Check the "Description coverage" percentage in the output
```

## Architecture

```
User → Databricks App (FastAPI)
         ↓
    Orchestrator (LLM intent classifier)
         ↓
    ┌────┼────┐
    ↓    ↓    ↓
 Discovery  Metrics  Q&A
    ↓         ↓       ↓
 UC API    Genie   Knowledge
 + VS      Space    Base VS
    ↓         ↓       ↓
    Unity Catalog (source data)
```

All LLM calls go through a dedicated serving endpoint with AI Gateway (rate limiting, usage tracking, input safety guardrails).

## Troubleshooting

**Warehouse not starting**: The setup needs a running SQL warehouse. Start one manually in the workspace UI.

**VS endpoint stuck provisioning**: VS endpoints can take 5-10 minutes. Re-run `--step provision` and it will detect the existing endpoint.

**Lakebase connection refused**: Check that the Lakebase instance is `AVAILABLE` and that the app identity has been added as an instance role.

**Metric views fail to create**: Metric views require specific YAML syntax. Check the generated SQL in `config.generated.metric_views` for any invalid column references.

**Genie Space not accepting tables**: Genie only accepts base tables (not views). The setup generates materialized tables for Genie consumption.

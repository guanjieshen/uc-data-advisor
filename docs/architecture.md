# UC Data Advisor Architecture

## Overview

The UC Data Advisor is a multi-agent system for natural language dataset discovery over Unity Catalog. It deploys as a Databricks App with a React frontend and FastAPI backend. The orchestrator classifies user intent and routes to specialized agents running on individual Model Serving endpoints.

## Architecture Diagram

```mermaid
flowchart TB
    subgraph APP["DATABRICKS APP (FastAPI + React)"]
        FE["React Frontend<br/><i>Chat UI · Feedback · Suggestions</i>"]
        API["FastAPI Backend<br/><i>Chat · Feedback · Health APIs</i>"]
        ORCH["Orchestrator<br/><i>LLM Intent Classifier</i><br/><i>Routes to Sub-Agents</i>"]
        LB["Lakebase<br/><i>Session Memory · Feedback</i>"]
        MLF["MLflow Tracing<br/><i>Agent Observability</i>"]
        FE --> API --> ORCH
        ORCH -.-> LB
        ORCH -.-> MLF
    end

    subgraph SERVING["MODEL SERVING ENDPOINTS (Agent Bricks)"]
        DA["Discovery Agent<br/><i>Dataset Existence · Schema</i><br/><b>Tables · Columns · Descriptions</b>"]
        DM["Metrics Agent<br/><i>Compute Metrics on</i><br/><b>Unity Catalog Assets</b>"]
        QA["Q&A Agent<br/><i>RAG over Knowledge Base</i><br/><b>FAQ · Governance · Docs</b>"]
    end

    subgraph LLM["LLM"]
        CLAUDE["Foundation Model<br/><i>Claude Opus 4.6 (pay-per-token)</i>"]
    end

    subgraph TOOLS["TOOLS & RETRIEVAL"]
        UC_API["UC API Tools<br/><i>List · Describe · Search</i><br/><b>Scoped to source_catalogs</b>"]
        VS1["Vector Search Index<br/><i>UC Metadata</i><br/><b>Semantic Table Discovery</b>"]
        GENIE["Genie Space<br/><i>NL to SQL</i><br/><b>Metrics Queries</b>"]
        VS2["Vector Search Index<br/><i>Knowledge Base</i><br/><b>FAQ Retrieval</b>"]
    end

    subgraph DATA["DATA LAYER"]
        UCat[("Unity Catalog<br/><b>Source Catalogs</b><br/><i>Catalogs · Schemas · Tables · Columns</i>")]
    end

    subgraph AUTH["AUTH & SECRETS"]
        SP_APP["App SP<br/><i>Auto-created</i>"]
        SP_AGENT["Agent SP<br/><i>OAuth M2M</i>"]
        SCOPE["Secret Scope<br/><i>Client ID + Secret</i>"]
        SP_AGENT -.-> SCOPE
    end

    ORCH -->|discovery| DA
    ORCH -->|metrics| DM
    ORCH -->|Q&A| QA
    ORCH -->|classify / general| CLAUDE

    DA -->|LLM| CLAUDE
    DM -->|LLM| CLAUDE
    QA -->|LLM| CLAUDE

    DA -.-> UC_API
    DA -.-> VS1
    DM -.-> GENIE
    QA -.-> VS2

    GENIE --> UCat
    UC_API --> UCat

    SP_APP -->|CAN_QUERY| SERVING
    SP_AGENT -->|OAuth| TOOLS

    classDef app fill:#e8f4f8,stroke:#0077b6
    classDef serving fill:#fff3e0,stroke:#ff9800
    classDef llm fill:#fce4ec,stroke:#e91e63
    classDef tools fill:#f3e5f5,stroke:#9c27b0
    classDef data fill:#e8f5e9,stroke:#4caf50
    classDef auth fill:#f5f5f5,stroke:#757575

    class FE,API,ORCH,LB,MLF app
    class DA,DM,QA serving
    class CLAUDE llm
    class UC_API,VS1,GENIE,VS2 tools
    class UCat data
    class SP_APP,SP_AGENT,SCOPE auth
```

## Component Details

### Databricks App

| Component | Description |
|-----------|-------------|
| **React Frontend** | Chat interface with message history, thumbs up/down feedback, and landing page suggestions |
| **FastAPI Backend** | `/api/chat`, `/api/feedback`, `/api/health`, `/api/ui-config` endpoints |
| **Orchestrator** | Single LLM call to classify intent (discovery/metrics/qa/general), then routes to the matching agent endpoint via HTTP |
| **Lakebase** | PostgreSQL-compatible database for session history and user feedback |
| **MLflow Tracing** | Automatic trace logging for orchestrator classification and agent calls |

### Model Serving Endpoints

Each agent is registered as an MLflow model in Unity Catalog and deployed to its own Model Serving endpoint via the Agent Bricks SDK.

| Agent | MLflow Class | Tools | Data Source |
|-------|-------------|-------|-------------|
| **Discovery** | `DiscoveryAgent` | `list_catalogs`, `list_schemas`, `list_tables`, `get_table_details`, `search_tables`, `semantic_search_tables` | UC API + VS metadata index |
| **Metrics** | `MetricsAgent` | `query_genie` | Genie Space (NL-to-SQL) |
| **Q&A** | `QAAgent` | `search_knowledge_base` | VS knowledge base index |

Endpoint properties:
- **Scale to zero** when idle (cost-efficient)
- **OAuth M2M** authentication via secret scope references (`{{secrets/scope/key}}`)
- **Environment variables**: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `SERVING_ENDPOINT`, `GENIE_SPACE_ID`, `VS_INDEX_METADATA`, `VS_INDEX_KNOWLEDGE`, `SOURCE_CATALOGS`

### Authentication

| SP | Created By | Used For |
|----|-----------|----------|
| **App SP** | Databricks (auto-created with app) | App runtime — calls agent endpoints, Lakebase, LLM endpoint |
| **Agent SP** | Setup pipeline (deployer-owned) | Model Serving outbound calls — UC API, VS, Genie, LLM |

The agent SP:
- Gets `workspace-access` entitlement via SCIM
- Has an OAuth secret generated via `service_principal_secrets_proxy`
- Credentials stored in a Databricks secret scope
- Referenced by endpoints as `{{secrets/{scope}/sp-client-id}}` and `{{secrets/{scope}/sp-client-secret}}`

Both SPs receive identical UC, warehouse, Genie, Lakebase, and endpoint grants.

### Tools & Retrieval

| Tool | Used By | Implementation |
|------|---------|----------------|
| **UC API Tools** | Discovery | Databricks SDK — `catalogs.list()`, `schemas.list()`, `tables.list()`, `tables.get()`. Scoped to `SOURCE_CATALOGS` env var |
| **VS Metadata Index** | Discovery | Delta Sync Vector Search index over `uc_metadata_docs` table. Embedding model: `databricks-bge-large-en` |
| **Genie Space** | Metrics | REST API — starts conversation, polls for SQL results. Warehouse executes generated SQL |
| **VS Knowledge Index** | Q&A | Delta Sync Vector Search index over `knowledge_base` table. Auto-generated governance FAQs |
| **Lakebase** | Orchestrator | `asyncpg` connection to Lakebase PostgreSQL instance for session history and feedback |

## Data Flow

1. User sends a message via the React chat UI
2. FastAPI routes to the **Orchestrator**, which loads session history from Lakebase
3. Orchestrator makes a single LLM call to classify intent: `discovery`, `metrics`, `qa`, or `general`
4. For `general`: orchestrator responds directly via LLM (no agent call)
5. For agent intents: orchestrator calls the agent's Model Serving endpoint via `/serving-endpoints/{name}/invocations`
6. Agent uses its tools (UC API, Genie, VS) and LLM to produce a response
7. Response returned to user; exchange saved to Lakebase for session continuity

## Setup Pipeline

The setup pipeline (`src/setup/run.py`) automates all infrastructure creation and content generation:

```
provision → grant-uc → audit → generate → register → deploy-agents → grant-agent-permissions → deploy
```

| Step | What It Does |
|------|-------------|
| `provision` | Creates warehouse, catalog, VS endpoint, app, Lakebase, Genie space, agent SP + secrets |
| `grant-uc` | Grants UC, warehouse, and Genie permissions to both SPs |
| `audit` | Walks source catalogs to collect metadata |
| `generate` | Generates prompts, knowledge base, benchmarks, UI config |
| `register` | Registers 3 agent MLflow models in UC (parallel) |
| `deploy-agents` | Deploys 3 Model Serving endpoints via Agent Bricks (parallel) |
| `grant-agent-permissions` | Grants `CAN_QUERY` on agent endpoints to app SP |
| `deploy` | Writes Delta tables, VS indexes, Genie config, deploys app |
| `verify` | Runs 8 benchmark questions against the live deployment |
| `teardown` | Deletes all 9 resource types in order |

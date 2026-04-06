# UC Data Advisor Architecture

## Overview

The UC Data Advisor is a multi-agent system for natural language dataset discovery over Unity Catalog. All agents — including the orchestrator — run on individual Databricks Model Serving endpoints. No Databricks App is required. The orchestrator endpoint is the single entry point, callable from Teams, notebooks, or any HTTP client.

## Architecture Diagram

```mermaid
flowchart TB
    subgraph CLIENTS["CLIENTS"]
        TEAMS[Microsoft Teams Bot]
        NB[Databricks Notebook]
        HTTP[Any HTTP Client]
    end

    subgraph SERVING["MODEL SERVING ENDPOINTS"]
        ORCH[Orchestrator Agent]
        DA[Discovery Agent]
        DM[Metrics Agent]
        QA[Q&A Agent]
    end

    subgraph LLM["LLM"]
        CLAUDE[Foundation Model]
    end

    subgraph TOOLS["TOOLS & RETRIEVAL"]
        UC_API[UC API Tools]
        VS1[Vector Index - Metadata]
        GENIE[Genie Space]
        VS2[Vector Index - Knowledge]
    end

    subgraph DATA["DATA LAYER"]
        UCat[(Unity Catalog)]
    end

    subgraph AUTH["AUTH"]
        SP[Service Principal]
        SCOPE[Secret Scope]
        SP -.-> SCOPE
    end

    CLIENTS --> ORCH
    ORCH -->|discovery| DA
    ORCH -->|metrics| DM
    ORCH -->|Q&A| QA
    ORCH -->|classify| CLAUDE

    DA -->|LLM| CLAUDE
    DM -->|LLM| CLAUDE
    QA -->|LLM| CLAUDE

    DA -.-> UC_API
    DA -.-> VS1
    DM -.-> GENIE
    QA -.-> VS2

    GENIE --> UCat
    UC_API --> UCat

    SP -->|CAN_QUERY| SERVING
    SP -->|OAuth M2M| TOOLS

    classDef clients fill:#e8f4f8,stroke:#0077b6
    classDef serving fill:#fff3e0,stroke:#ff9800
    classDef llm fill:#fce4ec,stroke:#e91e63
    classDef tools fill:#f3e5f5,stroke:#9c27b0
    classDef data fill:#e8f5e9,stroke:#4caf50
    classDef auth fill:#f5f5f5,stroke:#757575

    class TEAMS,NB,HTTP clients
    class ORCH,DA,DM,QA serving
    class CLAUDE llm
    class UC_API,VS1,GENIE,VS2 tools
    class UCat data
    class SP,SCOPE auth
```

## Component Details

### Model Serving Endpoints

All agents are registered as MLflow models in Unity Catalog and deployed to individual Model Serving endpoints via the Agent Bricks SDK.

| Agent | MLflow Class | Tools | Data Source |
|-------|-------------|-------|-------------|
| **Orchestrator** | `OrchestratorAgent` | None (routes to sub-agents) | LLM for classification |
| **Discovery** | `DiscoveryAgent` | `search_metadata`, `semantic_search_tables` | VS metadata index (tables, volumes, tags, constraints, lineage, privileges) |
| **Metrics** | `MetricsAgent` | `query_genie` | Genie Space (NL-to-SQL) |
| **Q&A** | `QAAgent` | `search_knowledge_base` | VS knowledge base index |

Discovery uses the VS metadata index exclusively — no runtime SQL queries. The index is populated at setup time from `system.information_schema` and `system.access` system tables.

Endpoint properties:
- **Scale to zero** when idle
- **OAuth M2M** authentication via SP credentials injected as env vars at deploy time
- **Environment variables**: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `SERVING_ENDPOINT`, `GENIE_SPACE_ID`, `VS_INDEX_METADATA`, `VS_INDEX_KNOWLEDGE`, `SOURCE_CATALOGS`
- **Orchestrator** also gets: `DISCOVERY_AGENT_ENDPOINT`, `METRICS_AGENT_ENDPOINT`, `QA_AGENT_ENDPOINT`

### Authentication

A single user-provided service principal handles all auth:

| What | Permission |
|------|-----------|
| **UC catalogs** | `USE CATALOG` + `SELECT` on source, `ALL PRIVILEGES` on advisor |
| **System tables** | `USE SCHEMA` + `SELECT` on `system.information_schema` and `system.access` |
| **SQL warehouse** | `CAN_USE` |
| **Genie Space** | `CAN_RUN` |
| **Agent endpoints** | `CAN_QUERY` |
| **Model Serving outbound** | OAuth M2M via `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` |

The SP's OAuth secret is:
1. Generated via `service_principal_secrets_proxy.create()`
2. Stored in a Databricks secret scope
3. Read from the scope at deploy time by the pipeline
4. Injected as env vars into serving endpoints

### Tools & Retrieval

| Tool | Used By | Implementation |
|------|---------|----------------|
| **VS Metadata Index** | Discovery | Delta Sync VS index over `uc_metadata_docs` — contains tables, volumes, columns, tags, constraints, lineage, privileges. Populated from `system.information_schema` at setup time |
| **Genie Space** | Metrics | REST API — NL-to-SQL, starts conversation, polls for SQL results |
| **VS Knowledge Index** | Q&A | Delta Sync VS index over `knowledge_base` — governance FAQs |

No runtime SQL queries — all metadata discovery goes through Vector Search.

## Data Flow

1. Client sends a message to the **orchestrator endpoint** via `/serving-endpoints/{name}/invocations`
2. Orchestrator makes a single LLM call to classify intent: `discovery`, `metrics`, `qa`, or `general`
3. For `general`: orchestrator responds directly via LLM
4. For agent intents: orchestrator calls the sub-agent endpoint via HTTP
5. Discovery agent searches the VS metadata index; Metrics agent queries Genie; QA searches VS knowledge base
6. Response returned to client

## Setup Pipeline

The setup pipeline (`src/setup/run.py`) automates all infrastructure creation and content generation:

```
provision → grant-uc → audit → generate → register → deploy-agents → grant-agent-permissions → deploy
```

| Step | What It Does |
|------|-------------|
| `provision` | Creates catalog, VS endpoint, Genie space, SP OAuth secret in scope |
| `grant-uc` | Grants UC, system tables, warehouse, and Genie permissions to SP |
| `audit` | Queries system.information_schema for enriched metadata (tags, constraints, lineage, privileges, volumes) |
| `generate` | Generates prompts, knowledge base, benchmarks |
| `register` | Registers 4 agent MLflow models in UC (parallel) |
| `deploy-agents` | Deploys 4 Model Serving endpoints via Agent Bricks (sub-agents parallel, orchestrator sequential) |
| `grant-agent-permissions` | Grants `CAN_QUERY` on all endpoints to SP |
| `deploy` | Writes Delta tables, VS indexes, Genie config |
| `verify` | Runs 8 benchmark questions (run separately) |
| `teardown` | Deletes all 6 resource types |

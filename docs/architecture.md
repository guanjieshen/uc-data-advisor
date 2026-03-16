# UC Data Advisor Architecture

## Overview

The UC Data Advisor is a multi-agent system that enables natural language dataset discovery over Unity Catalog. It uses an orchestrator pattern to route queries to specialized sub-agents.

## Architecture Diagram

```mermaid
flowchart TB
    subgraph SERVING["SERVING & ORCHESTRATION"]
        MS[("Databricks Model Serving<br/><i>Hosts All Agents</i><br/><i>Endpoint for Teams & Web App</i>")]
        MLF["MLflow Tracing<br/><i>Agent Observability</i>"]

        MS --> ORCH

        subgraph ORCH_BOX[" "]
            ORCH["Orchestrator Agent<br/><i>LLM Intent Classifier</i><br/><i>Routes to Sub-Agents</i>"]
        end
    end

    subgraph AGENTS["AGENT LAYER"]
        DA["Data Discovery Agent<br/><i>Dataset Existence · Schema</i><br/><b>Tables · Columns · Descriptions</b><br/>Agent Bricks"]
        DM["Data Metrics Agent<br/><i>Compute Metrics on</i><br/><b>Unity Catalog Assets</b><br/>Agent Bricks"]
        QA["Q&A Agent<br/><i>RAG over TIS D&A</i><br/><b>FAQ & Processes</b><br/>Agent Bricks"]
    end

    subgraph LLM["LLM GATEWAY & MODEL"]
        GW["Databricks AI Gateway<br/><i>Rate Limiting · Monitoring · Governance</i>"]
        CLAUDE["Claude Opus 4.5<br/><i>Foundation Model</i>"]
        GW -->|inference| CLAUDE
    end

    subgraph TOOLS["TOOLS & RETRIEVAL"]
        UC_API["UC API Tools<br/><i>List · Describe · Search</i><br/><b>Catalogs · Schemas · Tables</b>"]
        VS1["Vector Index<br/><i>UC Metadata</i><br/><b>Semantic Discovery</b>"]
        GENIE["Genie Space<br/><i>NL → SQL Metrics</i><br/><b>Databricks Genie</b>"]
        LB["Lakebase<br/><i>Session Memory</i><br/><b>Conversation History</b>"]
        VS2["Vector Index<br/><i>TIS D&A Knowledge</i><br/><b>FAQ · Onboarding · Docs</b>"]
    end

    subgraph DATA["DATA LAYER"]
        UCat[("Unity Catalog<br/><b>Enterprise Data Marketplace</b><br/><i>Catalogs · Schemas · Tables · Columns · Descriptions</i>")]
    end

    %% Orchestrator to Agents
    ORCH -->|discovery| DA
    ORCH -->|metrics| DM
    ORCH -->|Q&A| QA

    %% Agents to LLM
    DA -->|LLM| GW
    DM -->|LLM| GW
    QA -->|LLM| GW

    %% Agent to Tools mappings
    DA -.-> UC_API
    DA -.-> VS1
    DM -.-> GENIE
    ORCH -.-> LB
    QA -.-> VS2

    %% Tools to Data
    GENIE --> UCat
    UC_API --> UCat

    %% Styling
    classDef serving fill:#e8f4f8,stroke:#0077b6
    classDef agent fill:#fff3e0,stroke:#ff9800
    classDef llm fill:#fce4ec,stroke:#e91e63
    classDef tools fill:#f3e5f5,stroke:#9c27b0
    classDef data fill:#e8f5e9,stroke:#4caf50
    classDef orchestrator fill:#e3f2fd,stroke:#2196f3

    class MS,MLF serving
    class DA,DM,QA agent
    class GW,CLAUDE llm
    class UC_API,VS1,GENIE,LB,VS2 tools
    class UCat data
    class ORCH orchestrator
```

## Component Details

### Serving & Orchestration

| Component | Description |
|-----------|-------------|
| **Databricks Model Serving** | Hosts all agent endpoints, provides interface for Teams and Web App |
| **MLflow Tracing** | Observability for agent execution, latency tracking, debugging |
| **Orchestrator Agent** | LLM-based intent classifier that routes queries to specialized sub-agents |

### Agent Layer

| Agent | Purpose | Capabilities |
|-------|---------|--------------|
| **Data Discovery Agent** | Find datasets by name, schema, description | Tables, Columns, Descriptions |
| **Data Metrics Agent** | Compute metrics on UC assets | SQL generation via Genie |
| **Q&A Agent** | Answer questions from documentation | RAG over FAQ & processes |

All agents are built using **Databricks Agent Bricks**.

### LLM Gateway & Model

| Component | Description |
|-----------|-------------|
| **Databricks AI Gateway** | Rate limiting, monitoring, governance |
| **Claude Opus 4.5** | Foundation model for inference |

### Tools & Retrieval

| Tool | Used By | Purpose |
|------|---------|---------|
| **UC API Tools** | Data Discovery | List, describe, search catalogs/schemas/tables |
| **Vector Index (UC)** | Data Discovery | Semantic search over UC metadata |
| **Genie Space** | Data Metrics | Natural language to SQL conversion |
| **Lakebase** | Orchestrator | Session memory, conversation history |
| **Vector Index (Docs)** | Q&A Agent | RAG over FAQ, onboarding, documentation |

### Data Layer

| Component | Description |
|-----------|-------------|
| **Unity Catalog** | Enterprise data marketplace with catalogs, schemas, tables, columns, descriptions |

## Data Flow

1. User query arrives at **Databricks Model Serving**
2. **Orchestrator Agent** classifies intent and routes to appropriate sub-agent
3. Sub-agent uses **LLM** (via AI Gateway) and **Tools** to process query
4. Results returned through the serving endpoint
5. All interactions traced via **MLflow Tracing**

## Legend

| Color | Category |
|-------|----------|
| Blue | Discovery |
| Orange | Metrics |
| Purple | Q&A / RAG |
| Teal | Gateway / Orchestration |
| Yellow | Foundation Model |
| Green | Genie |
| Dark Blue | Agent Bricks |
| Gray | Session / Observability |

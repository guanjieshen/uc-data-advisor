# UC Data Advisor

A multi-agent system that enables natural language dataset discovery over Unity Catalog.

## Architecture

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

## Components

| Layer | Component | Purpose |
|-------|-----------|---------|
| **Orchestration** | Orchestrator Agent | Routes queries to specialized sub-agents |
| **Agents** | Data Discovery | Find datasets by name, schema, description |
| **Agents** | Data Metrics | Compute metrics via Genie SQL generation |
| **Agents** | Q&A | RAG over documentation and FAQ |
| **LLM** | AI Gateway + Claude | Rate limiting, monitoring, inference |
| **Tools** | UC API, Vector Search, Genie, Lakebase | Metadata access, semantic search, session memory |

## Getting Started

See [docs/architecture.md](docs/architecture.md) for detailed component information.

## License

See LICENSE file for details.

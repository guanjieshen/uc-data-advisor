# UC Data Advisor

A multi-agent system that enables natural language dataset discovery over Unity Catalog. Deploys entirely on Databricks Model Serving — no app required.

## Architecture

```mermaid
flowchart TB
    subgraph SERVING["MODEL SERVING ENDPOINTS"]
        ORCH["Orchestrator Agent<br/><i>LLM Intent Classifier + Router</i>"]
        DA["Discovery Agent<br/><i>Dataset Search · Schema · Columns</i>"]
        DM["Metrics Agent<br/><i>NL-to-SQL via Genie</i>"]
        QA["Q&A Agent<br/><i>RAG over Knowledge Base</i>"]
    end

    subgraph LLM["LLM"]
        CLAUDE["Foundation Model<br/><i>Claude Opus 4.6</i>"]
    end

    subgraph TOOLS["TOOLS & RETRIEVAL"]
        UC_API["UC API Tools<br/><i>List · Describe · Search</i>"]
        VS1["Vector Index<br/><i>UC Metadata</i>"]
        GENIE["Genie Space<br/><i>NL to SQL</i>"]
        VS2["Vector Index<br/><i>Knowledge Base</i>"]
    end

    subgraph DATA["DATA LAYER"]
        UCat[("Unity Catalog<br/><i>Source Catalogs</i>")]
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

    classDef serving fill:#fff3e0,stroke:#ff9800
    classDef llm fill:#fce4ec,stroke:#e91e63
    classDef tools fill:#f3e5f5,stroke:#9c27b0
    classDef data fill:#e8f5e9,stroke:#4caf50

    class ORCH,DA,DM,QA serving
    class CLAUDE llm
    class UC_API,VS1,GENIE,VS2 tools
    class UCat data
```

## Components

| Layer | Component | Purpose |
|-------|-----------|---------|
| **Model Serving** | Orchestrator Agent | LLM intent classifier that routes to sub-agents |
| **Model Serving** | Discovery Agent | Find datasets by name, schema, description via UC API + Vector Search |
| **Model Serving** | Metrics Agent | Answer analytical questions via Genie Space (NL-to-SQL) |
| **Model Serving** | Q&A Agent | RAG over governance FAQs and knowledge base |
| **LLM** | Foundation Model | Pay-per-token model for all inference |
| **Tools** | UC API, Vector Search, Genie | Metadata access, semantic search, SQL generation |

## Key Design Decisions

- **All agents on Model Serving**: Each agent (including orchestrator) runs on its own endpoint with scale-to-zero — independent scaling, versioning, and no app dependency
- **Single entry point**: The orchestrator endpoint handles classification + routing — callable from Teams, notebooks, or any HTTP client
- **User-provided SP**: A single service principal configured in YAML receives all grants and authenticates Model Serving containers via OAuth M2M
- **Secret scope for credentials**: SP OAuth secrets stored in Databricks secret scope, read at deploy time and injected as env vars
- **Catalog scoping**: Agents only see catalogs listed in `source_catalogs` config via `SOURCE_CATALOGS` env var
- **Cross-cloud**: Works on both AWS and Azure Databricks workspaces

## Quick Start

```bash
git clone https://github.com/guanjieshen/uc-data-advisor.git
cd uc-data-advisor
cp config/advisor_config.example.yaml config/my_config.yaml
# Edit my_config.yaml with your catalogs, workspace, and service principal
uv run python -m src.setup.run --config config/my_config.yaml
```

See [DEPLOYMENT.md](DEPLOYMENT.md) for full deployment guide, config reference, benchmarks, and troubleshooting.

## Project Structure

```
app/
  server/
    agents/
      base.py                   # ResponsesBaseAgent with tool-calling loop
      orchestrator_agent.py     # Orchestrator (classify + route) for Model Serving
      discovery.py              # UC metadata discovery agent
      metrics.py                # Genie Space metrics agent
      qa.py                     # Knowledge base Q&A agent
    tools/                      # UC API, Genie, Vector Search tool implementations
    config.py                   # Auth chain (Model Serving OAuth M2M, CLI)
    advisor_config.py           # Runtime config loader
    uc_tools.py                 # UC metadata tools (scoped to source_catalogs)
src/
  setup/
    run.py                      # Pipeline orchestrator (8 steps + teardown)
    provision_infrastructure.py # Creates catalog, VS endpoint, Genie, SP secrets
    audit_metadata.py           # Walks UC catalogs for metadata
    generate_*.py               # Content generation (prompts, KB, benchmarks)
    register_models.py          # MLflow model registration (parallel)
    deploy_agent_endpoints.py   # Agent Bricks deployment (parallel)
    deploy.py                   # Delta tables, VS indexes, Genie config
    teardown.py                 # Full resource cleanup
config/
  advisor_config.example.yaml   # Template config
teams/                          # Microsoft Teams bot integration
tests/
  benchmark.py                  # CLI benchmark script
  benchmark_notebook.py         # Databricks notebook benchmark
```

"""Configuration for the Teams bot."""

import os


# Databricks connection
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID", "")
DATABRICKS_CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

# Orchestrator serving endpoint name
ORCHESTRATOR_ENDPOINT = os.environ.get(
    "ORCHESTRATOR_ENDPOINT", "acao-bakehouse-advisor-orchestrator-agent"
)

# Bot server
PORT = int(os.environ.get("PORT", 3978))

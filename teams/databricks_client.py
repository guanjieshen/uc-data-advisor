"""Databricks client for calling the orchestrator serving endpoint."""

import logging
from databricks.sdk import WorkspaceClient

from config import (
    DATABRICKS_HOST,
    DATABRICKS_CLIENT_ID,
    DATABRICKS_CLIENT_SECRET,
    DATABRICKS_TOKEN,
    ORCHESTRATOR_ENDPOINT,
)

logger = logging.getLogger(__name__)

_client: WorkspaceClient | None = None


def _get_client() -> WorkspaceClient:
    global _client
    if _client is None:
        kwargs = {}
        if DATABRICKS_HOST:
            kwargs["host"] = DATABRICKS_HOST
        if DATABRICKS_TOKEN:
            kwargs["token"] = DATABRICKS_TOKEN
        elif DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET:
            kwargs["client_id"] = DATABRICKS_CLIENT_ID
            kwargs["client_secret"] = DATABRICKS_CLIENT_SECRET
        _client = WorkspaceClient(**kwargs)
    return _client


def query_orchestrator(message: str, history: list[dict] | None = None) -> str:
    """Call the orchestrator serving endpoint and return the text response.

    Args:
        message: The user's message.
        history: Optional conversation history as list of {"role": ..., "content": ...} dicts.

    Returns:
        The orchestrator's text response.
    """
    w = _get_client()

    messages = []
    if history:
        messages.extend(history)
    messages.append({"role": "user", "content": message})

    payload = {"input": messages}

    try:
        resp = w.api_client.do(
            "POST",
            f"/serving-endpoints/{ORCHESTRATOR_ENDPOINT}/invocations",
            body=payload,
        )
    except Exception as e:
        logger.error(f"Orchestrator call failed: {e}")
        return "I'm sorry, I encountered an error. Please try again."

    # Extract text from ResponsesAgent output
    for item in resp.get("output", []):
        if item.get("type") == "message":
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    return content.get("text", "")

    return "I received your message but couldn't generate a response."

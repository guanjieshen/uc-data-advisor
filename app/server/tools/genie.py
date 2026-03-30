"""Genie Space client for natural language data queries."""

import os
import time
import logging
from ..config import get_workspace_client

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 2  # seconds
_MAX_POLL_TIME = 120  # seconds


def query_genie(question: str) -> dict:
    """Send a natural language question to a Genie Space and return results.

    Uses the Genie REST API: start conversation -> poll for completion -> extract SQL + results.
    """
    client = get_workspace_client()
    space_id = os.environ.get("GENIE_SPACE_ID", "")

    if not space_id:
        return {"error": "GENIE_SPACE_ID not configured"}

    api = client.api_client

    try:
        # Start a new conversation
        start_resp = api.do(
            "POST",
            f"/api/2.0/genie/spaces/{space_id}/start-conversation",
            body={"content": question},
        )
        conversation_id = start_resp.get("conversation_id", "")
        message_id = start_resp.get("message_id", "")

        if not conversation_id or not message_id:
            return {"error": "Failed to start Genie conversation", "detail": str(start_resp)}

        # Poll for completion
        elapsed = 0
        while elapsed < _MAX_POLL_TIME:
            time.sleep(_POLL_INTERVAL)
            elapsed += _POLL_INTERVAL

            msg_resp = api.do(
                "GET",
                f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
            )
            status = msg_resp.get("status", "")

            if status == "COMPLETED":
                return _extract_genie_result(msg_resp)
            elif status in ("FAILED", "CANCELLED"):
                return {
                    "error": f"Genie query {status.lower()}",
                    "detail": msg_resp.get("error", {}).get("message", ""),
                }

        return {"error": "Genie query timed out after 120 seconds"}

    except Exception as e:
        logger.error(f"Genie query failed: {e}")
        return {"error": f"Genie query failed: {str(e)}"}


def _extract_genie_result(msg_resp: dict) -> dict:
    """Extract SQL and results from a completed Genie message response."""
    attachments = msg_resp.get("attachments", [])
    result = {
        "answer": None,
        "sql": None,
        "data": None,
        "columns": None,
    }

    for attachment in attachments:
        # Text answer (new format)
        if "text" in attachment:
            result["answer"] = attachment["text"].get("content", "")

        # SQL query and results
        if "query" in attachment:
            query_info = attachment["query"]
            result["sql"] = query_info.get("query", "")
            # Results may be inline or require fetching via statement_id
            query_result = query_info.get("result", {})
            result["data"] = query_result.get("data_array", [])
            result["columns"] = [
                col.get("name", "")
                for col in query_result.get("manifest", {}).get("columns", [])
            ]

    # Fallback to top-level content if no text attachment
    if not result["answer"]:
        result["answer"] = msg_resp.get("content", "")

    return result

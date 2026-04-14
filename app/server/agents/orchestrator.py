"""Orchestrator — calls the deployed orchestrator serving endpoint."""

import os
import logging

from ..config import get_workspace_client

logger = logging.getLogger(__name__)


def _extract_text(output: list) -> str:
    """Extract text from ResponsesAgent output format."""
    for item in output:
        if isinstance(item, dict) and item.get("type") == "message":
            for content in item.get("content", []):
                if isinstance(content, dict) and content.get("type") == "output_text":
                    return content.get("text", "")
        elif hasattr(item, "type") and item.type == "message":
            for content in item.content:
                if isinstance(content, dict):
                    if content.get("type") == "output_text":
                        return content.get("text", "")
                elif getattr(content, "type", None) == "output_text":
                    return content.text
    return ""


class Orchestrator:
    """Calls the orchestrator serving endpoint for intent classification and routing."""

    def __init__(self):
        self._endpoint = os.environ.get("ORCHESTRATOR_ENDPOINT", "")
        if not self._endpoint:
            from ..advisor_config import get_config
            infra = get_config().get("infrastructure", {})
            self._endpoint = infra.get("agent_endpoints", {}).get("orchestrator", "")

        if not self._endpoint:
            raise RuntimeError(
                "No orchestrator endpoint configured. Set ORCHESTRATOR_ENDPOINT env var "
                "or ensure agent_endpoints.orchestrator is in the config."
            )

        logger.info(f"Using orchestrator endpoint: {self._endpoint}")

    async def route(self, messages: list[dict]) -> tuple[str, str]:
        """Call the orchestrator endpoint and return (response_text, agent_name).

        The orchestrator endpoint handles intent classification and agent routing internally.
        """
        import asyncio

        response_text = await asyncio.to_thread(self._call, messages)

        # Infer agent name from response content (the endpoint doesn't return it separately)
        agent_name = "orchestrator"
        return response_text, agent_name

    def _call(self, messages: list[dict]) -> str:
        """Synchronous call to the orchestrator endpoint."""
        client = get_workspace_client()

        payload = {
            "input": [{"role": m["role"], "content": m["content"]} for m in messages]
        }

        try:
            resp = client.api_client.do(
                "POST",
                f"/serving-endpoints/{self._endpoint}/invocations",
                body=payload,
            )
        except Exception as e:
            logger.error(f"Orchestrator endpoint call failed: {e}")
            raise

        return _extract_text(resp.get("output", []))

"""Orchestrator — classifies intent and routes to specialized agents via Model Serving."""

import os
import asyncio
import logging

import mlflow
from openai import BadRequestError

from .base import get_llm_client
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse

logger = logging.getLogger(__name__)


def _extract_text(response: ResponsesAgentResponse) -> str:
    """Extract text from ResponsesAgentResponse, working around mlflow content parsing."""
    texts = []
    for output in response.output:
        if getattr(output, "type", None) == "message":
            for content in output.content:
                if isinstance(content, dict):
                    if content.get("type") == "output_text":
                        texts.append(content.get("text", ""))
                elif getattr(content, "type", None) == "output_text":
                    texts.append(content.text)
    return "".join(texts)

from ..advisor_config import get_prompts

DEFAULT_CLASSIFY_PROMPT = """You are an intent classifier for the UC Data Advisor.

Classify the user's latest message into exactly one category:
- discovery: Questions about finding datasets, browsing catalogs/schemas/tables, understanding table structures, or checking what data exists.
- metrics: Questions asking for specific numbers, aggregations, counts, trends, or analytical queries about the data.
- qa: Questions about data governance, access policies, how to request data, FAQs about the data catalog, or general knowledge questions.
- general: Greetings, small talk, clarifications, or anything that doesn't fit the above categories.

Respond with ONLY the category name, nothing else."""


class RemoteAgentClient:
    """Calls an agent hosted on a Model Serving endpoint."""

    def __init__(self, endpoint_name: str, agent_name: str):
        self.endpoint_name = endpoint_name
        self.name = agent_name

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        """Call the remote agent endpoint via the serving invocations API."""
        from ..config import get_workspace_client
        client = get_workspace_client()

        # Convert input to plain dicts (request.input may contain MLflow Message objects)
        input_data = []
        for item in request.input:
            if isinstance(item, dict):
                input_data.append(item)
            elif hasattr(item, "to_dict"):
                input_data.append(item.to_dict())
            else:
                input_data.append({"role": getattr(item, "role", "user"), "content": getattr(item, "content", str(item))})

        payload = {"input": input_data}
        try:
            resp = client.api_client.do(
                "POST",
                f"/serving-endpoints/{self.endpoint_name}/invocations",
                body=payload,
            )
        except Exception as e:
            logger.error(f"Agent endpoint {self.endpoint_name} call failed: {e}")
            raise

        return ResponsesAgentResponse(output=resp.get("output", []))


class Orchestrator:
    """Lightweight intent classifier that routes to sub-agents via Model Serving."""

    def __init__(self):
        from ..advisor_config import get_config
        infra = get_config().get("infrastructure", {})
        agent_endpoints = infra.get("agent_endpoints", {})

        if not agent_endpoints:
            raise RuntimeError(
                "No agent_endpoints configured. Run the setup pipeline with "
                "'--step register' and '--step deploy-agents' first."
            )

        logger.info(f"Using Model Serving agents: {list(agent_endpoints.keys())}")
        self._agents = {
            name: RemoteAgentClient(ep_name, name)
            for name, ep_name in agent_endpoints.items()
        }

    @mlflow.trace(name="orchestrator.route")
    async def route(self, messages: list[dict]) -> tuple[str, str]:
        """Classify intent and route to the appropriate agent.

        Returns (response_text, agent_name).
        """
        intent = await self._classify(messages)
        logger.info(f"Intent classified as: {intent}")

        agent = self._agents.get(intent)
        if agent:
            responses_input = [
                {"role": m["role"], "content": m["content"]} for m in messages
            ]
            request = ResponsesAgentRequest(input=responses_input)
            response = await asyncio.to_thread(agent.predict, request)
            return _extract_text(response), agent.name

        response = await self._general_response(messages)
        return response, "general"

    @mlflow.trace(name="orchestrator.classify")
    async def _classify(self, messages: list[dict]) -> str:
        """Single LLM call to classify intent."""
        client = get_llm_client()
        model = os.environ.get("SERVING_ENDPOINT", "databricks-claude-opus-4-6")

        last_user_msg = ""
        for msg in reversed(messages):
            if msg.get("role") == "user":
                last_user_msg = msg.get("content", "")
                break

        if not last_user_msg:
            return "general"

        try:
            response = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": get_prompts().get("classify", DEFAULT_CLASSIFY_PROMPT)},
                    {"role": "user", "content": last_user_msg},
                ],
                max_tokens=16,
                temperature=0,
            )
        except BadRequestError as e:
            if "guardrail_triggered" in str(e):
                logger.warning("Guardrail triggered during classification, defaulting to discovery")
                return "discovery"
            raise

        intent = (response.choices[0].message.content or "").strip().lower()
        if intent not in ("discovery", "metrics", "qa", "general"):
            logger.warning(f"Unexpected intent '{intent}', defaulting to discovery")
            intent = "discovery"

        return intent

    async def _general_response(self, messages: list[dict]) -> str:
        """Handle general/greeting messages without tools."""
        client = get_llm_client()
        model = os.environ.get("SERVING_ENDPOINT", "databricks-claude-opus-4-6")

        default_general = (
            "You are the UC Data Advisor, a helpful assistant for discovering and understanding "
            "datasets in Unity Catalog. Respond warmly and briefly. If the user seems "
            "to need data help, let them know you can help find datasets, explore table structures, "
            "and answer questions about the data catalog."
        )
        system = get_prompts().get("general", default_general)

        response = await client.chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": system}] + messages,
            max_tokens=512,
            temperature=0.5,
        )
        return response.choices[0].message.content or ""

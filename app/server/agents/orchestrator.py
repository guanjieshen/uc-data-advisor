"""Orchestrator — classifies intent and routes to specialized agents."""

import os
import asyncio
import logging

import mlflow
from openai import BadRequestError

from .base import get_llm_client
from .discovery import DiscoveryAgent
from .metrics import MetricsAgent
from .qa import QAAgent

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

CLASSIFY_PROMPT = """You are an intent classifier for the UC Data Advisor at Enbridge.

Classify the user's latest message into exactly one category:
- discovery: Questions about finding datasets, browsing catalogs/schemas/tables, understanding table structures, or checking what data exists.
- metrics: Questions asking for specific numbers, aggregations, counts, trends, or analytical queries about the data (e.g., "how many safety incidents", "total throughput last month").
- qa: Questions about data governance, access policies, how to request data, FAQs about the data catalog, or general knowledge questions.
- general: Greetings, small talk, clarifications, or anything that doesn't fit the above categories.

Respond with ONLY the category name, nothing else."""


class Orchestrator:
    """Lightweight intent classifier that routes to sub-agents."""

    def __init__(self):
        self._agents = {
            "discovery": DiscoveryAgent(),
            "metrics": MetricsAgent(),
            "qa": QAAgent(),
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
                    {"role": "system", "content": CLASSIFY_PROMPT},
                    {"role": "user", "content": last_user_msg},
                ],
                max_tokens=16,
                temperature=0,
            )
        except BadRequestError as e:
            if "guardrail_triggered" in str(e):
                logger.warning(f"Guardrail triggered during classification, defaulting to discovery")
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

        system = (
            "You are the UC Data Advisor, a helpful assistant for discovering and understanding "
            "datasets in Unity Catalog at Enbridge. Respond warmly and briefly. If the user seems "
            "to need data help, let them know you can help find datasets, explore table structures, "
            "and answer questions about the data catalog."
        )

        response = await client.chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": system}] + messages,
            max_tokens=512,
            temperature=0.5,
        )
        return response.choices[0].message.content or ""

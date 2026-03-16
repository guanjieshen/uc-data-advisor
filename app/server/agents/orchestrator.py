"""Orchestrator — classifies intent and routes to specialized agents."""

import os
import time
import logging
from .base import get_llm_client, _try_start_span
from .discovery import DiscoveryAgent
from .metrics import MetricsAgent
from .qa import QAAgent

logger = logging.getLogger(__name__)

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

    async def route(self, messages: list[dict]) -> tuple[str, str]:
        """Classify intent and route to the appropriate agent.

        Returns (response_text, agent_name).
        """
        start = time.time()
        span_ctx, mlflow = _try_start_span("orchestrator.route")

        try:
            if span_ctx:
                span = span_ctx.__enter__()
                span.set_attribute("input.message_count", len(messages))

            intent = await self._classify(messages)
            logger.info(f"Intent classified as: {intent}")

            if span_ctx:
                span.set_attribute("intent", intent)

            agent = self._agents.get(intent)
            if agent:
                response = await agent.run(messages)
                if span_ctx:
                    span.set_attribute("agent", agent.name)
                    span.set_attribute("latency_ms", int((time.time() - start) * 1000))
                return response, agent.name

            response = await self._general_response(messages)
            if span_ctx:
                span.set_attribute("agent", "general")
                span.set_attribute("latency_ms", int((time.time() - start) * 1000))
            return response, "general"
        finally:
            if span_ctx:
                try:
                    span_ctx.__exit__(None, None, None)
                except Exception:
                    pass

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

        span_ctx, mlflow = _try_start_span("orchestrator.classify")
        try:
            if span_ctx:
                span = span_ctx.__enter__()
                span.set_attribute("user_message", last_user_msg[:200])

            response = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": CLASSIFY_PROMPT},
                    {"role": "user", "content": last_user_msg},
                ],
                max_tokens=16,
                temperature=0,
            )

            intent = (response.choices[0].message.content or "").strip().lower()
            if intent not in ("discovery", "metrics", "qa", "general"):
                logger.warning(f"Unexpected intent '{intent}', defaulting to discovery")
                intent = "discovery"

            if span_ctx:
                span.set_attribute("intent", intent)

            return intent
        finally:
            if span_ctx:
                try:
                    span_ctx.__exit__(None, None, None)
                except Exception:
                    pass

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

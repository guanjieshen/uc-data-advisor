"""Orchestrator as a ResponsesAgent for Model Serving deployment.

Classifies intent via LLM, routes to sub-agent endpoints, returns the response.
This enables calling the full orchestrator via a single serving endpoint
without going through the Databricks App.
"""

import os
import json
import logging
from uuid import uuid4

from openai import OpenAI, BadRequestError
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse

from ..config import get_workspace_client, get_workspace_host, get_oauth_token

logger = logging.getLogger(__name__)

DEFAULT_CLASSIFY_PROMPT = """You are an intent classifier for the UC Data Advisor.

Classify the user's latest message into exactly one category:
- discovery: Questions about finding datasets, browsing catalogs/schemas/tables, understanding table structures, or checking what data exists.
- metrics: Questions asking for specific numbers, aggregations, counts, trends, or analytical queries about the data.
- qa: Questions about data governance, access policies, how to request data, FAQs about the data catalog, or general knowledge questions.
- general: Greetings, small talk, clarifications, or anything that doesn't fit the above categories.

Respond with ONLY the category name, nothing else."""

DEFAULT_GENERAL_PROMPT = (
    "You are the UC Data Advisor, a helpful assistant for discovering and understanding "
    "datasets in Unity Catalog. Respond warmly and briefly. If the user seems "
    "to need data help, let them know you can help find datasets, explore table structures, "
    "and answer questions about the data catalog."
)


def _get_token() -> str:
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if token:
        return token
    return get_oauth_token()


def _get_llm_client() -> OpenAI:
    host = get_workspace_host()
    token = _get_token()
    return OpenAI(api_key=token, base_url=f"{host}/serving-endpoints")


class OrchestratorAgent(ResponsesAgent):
    """Orchestrator agent that classifies intent and routes to sub-agents."""

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        model = os.environ.get("SERVING_ENDPOINT", "databricks-claude-opus-4-6")
        client = _get_llm_client()

        # Read agent endpoint names from env vars
        agent_endpoints = {}
        for agent in ["discovery", "metrics", "qa"]:
            ep = os.environ.get(f"{agent.upper()}_AGENT_ENDPOINT", "")
            if ep:
                agent_endpoints[agent] = ep

        # Convert input to plain dicts
        messages = []
        for item in request.input:
            if isinstance(item, dict):
                messages.append(item)
            elif hasattr(item, "to_dict"):
                messages.append(item.to_dict())
            else:
                messages.append({"role": getattr(item, "role", "user"), "content": getattr(item, "content", str(item))})

        # Classify intent
        intent = self._classify(client, model, messages)
        logger.info(f"Intent classified as: {intent}")

        # Route
        if intent in agent_endpoints:
            response_text = self._call_agent(agent_endpoints[intent], messages)
        else:
            response_text = self._general_response(client, model, messages)

        output_items = [
            self.create_text_output_item(text=response_text, id=str(uuid4()))
        ]
        return ResponsesAgentResponse(output=output_items)

    def _classify(self, client: OpenAI, model: str, messages: list[dict]) -> str:
        last_user_msg = ""
        for msg in reversed(messages):
            if msg.get("role") == "user":
                last_user_msg = msg.get("content", "")
                break

        if not last_user_msg:
            return "general"

        classify_prompt = os.environ.get("CLASSIFY_PROMPT", DEFAULT_CLASSIFY_PROMPT)

        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": classify_prompt},
                    {"role": "user", "content": last_user_msg},
                ],
                max_tokens=16,
                temperature=0,
            )
        except BadRequestError as e:
            if "guardrail_triggered" in str(e):
                return "discovery"
            raise

        intent = (response.choices[0].message.content or "").strip().lower()
        if intent not in ("discovery", "metrics", "qa", "general"):
            intent = "discovery"
        return intent

    def _call_agent(self, endpoint_name: str, messages: list[dict]) -> str:
        w = get_workspace_client()
        payload = {"input": [{"role": m["role"], "content": m["content"]} for m in messages]}
        try:
            resp = w.api_client.do(
                "POST",
                f"/serving-endpoints/{endpoint_name}/invocations",
                body=payload,
            )
        except Exception as e:
            logger.error(f"Agent endpoint {endpoint_name} call failed: {e}")
            return f"I encountered an error routing to the {endpoint_name} agent. Please try again."

        # Extract text from ResponsesAgent output
        for item in resp.get("output", []):
            if item.get("type") == "message":
                for content in item.get("content", []):
                    if content.get("type") == "output_text":
                        return content.get("text", "")
        return json.dumps(resp.get("output", []))

    def _general_response(self, client: OpenAI, model: str, messages: list[dict]) -> str:
        general_prompt = os.environ.get("GENERAL_PROMPT", DEFAULT_GENERAL_PROMPT)
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "system", "content": general_prompt}] + messages,
                max_tokens=512,
                temperature=0.5,
            )
            return response.choices[0].message.content or ""
        except BadRequestError as e:
            if "guardrail_triggered" in str(e):
                return "Hello! I'm the UC Data Advisor. I can help you find datasets, explore table structures, and answer questions about the data catalog."
            raise

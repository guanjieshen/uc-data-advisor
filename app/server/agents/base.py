"""Base agent with reusable tool-calling loop."""

import os
import json
import time
import logging
from openai import AsyncOpenAI
from ..config import get_oauth_token, get_workspace_host, IS_DATABRICKS_APP

logger = logging.getLogger(__name__)


def get_llm_client() -> AsyncOpenAI:
    host = get_workspace_host()
    if IS_DATABRICKS_APP:
        token = os.environ.get("DATABRICKS_TOKEN") or get_oauth_token()
    else:
        token = get_oauth_token()
    return AsyncOpenAI(api_key=token, base_url=f"{host}/serving-endpoints")


def _message_to_dict(message) -> dict:
    """Convert an OpenAI message object to a dict for Databricks compatibility."""
    d = {"role": "assistant", "content": message.content or ""}
    if message.tool_calls:
        tool_calls = []
        for tc in message.tool_calls:
            if isinstance(tc, dict):
                tool_calls.append(tc)
            else:
                tool_calls.append({
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                })
        d["tool_calls"] = tool_calls
    return d


def _try_start_span(name: str):
    """Start an MLflow span if tracing is initialized. Returns (span, mlflow) or (None, None)."""
    try:
        from ..tracing import _initialized
        if _initialized:
            import mlflow
            return mlflow.start_span(name=name), mlflow
    except Exception:
        pass
    return None, None


class BaseAgent:
    """Agent with a system prompt, tools, and tool executor."""

    name: str = "base"
    system_prompt: str = ""
    tools: list[dict] = []

    async def execute_tool(self, name: str, args: dict) -> dict | list:
        raise NotImplementedError

    async def run(self, messages: list[dict]) -> str:
        """Run a tool-calling conversation loop with MLflow tracing."""
        client = get_llm_client()
        model = os.environ.get("SERVING_ENDPOINT", "databricks-claude-opus-4-6")
        start = time.time()
        tool_calls_made = []

        full_messages = [{"role": "system", "content": self.system_prompt}] + messages

        kwargs = {"model": model, "messages": full_messages, "max_tokens": 4096, "temperature": 0.3}
        if self.tools:
            kwargs["tools"] = self.tools

        span_ctx, mlflow = _try_start_span(f"agent.{self.name}")
        try:
            if span_ctx:
                span = span_ctx.__enter__()
                span.set_attribute("agent.name", self.name)
                span.set_attribute("input.message_count", len(messages))

            for iteration in range(5):
                response = await client.chat.completions.create(**kwargs)
                choice = response.choices[0]

                if not choice.message.tool_calls:
                    result = choice.message.content or ""
                    if span_ctx:
                        span.set_attribute("output.length", len(result))
                        span.set_attribute("tool_calls", json.dumps(tool_calls_made))
                        span.set_attribute("iterations", iteration + 1)
                        span.set_attribute("latency_ms", int((time.time() - start) * 1000))
                        if hasattr(response, "usage") and response.usage:
                            span.set_attribute("tokens.prompt", response.usage.prompt_tokens or 0)
                            span.set_attribute("tokens.completion", response.usage.completion_tokens or 0)
                    return result

                full_messages.append(_message_to_dict(choice.message))

                for tc in choice.message.tool_calls:
                    if isinstance(tc, dict):
                        tc_id = tc.get("id", "")
                        func_name = tc.get("function", {}).get("name", "")
                        func_args = tc.get("function", {}).get("arguments", "{}")
                    else:
                        tc_id = tc.id
                        func_name = tc.function.name
                        func_args = tc.function.arguments

                    args = json.loads(func_args) if isinstance(func_args, str) else func_args
                    tool_calls_made.append({"name": func_name, "args": args})

                    result = await self.execute_tool(func_name, args)

                    full_messages.append({
                        "role": "tool",
                        "tool_call_id": tc_id,
                        "content": json.dumps(result, default=str),
                    })

                kwargs["messages"] = full_messages

            return "I'm still working on this but hit the maximum number of tool calls. Could you try a more specific question?"
        finally:
            if span_ctx:
                try:
                    span_ctx.__exit__(None, None, None)
                except Exception:
                    pass

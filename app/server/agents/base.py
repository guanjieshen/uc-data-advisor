"""Base agent built on mlflow ResponsesAgent with reusable tool-calling loop."""

import os
import json
import logging
from uuid import uuid4

from openai import OpenAI, AsyncOpenAI, BadRequestError
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse

from ..config import get_oauth_token, get_workspace_host, IS_DATABRICKS_APP

logger = logging.getLogger(__name__)


def get_llm_client() -> AsyncOpenAI:
    """Async client for orchestrator classify/general_response calls."""
    host = get_workspace_host()
    if IS_DATABRICKS_APP:
        token = os.environ.get("DATABRICKS_TOKEN") or get_oauth_token()
    else:
        token = get_oauth_token()
    return AsyncOpenAI(api_key=token, base_url=f"{host}/serving-endpoints")


def get_sync_llm_client() -> OpenAI:
    """Sync client for ResponsesAgent.predict() calls."""
    host = get_workspace_host()
    if IS_DATABRICKS_APP:
        token = os.environ.get("DATABRICKS_TOKEN") or get_oauth_token()
    else:
        token = get_oauth_token()
    return OpenAI(api_key=token, base_url=f"{host}/serving-endpoints")


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


class ResponsesBaseAgent(ResponsesAgent):
    """Agent with a system prompt, tools, and tool executor using ResponsesAgent framework."""

    name: str = "base"
    system_prompt: str = ""
    tools: list[dict] = []

    def execute_tool(self, name: str, args: dict) -> dict | list:
        raise NotImplementedError

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        client = get_sync_llm_client()
        model = os.environ.get("SERVING_ENDPOINT", "databricks-claude-opus-4-6")

        cc_messages = self.prep_msgs_for_cc_llm(request.input)
        full_messages = [{"role": "system", "content": self.system_prompt}] + cc_messages

        output_items = []
        for _ in range(5):
            kwargs = {
                "model": model,
                "messages": full_messages,
                "max_tokens": 4096,
                "temperature": 0.3,
            }
            if self.tools:
                kwargs["tools"] = self.tools

            try:
                response = client.chat.completions.create(**kwargs)
            except BadRequestError as e:
                error_str = str(e)
                if "input_guardrail_triggered" in error_str:
                    logger.warning(f"Input guardrail triggered for {self.name}, retrying with sanitized context")
                    # Strip tool results from context and retry with a rephrased system hint
                    sanitized = [m for m in full_messages if m["role"] != "tool"]
                    sanitized.append({
                        "role": "user",
                        "content": "The previous message was flagged by the safety filter. Please answer the question about database tables and columns using only the tool call results you already have. Focus on the technical metadata only.",
                    })
                    try:
                        retry_kwargs = {**kwargs, "messages": sanitized}
                        retry_kwargs.pop("tools", None)
                        response = client.chat.completions.create(**retry_kwargs)
                        text = response.choices[0].message.content or ""
                    except Exception:
                        text = "The safety filter blocked this request. Try asking about the table using its full name (e.g., catalog.schema.table)."
                    output_items.append(
                        self.create_text_output_item(text=text, id=str(uuid4()))
                    )
                    return ResponsesAgentResponse(output=output_items)
                if "output_guardrail_triggered" in error_str:
                    logger.warning(f"Output guardrail triggered for {self.name}, retrying without tool results")
                    summary = "I found relevant results but the response was filtered. Please provide a summary based on the tool calls made."
                    full_messages.append({"role": "user", "content": summary})
                    try:
                        retry_kwargs = {**kwargs, "messages": full_messages}
                        retry_kwargs.pop("tools", None)
                        response = client.chat.completions.create(**retry_kwargs)
                        text = response.choices[0].message.content or ""
                    except Exception:
                        text = "I found some results but the endpoint's safety filter blocked the detailed response. Try asking about a specific catalog or table name instead."
                    output_items.append(
                        self.create_text_output_item(text=text, id=str(uuid4()))
                    )
                    return ResponsesAgentResponse(output=output_items)
                raise

            choice = response.choices[0]

            if not choice.message.tool_calls:
                text = choice.message.content or ""
                output_items.append(
                    self.create_text_output_item(text=text, id=str(uuid4()))
                )
                return ResponsesAgentResponse(output=output_items)

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
                result = self.execute_tool(func_name, args)
                result_str = json.dumps(result, default=str)

                # Emit function_call + function_call_output for observability
                output_items.append(
                    self.create_function_call_item(
                        id=str(uuid4()),
                        call_id=tc_id,
                        name=func_name,
                        arguments=func_args if isinstance(func_args, str) else json.dumps(func_args),
                    )
                )
                output_items.append(
                    self.create_function_call_output_item(
                        call_id=tc_id,
                        output=result_str,
                    )
                )

                full_messages.append({
                    "role": "tool",
                    "tool_call_id": tc_id,
                    "content": result_str,
                })

        # Fallback after max iterations
        output_items.append(
            self.create_text_output_item(
                text="I'm still working on this but hit the maximum number of tool calls. Could you try a more specific question?",
                id=str(uuid4()),
            )
        )
        return ResponsesAgentResponse(output=output_items)

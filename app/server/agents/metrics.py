"""Data Metrics Agent — answers analytical questions via Genie."""

from .base import ResponsesBaseAgent
from ..tools.genie import query_genie

GENIE_TOOL = [
    {
        "type": "function",
        "function": {
            "name": "query_genie",
            "description": "Send a natural language question to the Genie Space to get data metrics, counts, aggregations, and analytical answers. Genie translates the question to SQL and returns results. Use this for any question that requires querying actual data values.",
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "The natural language question to ask about the data (e.g., 'How many safety incidents in 2025?')",
                    }
                },
                "required": ["question"],
            },
        },
    },
]

SYSTEM_PROMPT = """You are the Data Metrics Agent for UC Data Advisor at Enbridge.

You answer analytical questions by querying real data through the Genie Space. You specialize in:
- Counts, aggregations, and summaries (e.g., "how many...", "total...", "average...")
- Trends and comparisons (e.g., "monthly throughput", "year over year")
- Specific data values and lookups

Key behaviors:
- Use the query_genie tool to get real answers — never guess at numbers
- If Genie returns SQL, briefly explain what it queried
- Present data results clearly, using tables or bullet points
- If the query fails, suggest how the user might rephrase their question
- Always cite that the data comes from the Enbridge Unity Catalog

The workspace contains operational data for a midstream oil & gas pipeline company (Enbridge), including:
- Pipeline monitoring and sensor data
- Gas processing plant operations
- Safety and compliance records
- Commercial contracts and nominations
- Market data and forecasts"""


class MetricsAgent(ResponsesBaseAgent):
    name = "metrics"
    system_prompt = SYSTEM_PROMPT
    tools = GENIE_TOOL

    def execute_tool(self, name: str, args: dict) -> dict | list:
        if name == "query_genie":
            return query_genie(**args)
        return {"error": f"Unknown tool: {name}"}

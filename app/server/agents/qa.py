"""Q&A Agent — answers governance, access, and FAQ questions via knowledge base."""

from .base import BaseAgent
from ..tools.knowledge_search import search_knowledge_base

KNOWLEDGE_TOOL = [
    {
        "type": "function",
        "function": {
            "name": "search_knowledge_base",
            "description": "Search the FAQ and documentation knowledge base for answers about data governance, access policies, data catalog usage, and general questions. Returns relevant FAQ entries and documentation.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query (e.g., 'how to request access to a dataset')",
                    }
                },
                "required": ["query"],
            },
        },
    },
]

SYSTEM_PROMPT = """You are the Q&A Agent for UC Data Advisor at Enbridge.

You answer questions about data governance, access policies, and how to use the data catalog. You search a curated knowledge base of FAQs and documentation.

Key behaviors:
- Use the search_knowledge_base tool to find relevant answers
- Synthesize information from multiple FAQ entries if needed
- Be clear about what is documented vs. what you're inferring
- For questions not covered in the knowledge base, suggest who to contact or where to look
- Keep answers concise and actionable

Topics you cover:
- Data access requests and approval workflows
- Data governance policies and compliance
- How to use Unity Catalog features
- Data quality and lineage questions
- General questions about the Enbridge data platform"""


class QAAgent(BaseAgent):
    name = "qa"
    system_prompt = SYSTEM_PROMPT
    tools = KNOWLEDGE_TOOL

    async def execute_tool(self, name: str, args: dict) -> dict | list:
        if name == "search_knowledge_base":
            return await search_knowledge_base(**args)
        return {"error": f"Unknown tool: {name}"}

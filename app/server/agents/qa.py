"""Q&A Agent — answers governance, access, and FAQ questions via knowledge base."""

from .base import ResponsesBaseAgent
from ..tools.knowledge_search import search_knowledge_base
from ..advisor_config import get_prompts

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

DEFAULT_QA_PROMPT = """You are the Q&A Agent for UC Data Advisor.

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
- General questions about the data platform"""


class QAAgent(ResponsesBaseAgent):
    name = "qa"
    tools = KNOWLEDGE_TOOL

    @property
    def system_prompt(self):
        return get_prompts().get("qa", DEFAULT_QA_PROMPT)

    def execute_tool(self, name: str, args: dict) -> dict | list:
        if name == "search_knowledge_base":
            return search_knowledge_base(**args)
        return {"error": f"Unknown tool: {name}"}

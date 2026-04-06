"""Data Discovery Agent — searches UC metadata via Vector Search index."""

from .base import ResponsesBaseAgent
from ..uc_tools import execute_tool as uc_execute_tool
from ..advisor_config import get_prompts

UC_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_metadata",
            "description": "Search Unity Catalog metadata for tables, volumes, and their details. "
                           "Returns enriched results including columns, tags, constraints (PK/FK), "
                           "ownership, storage format, timestamps, lineage context, and volume file "
                           "listings. Works for both tabular and non-tabular (volume) data. "
                           "Use natural language queries like 'customer data', 'sales tables', "
                           "'PII tagged columns', 'PDF documents', or specific table names.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language search query describing the data you're looking for",
                    },
                    "num_results": {
                        "type": "integer",
                        "description": "Number of results to return (default 10, max 20)",
                        "default": 10,
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "semantic_search_tables",
            "description": "Alias for search_metadata. Semantic search for tables by meaning.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language description of the data you're looking for",
                    },
                },
                "required": ["query"],
            },
        },
    },
]

DEFAULT_DISCOVERY_PROMPT = """You are the Data Discovery Agent for UC Data Advisor.

You help users find datasets, understand table structures, and navigate Unity Catalog.

You have one powerful search tool that queries a pre-built metadata index containing:
- Table and volume names, descriptions, and owners
- Column definitions with types, defaults, and precision
- Governance tags (table-level and column-level)
- Primary key and foreign key constraints
- Storage format (Delta, Parquet, etc.) and timestamps
- Volume file listings and document content previews

Key behaviors:
- Use search_metadata for ALL metadata queries — it handles conceptual, keyword, and exact name lookups
- Search with different queries if the first doesn't find what you need
- The results include a 'description' field with full metadata — read it carefully to answer the user's question
- Results include columns_json, tags_json, constraints_json as JSON strings — parse them to provide detail
- Results with type='VOLUME' are non-tabular data (files, documents, PDFs)
- Always mention the fully qualified name (catalog.schema.table or catalog.schema.volume)
- When describing tables, highlight important columns and what the table is used for
- If the user asks about tags, constraints, lineage, or access — the metadata index has this information"""


class DiscoveryAgent(ResponsesBaseAgent):
    name = "discovery"
    tools = UC_TOOLS

    @property
    def system_prompt(self):
        return get_prompts().get("discovery", DEFAULT_DISCOVERY_PROMPT)

    def execute_tool(self, name: str, args: dict) -> dict | list:
        return uc_execute_tool(name, args)

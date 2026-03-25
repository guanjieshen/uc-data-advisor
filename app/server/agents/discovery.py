"""Data Discovery Agent — browses Unity Catalog metadata."""

from .base import ResponsesBaseAgent
from ..uc_tools import execute_tool as uc_execute_tool
from ..tools.vector_search import semantic_search_tables
from ..advisor_config import get_prompts

UC_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "list_catalogs",
            "description": "List all Unity Catalog catalogs accessible in the workspace. Returns catalog names and descriptions. Use this to understand what data domains are available.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_schemas",
            "description": "List all schemas within a specific catalog. Returns schema names and descriptions. Use this to explore the data organization within a catalog.",
            "parameters": {
                "type": "object",
                "properties": {
                    "catalog_name": {
                        "type": "string",
                        "description": "Name of the catalog to list schemas from",
                    }
                },
                "required": ["catalog_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_tables",
            "description": "List all tables within a specific schema. Returns table names, types, and descriptions. Use this to find specific datasets.",
            "parameters": {
                "type": "object",
                "properties": {
                    "catalog_name": {
                        "type": "string",
                        "description": "Name of the catalog",
                    },
                    "schema_name": {
                        "type": "string",
                        "description": "Name of the schema to list tables from",
                    },
                },
                "required": ["catalog_name", "schema_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_details",
            "description": "Get detailed metadata for a specific table including all columns, their types, and descriptions. Use this when a user asks about a specific dataset's structure.",
            "parameters": {
                "type": "object",
                "properties": {
                    "full_name": {
                        "type": "string",
                        "description": "Fully qualified table name: catalog.schema.table",
                    }
                },
                "required": ["full_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_tables",
            "description": "Search for tables across all catalogs by exact name or keyword match. Returns matching tables with their full paths and descriptions. Best for searching by exact table names or specific keywords.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search keyword to match against table names, schema names, and descriptions",
                    }
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "semantic_search_tables",
            "description": "Search for tables using semantic similarity. Best for conceptual queries like 'tables about environmental emissions' or 'pipeline safety data'. Returns tables whose descriptions are most relevant to the query meaning.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language description of the data you're looking for",
                    }
                },
                "required": ["query"],
            },
        },
    },
]

DEFAULT_DISCOVERY_PROMPT = """You are the Data Discovery Agent for UC Data Advisor.

You help users find datasets, understand table structures, and navigate the Unity Catalog. You have access to tools that let you browse UC metadata.

Key behaviors:
- For conceptual queries (e.g., "data about emissions"), prefer semantic_search_tables for better results
- For exact name lookups (e.g., "nominations table"), use search_tables or get_table_details
- When users ask about available data, start by listing catalogs or searching for relevant tables
- When users ask about a specific dataset, get the full table details including column descriptions
- Provide clear, concise answers about what data is available and how it's organized
- If you're not sure which catalog or schema to look in, search across all of them
- Always mention the fully qualified table name (catalog.schema.table) so users can reference it
- When describing tables, highlight the most important columns and what the table is used for"""


class DiscoveryAgent(ResponsesBaseAgent):
    name = "discovery"
    tools = UC_TOOLS

    @property
    def system_prompt(self):
        return get_prompts().get("discovery", DEFAULT_DISCOVERY_PROMPT)

    def execute_tool(self, name: str, args: dict) -> dict | list:
        if name == "semantic_search_tables":
            return semantic_search_tables(**args)
        return uc_execute_tool(name, args)

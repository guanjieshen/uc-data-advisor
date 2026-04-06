"""Data Discovery Agent — browses Unity Catalog metadata via system tables."""

from .base import ResponsesBaseAgent
from ..uc_tools import execute_tool as uc_execute_tool
from ..tools.vector_search import semantic_search_tables
from ..advisor_config import get_prompts

FULL_NAME_PARAM = {
    "type": "object",
    "properties": {
        "full_name": {
            "type": "string",
            "description": "Fully qualified table name: catalog.schema.table",
        }
    },
    "required": ["full_name"],
}

UC_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "list_catalogs",
            "description": "List all Unity Catalog catalogs. Returns names, owners, descriptions, and creation dates.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_schemas",
            "description": "List schemas in a catalog with owners and descriptions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "catalog_name": {"type": "string", "description": "Catalog name"}
                },
                "required": ["catalog_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_tables",
            "description": "List tables in a schema with types, owners, formats, and timestamps.",
            "parameters": {
                "type": "object",
                "properties": {
                    "catalog_name": {"type": "string", "description": "Catalog name"},
                    "schema_name": {"type": "string", "description": "Schema name"},
                },
                "required": ["catalog_name", "schema_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_details",
            "description": "Get full table metadata: columns (types, defaults, precision), tags, constraints (PK/FK), owner, format, timestamps.",
            "parameters": FULL_NAME_PARAM,
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_tables",
            "description": "Search tables by keyword in name, schema, or description. Returns up to 10 matches.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search keyword"}
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "semantic_search_tables",
            "description": "Semantic search for tables by meaning. Best for conceptual queries like 'sales data' or 'customer reviews'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Natural language description"}
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_tags",
            "description": "Get governance tags on a table (e.g., PII, sensitivity, domain).",
            "parameters": FULL_NAME_PARAM,
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_column_tags",
            "description": "Get governance tags on columns (e.g., PII classification per column).",
            "parameters": FULL_NAME_PARAM,
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_lineage",
            "description": "Get data lineage: which tables feed into this table (upstream) and which tables consume it (downstream).",
            "parameters": FULL_NAME_PARAM,
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_constraints",
            "description": "Get primary key and foreign key constraints defined on a table.",
            "parameters": FULL_NAME_PARAM,
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_table_privileges",
            "description": "Show who has access to a table and what privileges they have (SELECT, MODIFY, etc.).",
            "parameters": FULL_NAME_PARAM,
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_volumes",
            "description": "List Unity Catalog volumes (file storage) in a schema. Volumes contain non-tabular data like PDFs, CSVs, images, and other documents.",
            "parameters": {
                "type": "object",
                "properties": {
                    "catalog_name": {"type": "string", "description": "Catalog name"},
                    "schema_name": {"type": "string", "description": "Schema name"},
                },
                "required": ["catalog_name", "schema_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_volume_details",
            "description": "Get volume metadata and list files inside it. Shows file names, sizes, and types. Use catalog.schema.volume format.",
            "parameters": FULL_NAME_PARAM,
        },
    },
]

DEFAULT_DISCOVERY_PROMPT = """You are the Data Discovery Agent for UC Data Advisor.

You help users find datasets, understand table structures, and navigate Unity Catalog. You query system tables for rich metadata including non-tabular data in volumes.

Key behaviors:
- For conceptual queries (e.g., "data about sales"), prefer semantic_search_tables
- For exact name lookups, use search_tables or get_table_details
- get_table_details returns columns, tags, constraints (PK/FK), owner, format, and timestamps
- Use get_table_lineage to show what feeds into or consumes a table
- Use get_table_tags / get_column_tags for governance metadata
- Use get_table_constraints for PK/FK relationships
- Use get_table_privileges to show who has access
- Use list_volumes and get_volume_details to find non-tabular data (PDFs, CSVs, documents, images)
- Always mention the fully qualified name (catalog.schema.table or catalog.schema.volume)
- When describing tables, highlight important columns and what the table is used for"""


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

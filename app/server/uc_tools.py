"""Unity Catalog metadata tools via Vector Search index.

All metadata is populated at setup time from system tables into a VS index.
Agents query the index at runtime — no SQL warehouse needed.
"""

import os
import logging
from .tools.vector_search import semantic_search_tables

logger = logging.getLogger(__name__)


def execute_tool(name: str, args: dict) -> dict | list:
    """Execute a UC metadata tool by name."""
    handlers = {
        "search_metadata": _search_metadata,
        "semantic_search_tables": _semantic_search,
    }
    handler = handlers.get(name)
    if not handler:
        return {"error": f"Unknown tool: {name}"}
    return handler(**args)


def _search_metadata(query: str, num_results: int = 10) -> list[dict]:
    """Search the UC metadata index for tables, volumes, and their details.

    The index contains enriched metadata: columns, tags, constraints, lineage,
    storage format, ownership, and volume file listings — all from system tables.
    """
    return semantic_search_tables(query=query, num_results=num_results)


def _semantic_search(query: str) -> list[dict]:
    """Semantic search over UC metadata. Alias for search_metadata."""
    return semantic_search_tables(query=query)

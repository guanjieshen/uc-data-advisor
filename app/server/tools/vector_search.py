"""Vector Search tool for semantic table discovery."""

import os
from databricks.sdk import WorkspaceClient
from ..config import get_workspace_client


_client: WorkspaceClient | None = None


def _get_client() -> WorkspaceClient:
    global _client
    if _client is None:
        _client = get_workspace_client()
    return _client


async def semantic_search_tables(query: str) -> list[dict]:
    """Search UC metadata using Vector Search for semantic similarity.

    Returns tables whose descriptions are semantically similar to the query.
    """
    client = _get_client()
    index_name = os.environ.get(
        "VS_INDEX_METADATA",
        "enbridge_operations.uc_advisor.uc_metadata_vs_index",
    )
    endpoint_name = os.environ.get("VS_ENDPOINT_NAME", "uc-advisor-vs")

    try:
        response = client.vector_search_indexes.query_index(
            index_name=index_name,
            columns=["full_table_name", "catalog_name", "schema_name", "table_name", "table_comment", "description_text"],
            query_text=query,
            num_results=10,
        )

        results = []
        if response.result and response.result.data_array:
            columns = [c.name for c in response.result.manifest.columns]
            for row in response.result.data_array:
                entry = dict(zip(columns, row))
                results.append({
                    "full_name": entry.get("full_table_name", ""),
                    "catalog": entry.get("catalog_name", ""),
                    "schema": entry.get("schema_name", ""),
                    "name": entry.get("table_name", ""),
                    "comment": entry.get("table_comment", ""),
                    "description": entry.get("description_text", ""),
                })
        return results
    except Exception as e:
        return [{"error": f"Vector search failed: {str(e)}"}]

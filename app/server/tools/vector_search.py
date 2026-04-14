"""Vector Search tool for semantic metadata discovery."""

import os
from databricks.sdk import WorkspaceClient
from ..config import get_workspace_client


_client: WorkspaceClient | None = None


def _get_client() -> WorkspaceClient:
    global _client
    if _client is None:
        _client = get_workspace_client()
    return _client


def semantic_search_tables(query: str, num_results: int = 10) -> list[dict]:
    """Search UC metadata using Vector Search for semantic similarity.

    The index contains enriched metadata from system tables: table/volume names,
    columns, tags, constraints, ownership, format, timestamps, and volume file
    contents (if indexing enabled). Returns the most relevant results.
    """
    client = _get_client()
    index_name = os.environ.get(
        "VS_INDEX_METADATA",
        "uc_data_advisor.default.uc_metadata_vs_index",
    )
    try:
        response = client.vector_search_indexes.query_index(
            index_name=index_name,
            columns=[
                "full_table_name", "catalog_name", "schema_name", "table_name",
                "table_comment", "table_type", "table_owner", "data_source_format",
                "tags_json", "constraints_json", "columns_json", "description_text",
            ],
            query_text=query,
            num_results=num_results,
        )

        results = []
        if response.result and response.result.data_array:
            manifest = getattr(response, "manifest", None) or getattr(response.result, "manifest", None)
            columns = [c.name for c in manifest.columns]
            for row in response.result.data_array:
                entry = dict(zip(columns, row))
                results.append({
                    "full_name": entry.get("full_table_name", ""),
                    "catalog": entry.get("catalog_name", ""),
                    "schema": entry.get("schema_name", ""),
                    "name": entry.get("table_name", ""),
                    "type": entry.get("table_type", ""),
                    "comment": entry.get("table_comment", ""),
                    "owner": entry.get("table_owner", ""),
                    "format": entry.get("data_source_format", ""),
                    "tags": entry.get("tags_json", "[]"),
                    "constraints": entry.get("constraints_json", "[]"),
                    "columns": entry.get("columns_json", "[]"),
                    "description": entry.get("description_text", ""),
                })
        return results
    except Exception as e:
        return [{"error": f"Vector search failed: {str(e)}"}]

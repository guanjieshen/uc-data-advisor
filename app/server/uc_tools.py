"""Unity Catalog metadata tool implementations.

All operations are scoped to the source_catalogs defined in the advisor config
or the SOURCE_CATALOGS env var (used on Model Serving).
"""

import os
from databricks.sdk import WorkspaceClient
from .config import get_workspace_client
from .advisor_config import get_config

_client: WorkspaceClient | None = None


def _get_client() -> WorkspaceClient:
    global _client
    if _client is None:
        _client = get_workspace_client()
    return _client


def _allowed_catalogs() -> set[str]:
    """Get the set of source catalogs this deployment is scoped to."""
    # Model Serving: SOURCE_CATALOGS env var (comma-separated)
    env_catalogs = os.environ.get("SOURCE_CATALOGS", "")
    if env_catalogs:
        return {c.strip() for c in env_catalogs.split(",") if c.strip()}
    # App runtime: from config file
    return set(get_config().get("source_catalogs", []))


def execute_tool(name: str, args: dict) -> dict | list:
    """Execute a UC metadata tool by name."""
    handlers = {
        "list_catalogs": _list_catalogs,
        "list_schemas": _list_schemas,
        "list_tables": _list_tables,
        "get_table_details": _get_table_details,
        "search_tables": _search_tables,
    }
    handler = handlers.get(name)
    if not handler:
        return {"error": f"Unknown tool: {name}"}
    return handler(**args)


def _list_catalogs() -> list[dict]:
    client = _get_client()
    allowed = _allowed_catalogs()
    results = []
    for cat in client.catalogs.list():
        if not cat.name or cat.name.startswith("__"):
            continue
        if allowed and cat.name not in allowed:
            continue
        results.append({
            "name": cat.name,
            "comment": cat.comment or "",
            "owner": cat.owner or "",
        })
    return results


def _list_schemas(catalog_name: str) -> list[dict]:
    allowed = _allowed_catalogs()
    if allowed and catalog_name not in allowed:
        return [{"error": f"Catalog '{catalog_name}' is not in the configured source catalogs."}]
    client = _get_client()
    results = []
    for schema in client.schemas.list(catalog_name=catalog_name):
        if schema.name not in ("information_schema", "default"):
            results.append({
                "name": schema.name,
                "full_name": f"{catalog_name}.{schema.name}",
                "comment": schema.comment or "",
            })
    return results


def _list_tables(catalog_name: str, schema_name: str) -> list[dict]:
    allowed = _allowed_catalogs()
    if allowed and catalog_name not in allowed:
        return [{"error": f"Catalog '{catalog_name}' is not in the configured source catalogs."}]
    client = _get_client()
    results = []
    for table in client.tables.list(
        catalog_name=catalog_name, schema_name=schema_name
    ):
        results.append({
            "name": table.name,
            "full_name": table.full_name,
            "table_type": str(table.table_type) if table.table_type else None,
            "comment": table.comment or "",
        })
    return results


def _get_table_details(full_name: str) -> dict:
    allowed = _allowed_catalogs()
    catalog_name = full_name.split(".")[0] if "." in full_name else ""
    if allowed and catalog_name not in allowed:
        return {"error": f"Catalog '{catalog_name}' is not in the configured source catalogs."}
    client = _get_client()
    try:
        table = client.tables.get(full_name=full_name)
        return {
            "name": table.name,
            "full_name": table.full_name,
            "catalog_name": table.catalog_name,
            "schema_name": table.schema_name,
            "table_type": str(table.table_type) if table.table_type else None,
            "owner": table.owner,
            "comment": table.comment or "",
            "created_at": str(table.created_at) if table.created_at else None,
            "updated_at": str(table.updated_at) if table.updated_at else None,
            "columns": [
                {
                    "name": col.name,
                    "type": col.type_text,
                    "comment": col.comment or "",
                    "nullable": col.nullable,
                }
                for col in (table.columns or [])
            ],
        }
    except Exception as e:
        return {"error": f"Table not found: {full_name}", "detail": str(e)}


def _search_tables(query: str) -> list[dict]:
    """Search for tables matching a keyword across configured source catalogs."""
    client = _get_client()
    allowed = _allowed_catalogs()
    query_lower = query.lower()
    matches = []

    for cat in client.catalogs.list():
        if not cat.name or cat.name.startswith("__") or cat.name in ("system", "samples"):
            continue
        if allowed and cat.name not in allowed:
            continue
        try:
            for schema in client.schemas.list(catalog_name=cat.name):
                if schema.name in ("information_schema", "default"):
                    continue
                try:
                    for table in client.tables.list(
                        catalog_name=cat.name, schema_name=schema.name
                    ):
                        name_match = query_lower in (table.name or "").lower()
                        schema_match = query_lower in (schema.name or "").lower()
                        comment_match = query_lower in (table.comment or "").lower()
                        if name_match or schema_match or comment_match:
                            matches.append({
                                "name": table.name,
                                "full_name": table.full_name,
                                "schema": schema.name,
                                "catalog": cat.name,
                                "comment": table.comment or "",
                            })
                            if len(matches) >= 10:
                                return matches
                except Exception:
                    continue
        except Exception:
            continue

    return matches

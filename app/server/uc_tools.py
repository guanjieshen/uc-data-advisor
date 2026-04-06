"""Unity Catalog metadata tools via system tables.

All operations query system.information_schema via SQL warehouse,
scoped to SOURCE_CATALOGS. Runs as the configured SP on Model Serving.
"""

import os
import time
import logging
from .config import get_workspace_client

logger = logging.getLogger(__name__)


def _allowed_catalogs() -> set[str]:
    """Get the set of source catalogs this deployment is scoped to."""
    env_catalogs = os.environ.get("SOURCE_CATALOGS", "")
    if env_catalogs:
        return {c.strip() for c in env_catalogs.split(",") if c.strip()}
    from .advisor_config import get_config
    return set(get_config().get("source_catalogs", []))


def _warehouse_id() -> str:
    """Get the SQL warehouse ID for queries."""
    wh = os.environ.get("WAREHOUSE_ID", "")
    if not wh:
        from .advisor_config import get_config
        wh = get_config().get("infrastructure", {}).get("warehouse_id", "")
    return wh


def _query(sql: str) -> list[dict]:
    """Execute SQL via statement execution API and return rows as dicts."""
    client = get_workspace_client()
    wh = _warehouse_id()
    if not wh:
        return [{"error": "WAREHOUSE_ID not configured"}]

    resp = client.statement_execution.execute_statement(
        warehouse_id=wh, statement=sql, wait_timeout="50s",
    )
    status = resp.status.state.value if resp.status else "unknown"
    while status in ("PENDING", "RUNNING"):
        time.sleep(2)
        resp = client.statement_execution.get_statement(resp.statement_id)
        status = resp.status.state.value if resp.status else "unknown"

    if status != "SUCCEEDED":
        error = resp.status.error.message if resp.status and resp.status.error else "unknown"
        return [{"error": f"Query failed: {error}"}]

    if not resp.result or not resp.result.data_array:
        return []

    columns = [c.name.lower() for c in resp.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in resp.result.data_array]


def _query_safe(sql: str) -> list[dict]:
    """Execute SQL, return empty list on failure."""
    try:
        result = _query(sql)
        if result and isinstance(result[0], dict) and "error" in result[0]:
            return []
        return result
    except Exception:
        return []


def _catalog_filter() -> str:
    """SQL IN clause for allowed catalogs."""
    allowed = _allowed_catalogs()
    if not allowed:
        return "''"
    return ", ".join(f"'{c}'" for c in allowed)


def _check_catalog(catalog_name: str) -> str | None:
    """Return error message if catalog not allowed, else None."""
    allowed = _allowed_catalogs()
    if allowed and catalog_name not in allowed:
        return f"Catalog '{catalog_name}' is not in the configured source catalogs."
    return None


# ---------------------------------------------------------------------------
# Core tools (same signatures as before)
# ---------------------------------------------------------------------------

def execute_tool(name: str, args: dict) -> dict | list:
    """Execute a UC metadata tool by name."""
    handlers = {
        "list_catalogs": _list_catalogs,
        "list_schemas": _list_schemas,
        "list_tables": _list_tables,
        "get_table_details": _get_table_details,
        "search_tables": _search_tables,
        "get_table_tags": _get_table_tags,
        "get_column_tags": _get_column_tags,
        "get_table_lineage": _get_table_lineage,
        "get_table_constraints": _get_table_constraints,
        "get_table_privileges": _get_table_privileges,
        "list_volumes": _list_volumes,
        "get_volume_details": _get_volume_details,
    }
    handler = handlers.get(name)
    if not handler:
        return {"error": f"Unknown tool: {name}"}
    return handler(**args)


def _list_catalogs() -> list[dict]:
    return _query(f"""
        SELECT catalog_name, catalog_owner, comment, created, last_altered
        FROM system.information_schema.catalogs
        WHERE catalog_name IN ({_catalog_filter()})
        ORDER BY catalog_name
    """)


def _list_schemas(catalog_name: str) -> list[dict]:
    err = _check_catalog(catalog_name)
    if err:
        return [{"error": err}]
    return _query(f"""
        SELECT schema_name, schema_owner, comment
        FROM system.information_schema.schemata
        WHERE catalog_name = '{catalog_name}'
          AND schema_name NOT IN ('information_schema', 'pg_catalog', '__db_system', 'default')
        ORDER BY schema_name
    """)


def _list_tables(catalog_name: str, schema_name: str) -> list[dict]:
    err = _check_catalog(catalog_name)
    if err:
        return [{"error": err}]
    return _query(f"""
        SELECT table_name, table_type, table_owner, comment, data_source_format,
               created, last_altered
        FROM system.information_schema.tables
        WHERE table_catalog = '{catalog_name}' AND table_schema = '{schema_name}'
        ORDER BY table_name
    """)


def _get_table_details(full_name: str) -> dict:
    parts = full_name.split(".")
    if len(parts) != 3:
        return {"error": f"Expected catalog.schema.table format, got: {full_name}"}
    catalog, schema, table = parts

    err = _check_catalog(catalog)
    if err:
        return {"error": err}

    # Table metadata
    table_rows = _query(f"""
        SELECT table_type, table_owner, comment, created, last_altered, data_source_format
        FROM system.information_schema.tables
        WHERE table_catalog = '{catalog}' AND table_schema = '{schema}' AND table_name = '{table}'
    """)
    if not table_rows:
        return {"error": f"Table not found: {full_name}"}

    t = table_rows[0]

    # Columns
    col_rows = _query(f"""
        SELECT column_name, data_type, full_data_type, is_nullable, column_default, comment,
               numeric_precision, numeric_scale, character_maximum_length
        FROM system.information_schema.columns
        WHERE table_catalog = '{catalog}' AND table_schema = '{schema}' AND table_name = '{table}'
        ORDER BY ordinal_position
    """)

    # Tags
    tags = _query_safe(f"""
        SELECT tag_name, tag_value
        FROM system.information_schema.table_tags
        WHERE catalog_name = '{catalog}' AND schema_name = '{schema}' AND table_name = '{table}'
    """)

    # Constraints
    constraints = _query_safe(f"""
        SELECT tc.constraint_name, tc.constraint_type, ccu.column_name
        FROM system.information_schema.table_constraints tc
        JOIN system.information_schema.constraint_column_usage ccu
          ON tc.constraint_name = ccu.constraint_name AND tc.constraint_catalog = ccu.constraint_catalog
        WHERE tc.table_catalog = '{catalog}' AND tc.table_schema = '{schema}' AND tc.table_name = '{table}'
    """)

    return {
        "name": table,
        "full_name": full_name,
        "catalog_name": catalog,
        "schema_name": schema,
        "table_type": t.get("table_type"),
        "owner": t.get("table_owner"),
        "comment": t.get("comment") or "",
        "created": t.get("created"),
        "last_altered": t.get("last_altered"),
        "data_source_format": t.get("data_source_format"),
        "columns": [
            {
                "name": c.get("column_name"),
                "type": c.get("full_data_type") or c.get("data_type"),
                "comment": c.get("comment") or "",
                "nullable": c.get("is_nullable") == "YES",
                "default": c.get("column_default"),
            }
            for c in col_rows
        ],
        "tags": tags,
        "constraints": constraints,
    }


def _search_tables(query: str) -> list[dict]:
    q = query.replace("'", "''")
    return _query(f"""
        SELECT table_catalog, table_schema, table_name, table_type, comment
        FROM system.information_schema.tables
        WHERE table_catalog IN ({_catalog_filter()})
          AND table_schema NOT IN ('information_schema', 'pg_catalog', '__db_system', 'default')
          AND (
            LOWER(table_name) LIKE '%{q.lower()}%'
            OR LOWER(table_schema) LIKE '%{q.lower()}%'
            OR LOWER(comment) LIKE '%{q.lower()}%'
          )
        LIMIT 10
    """)


# ---------------------------------------------------------------------------
# New tools
# ---------------------------------------------------------------------------

def _get_table_tags(full_name: str) -> list[dict]:
    parts = full_name.split(".")
    if len(parts) != 3:
        return [{"error": "Expected catalog.schema.table format"}]
    catalog, schema, table = parts
    err = _check_catalog(catalog)
    if err:
        return [{"error": err}]
    return _query_safe(f"""
        SELECT tag_name, tag_value
        FROM system.information_schema.table_tags
        WHERE catalog_name = '{catalog}' AND schema_name = '{schema}' AND table_name = '{table}'
    """) or [{"info": "No tags found"}]


def _get_column_tags(full_name: str) -> list[dict]:
    parts = full_name.split(".")
    if len(parts) != 3:
        return [{"error": "Expected catalog.schema.table format"}]
    catalog, schema, table = parts
    err = _check_catalog(catalog)
    if err:
        return [{"error": err}]
    return _query_safe(f"""
        SELECT column_name, tag_name, tag_value
        FROM system.information_schema.column_tags
        WHERE catalog_name = '{catalog}' AND schema_name = '{schema}' AND table_name = '{table}'
    """) or [{"info": "No column tags found"}]


def _get_table_lineage(full_name: str) -> dict:
    parts = full_name.split(".")
    if len(parts) != 3:
        return {"error": "Expected catalog.schema.table format"}
    catalog, schema, table = parts
    err = _check_catalog(catalog)
    if err:
        return {"error": err}

    upstream = _query_safe(f"""
        SELECT DISTINCT source_table_full_name, source_type
        FROM system.access.table_lineage
        WHERE target_table_catalog = '{catalog}'
          AND target_table_schema = '{schema}'
          AND target_table_name = '{table}'
          AND source_table_full_name IS NOT NULL
        LIMIT 20
    """)

    downstream = _query_safe(f"""
        SELECT DISTINCT target_table_full_name, target_type
        FROM system.access.table_lineage
        WHERE source_table_catalog = '{catalog}'
          AND source_table_schema = '{schema}'
          AND source_table_name = '{table}'
          AND target_table_full_name IS NOT NULL
        LIMIT 20
    """)

    if not upstream and not downstream:
        return {"info": "No lineage data available. Lineage system tables may not be enabled on this workspace."}

    return {"upstream": upstream, "downstream": downstream}


def _get_table_constraints(full_name: str) -> list[dict]:
    parts = full_name.split(".")
    if len(parts) != 3:
        return [{"error": "Expected catalog.schema.table format"}]
    catalog, schema, table = parts
    err = _check_catalog(catalog)
    if err:
        return [{"error": err}]
    return _query_safe(f"""
        SELECT tc.constraint_name, tc.constraint_type, ccu.column_name
        FROM system.information_schema.table_constraints tc
        JOIN system.information_schema.constraint_column_usage ccu
          ON tc.constraint_name = ccu.constraint_name AND tc.constraint_catalog = ccu.constraint_catalog
        WHERE tc.table_catalog = '{catalog}' AND tc.table_schema = '{schema}' AND tc.table_name = '{table}'
    """) or [{"info": "No constraints found"}]


def _get_table_privileges(full_name: str) -> list[dict]:
    parts = full_name.split(".")
    if len(parts) != 3:
        return [{"error": "Expected catalog.schema.table format"}]
    catalog, schema, table = parts
    err = _check_catalog(catalog)
    if err:
        return [{"error": err}]
    return _query_safe(f"""
        SELECT grantee, privilege_type, grantor, inherited_from
        FROM system.information_schema.table_privileges
        WHERE table_catalog = '{catalog}' AND table_schema = '{schema}' AND table_name = '{table}'
    """) or [{"info": "No privilege data available"}]


# ---------------------------------------------------------------------------
# Volume tools
# ---------------------------------------------------------------------------

def _list_volumes(catalog_name: str, schema_name: str) -> list[dict]:
    err = _check_catalog(catalog_name)
    if err:
        return [{"error": err}]
    return _query(f"""
        SELECT volume_name, volume_type, comment, storage_location, created, created_by
        FROM system.information_schema.volumes
        WHERE catalog_name = '{catalog_name}' AND schema_name = '{schema_name}'
        ORDER BY volume_name
    """) or [{"info": "No volumes found"}]


def _get_volume_details(full_name: str) -> dict:
    """Get volume metadata and list files."""
    parts = full_name.split(".")
    if len(parts) != 3:
        return {"error": "Expected catalog.schema.volume format"}
    catalog, schema, volume = parts
    err = _check_catalog(catalog)
    if err:
        return {"error": err}

    # Volume metadata
    vol_rows = _query(f"""
        SELECT volume_name, volume_type, comment, storage_location, created, last_altered, created_by
        FROM system.information_schema.volumes
        WHERE catalog_name = '{catalog}' AND schema_name = '{schema}' AND volume_name = '{volume}'
    """)
    if not vol_rows:
        return {"error": f"Volume not found: {full_name}"}

    v = vol_rows[0]
    result = {
        "name": volume,
        "full_name": full_name,
        "volume_type": v.get("volume_type"),
        "comment": v.get("comment") or "",
        "storage_location": v.get("storage_location"),
        "owner": v.get("created_by"),
        "created": v.get("created"),
        "files": [],
    }

    # List files via Files API
    try:
        client = get_workspace_client()
        file_list = client.files.list_directory_contents(f"/Volumes/{catalog}/{schema}/{volume}")
        for f in file_list:
            result["files"].append({
                "name": f.name,
                "path": f.path,
                "size": getattr(f, "file_size", None),
                "is_directory": getattr(f, "is_directory", False),
            })
    except Exception as e:
        result["files_error"] = str(e)[:200]

    return result

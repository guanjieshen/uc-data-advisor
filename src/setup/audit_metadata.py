"""Audit Unity Catalog metadata via system tables.

Queries system.information_schema for catalogs, schemas, tables, columns,
tags, and constraints. All queries run as the configured service principal.
"""

import base64
import logging
import time

logger = logging.getLogger(__name__)

SKIP_SCHEMAS = {"information_schema", "pg_catalog", "__db_system", "default", "metrics"}


def audit(config: dict, w) -> dict:
    """Audit source catalogs via system tables and return enriched metadata."""
    source_catalogs = config.get("source_catalogs", [])
    include_schemas = set(config.get("include_schemas", []))
    exclude_schemas = set(config.get("exclude_schemas", []))
    infra = config.get("infrastructure", {})
    warehouse_id = infra.get("warehouse_id", "")

    if not warehouse_id:
        raise RuntimeError("warehouse_id required for system table audit")

    # Authenticate as the configured SP
    sp_client = _get_sp_client(config, w)

    print("=" * 60)
    print("Metadata Audit (system tables)")
    print("=" * 60)

    catalog_filter = ", ".join(f"'{c}'" for c in source_catalogs)

    # 1. Catalogs
    print("  Querying catalogs...", end=" ", flush=True)
    cat_rows = _query(sp_client, warehouse_id, f"""
        SELECT catalog_name, catalog_owner, comment, created, last_altered
        FROM system.information_schema.catalogs
        WHERE catalog_name IN ({catalog_filter})
    """)
    print(f"{len(cat_rows)} found")

    # 2. Schemas
    print("  Querying schemas...", end=" ", flush=True)
    schema_rows = _query(sp_client, warehouse_id, f"""
        SELECT catalog_name, schema_name, schema_owner, comment, created
        FROM system.information_schema.schemata
        WHERE catalog_name IN ({catalog_filter})
          AND schema_name NOT IN ('information_schema', 'pg_catalog', '__db_system')
    """)
    # Apply include/exclude filters
    schema_rows = [
        r for r in schema_rows
        if r["schema_name"] not in SKIP_SCHEMAS
        and (not include_schemas or r["schema_name"] in include_schemas)
        and r["schema_name"] not in exclude_schemas
    ]
    print(f"{len(schema_rows)} found")

    # 3. Tables
    print("  Querying tables...", end=" ", flush=True)
    valid_schemas = {(r["catalog_name"], r["schema_name"]) for r in schema_rows}
    table_rows = _query(sp_client, warehouse_id, f"""
        SELECT table_catalog, table_schema, table_name, table_type, table_owner,
               comment, created, last_altered, data_source_format
        FROM system.information_schema.tables
        WHERE table_catalog IN ({catalog_filter})
          AND table_schema NOT IN ('information_schema', 'pg_catalog', '__db_system')
    """)
    table_rows = [r for r in table_rows if (r["table_catalog"], r["table_schema"]) in valid_schemas]
    print(f"{len(table_rows)} found")

    # 4. Columns
    print("  Querying columns...", end=" ", flush=True)
    col_rows = _query(sp_client, warehouse_id, f"""
        SELECT table_catalog, table_schema, table_name, column_name, ordinal_position,
               data_type, full_data_type, is_nullable, column_default, comment,
               character_maximum_length, numeric_precision, numeric_scale
        FROM system.information_schema.columns
        WHERE table_catalog IN ({catalog_filter})
          AND table_schema NOT IN ('information_schema', 'pg_catalog', '__db_system')
        ORDER BY table_catalog, table_schema, table_name, ordinal_position
    """)
    print(f"{len(col_rows)} found")

    # 5. Tags (best-effort)
    print("  Querying tags...", end=" ", flush=True)
    table_tag_rows = _query_safe(sp_client, warehouse_id, f"""
        SELECT catalog_name, schema_name, table_name, tag_name, tag_value
        FROM system.information_schema.table_tags
        WHERE catalog_name IN ({catalog_filter})
    """)
    col_tag_rows = _query_safe(sp_client, warehouse_id, f"""
        SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value
        FROM system.information_schema.column_tags
        WHERE catalog_name IN ({catalog_filter})
    """)
    print(f"{len(table_tag_rows)} table tags, {len(col_tag_rows)} column tags")

    # 6. Constraints (best-effort)
    print("  Querying constraints...", end=" ", flush=True)
    constraint_rows = _query_safe(sp_client, warehouse_id, f"""
        SELECT tc.table_catalog, tc.table_schema, tc.table_name, tc.constraint_name,
               tc.constraint_type, ccu.column_name
        FROM system.information_schema.table_constraints tc
        JOIN system.information_schema.constraint_column_usage ccu
          ON tc.constraint_name = ccu.constraint_name
          AND tc.constraint_catalog = ccu.constraint_catalog
        WHERE tc.table_catalog IN ({catalog_filter})
    """)
    print(f"{len(constraint_rows)} found")

    # Build structured output
    catalogs, schemas, tables = _build_audit_result(
        cat_rows, schema_rows, table_rows, col_rows,
        table_tag_rows, col_tag_rows, constraint_rows,
    )

    total_cols = sum(len(t["columns"]) for t in tables)
    cols_with_comments = sum(1 for t in tables for c in t["columns"] if c.get("comment"))
    tables_with_comments = sum(1 for t in tables if t.get("comment"))

    audit_result = {
        "catalogs": catalogs,
        "schemas": schemas,
        "tables": tables,
        "total_tables": len(tables),
        "total_columns": total_cols,
        "tables_with_comments": tables_with_comments,
        "columns_with_comments": cols_with_comments,
        "description_coverage_pct": round(
            100.0 * (tables_with_comments + cols_with_comments) / max(len(tables) + total_cols, 1), 1
        ),
    }

    print()
    print(f"  Total: {len(catalogs)} catalogs, {len(schemas)} schemas, {len(tables)} tables, {total_cols} columns")
    print(f"  Description coverage: {audit_result['description_coverage_pct']}%")

    return audit_result


def _get_sp_client(config: dict, w):
    """Create a WorkspaceClient authenticated as the configured SP."""
    from databricks.sdk import WorkspaceClient

    infra = config.get("infrastructure", {})
    host = config.get("workspace", {}).get("host", "")
    scope = infra.get("secret_scope", "")

    if scope:
        try:
            raw_id = w.secrets.get_secret(scope=scope, key="sp-client-id").value or ""
            raw_secret = w.secrets.get_secret(scope=scope, key="sp-client-secret").value or ""
            client_id = base64.b64decode(raw_id).decode() if raw_id else ""
            client_secret = base64.b64decode(raw_secret).decode() if raw_secret else ""
            if host and client_id and client_secret:
                return WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
        except Exception as e:
            logger.warning(f"Could not authenticate as SP from scope: {e}")

    # Fall back to deployer's client
    return w


def _query(client, warehouse_id: str, sql: str) -> list[dict]:
    """Execute SQL and return rows as list of dicts."""
    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql, wait_timeout="50s",
    )
    status = resp.status.state.value if resp.status else "unknown"
    while status in ("PENDING", "RUNNING"):
        time.sleep(3)
        resp = client.statement_execution.get_statement(resp.statement_id)
        status = resp.status.state.value if resp.status else "unknown"

    if status != "SUCCEEDED":
        error = resp.status.error.message if resp.status and resp.status.error else "unknown"
        raise RuntimeError(f"SQL failed: {error}")

    if not resp.result or not resp.result.data_array:
        return []

    columns = [c.name.lower() for c in resp.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in resp.result.data_array]


def _query_safe(client, warehouse_id: str, sql: str) -> list[dict]:
    """Execute SQL, return empty list on failure (for optional system tables)."""
    try:
        return _query(client, warehouse_id, sql)
    except Exception as e:
        logger.info(f"Optional query failed (expected on some workspaces): {e}")
        return []


def _build_audit_result(cat_rows, schema_rows, table_rows, col_rows,
                        table_tag_rows, col_tag_rows, constraint_rows):
    """Assemble structured audit result from raw query rows."""

    # Index columns by table
    col_index = {}
    for c in col_rows:
        key = (c["table_catalog"], c["table_schema"], c["table_name"])
        col_index.setdefault(key, []).append(c)

    # Index table tags
    tag_index = {}
    for t in table_tag_rows:
        key = (t["catalog_name"], t["schema_name"], t["table_name"])
        tag_index.setdefault(key, []).append({"tag_name": t["tag_name"], "tag_value": t["tag_value"]})

    # Index column tags
    col_tag_index = {}
    for ct in col_tag_rows:
        key = (ct["catalog_name"], ct["schema_name"], ct["table_name"], ct["column_name"])
        col_tag_index.setdefault(key, []).append({"tag_name": ct["tag_name"], "tag_value": ct["tag_value"]})

    # Index constraints
    constraint_index = {}
    for cr in constraint_rows:
        key = (cr["table_catalog"], cr["table_schema"], cr["table_name"])
        cname = cr["constraint_name"]
        if key not in constraint_index:
            constraint_index[key] = {}
        if cname not in constraint_index[key]:
            constraint_index[key][cname] = {
                "constraint_name": cname,
                "constraint_type": cr["constraint_type"],
                "columns": [],
            }
        constraint_index[key][cname]["columns"].append(cr["column_name"])

    # Build catalogs
    catalogs = []
    for r in cat_rows:
        catalogs.append({
            "name": r["catalog_name"],
            "comment": r.get("comment") or "",
            "owner": r.get("catalog_owner") or "",
            "created": r.get("created") or "",
            "last_altered": r.get("last_altered") or "",
            "schema_count": 0,
            "table_count": 0,
        })

    # Build schemas
    schemas = []
    for r in schema_rows:
        schemas.append({
            "catalog_name": r["catalog_name"],
            "name": r["schema_name"],
            "full_name": f"{r['catalog_name']}.{r['schema_name']}",
            "comment": r.get("comment") or "",
            "owner": r.get("schema_owner") or "",
            "table_count": 0,
        })

    # Build tables with enriched metadata
    tables = []
    for r in table_rows:
        key = (r["table_catalog"], r["table_schema"], r["table_name"])
        full_name = f"{r['table_catalog']}.{r['table_schema']}.{r['table_name']}"

        # Columns
        columns = []
        for c in col_index.get(key, []):
            col_tags = col_tag_index.get((*key, c["column_name"]), [])
            columns.append({
                "name": c["column_name"],
                "type": c.get("full_data_type") or c.get("data_type") or "STRING",
                "comment": c.get("comment") or "",
                "nullable": c.get("is_nullable", "YES") == "YES",
                "column_default": c.get("column_default") or "",
                "numeric_precision": c.get("numeric_precision"),
                "numeric_scale": c.get("numeric_scale"),
                "character_maximum_length": c.get("character_maximum_length"),
                "tags": col_tags if col_tags else [],
            })

        # Constraints for this table
        constraints = list(constraint_index.get(key, {}).values())

        table_record = {
            "catalog_name": r["table_catalog"],
            "schema_name": r["table_schema"],
            "name": r["table_name"],
            "full_name": full_name,
            "comment": r.get("comment") or "",
            "table_type": r.get("table_type") or "",
            "owner": r.get("table_owner") or "",
            "created": r.get("created") or "",
            "last_altered": r.get("last_altered") or "",
            "data_source_format": r.get("data_source_format") or "",
            "columns": columns,
            "tags": tag_index.get(key, []),
            "constraints": constraints,
        }
        tables.append(table_record)

    # Update counts
    for cat in catalogs:
        cat["schema_count"] = len([s for s in schemas if s["catalog_name"] == cat["name"]])
        cat["table_count"] = len([t for t in tables if t["catalog_name"] == cat["name"]])
    for schema in schemas:
        schema["table_count"] = len([
            t for t in tables
            if t["catalog_name"] == schema["catalog_name"] and t["schema_name"] == schema["name"]
        ])

    return catalogs, schemas, tables

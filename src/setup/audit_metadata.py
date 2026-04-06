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

    # 7. Lineage (best-effort — requires system.access enabled)
    print("  Querying lineage...", end=" ", flush=True)
    lineage_rows = _query_safe(sp_client, warehouse_id, f"""
        SELECT DISTINCT source_table_full_name, target_table_full_name
        FROM system.access.table_lineage
        WHERE (source_table_catalog IN ({catalog_filter}) OR target_table_catalog IN ({catalog_filter}))
          AND source_table_full_name IS NOT NULL
          AND target_table_full_name IS NOT NULL
    """)
    print(f"{len(lineage_rows)} relationships")

    # 8. Privileges (best-effort)
    print("  Querying privileges...", end=" ", flush=True)
    privilege_rows = _query_safe(sp_client, warehouse_id, f"""
        SELECT table_catalog, table_schema, table_name, grantee, privilege_type
        FROM system.information_schema.table_privileges
        WHERE table_catalog IN ({catalog_filter})
    """)
    print(f"{len(privilege_rows)} grants")

    sample_data = {}

    # 9. Volumes
    print("  Querying volumes...", end=" ", flush=True)
    volume_rows = _query_safe(sp_client, warehouse_id, f"""
        SELECT catalog_name, schema_name, volume_name, volume_type, comment,
               storage_location, created, last_altered, created_by
        FROM system.information_schema.volumes
        WHERE catalog_name IN ({catalog_filter})
          AND schema_name NOT IN ('information_schema', 'pg_catalog', '__db_system')
    """)
    volume_rows = [r for r in volume_rows if (r.get("catalog_name"), r.get("schema_name")) in valid_schemas]
    print(f"{len(volume_rows)} found")

    # 8. Volume file listing (best-effort via Files API)
    volumes = _build_volumes(volume_rows, sp_client)

    # 9. Volume content indexing (if enabled)
    if config.get("enable_volume_indexing", False) and volumes:
        print("  Indexing volume contents...", end=" ", flush=True)
        _index_volume_contents(volumes, sp_client)
        print(f"{sum(len(v.get('files', [])) for v in volumes)} files indexed")

    # Build structured output
    catalogs, schemas, tables = _build_audit_result(
        cat_rows, schema_rows, table_rows, col_rows,
        table_tag_rows, col_tag_rows, constraint_rows,
        lineage_rows, privilege_rows, sample_data,
    )

    total_cols = sum(len(t["columns"]) for t in tables)
    cols_with_comments = sum(1 for t in tables for c in t["columns"] if c.get("comment"))
    tables_with_comments = sum(1 for t in tables if t.get("comment"))

    audit_result = {
        "catalogs": catalogs,
        "schemas": schemas,
        "tables": tables,
        "volumes": volumes,
        "total_tables": len(tables),
        "total_columns": total_cols,
        "total_volumes": len(volumes),
        "tables_with_comments": tables_with_comments,
        "columns_with_comments": cols_with_comments,
        "description_coverage_pct": round(
            100.0 * (tables_with_comments + cols_with_comments) / max(len(tables) + total_cols, 1), 1
        ),
    }

    print()
    print(f"  Total: {len(catalogs)} catalogs, {len(schemas)} schemas, {len(tables)} tables, {len(volumes)} volumes, {total_cols} columns")
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
                        table_tag_rows, col_tag_rows, constraint_rows,
                        lineage_rows=None, privilege_rows=None, sample_data=None):
    """Assemble structured audit result from raw query rows."""
    lineage_rows = lineage_rows or []
    privilege_rows = privilege_rows or []
    sample_data = sample_data or {}

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

    # Index lineage
    lineage_upstream = {}  # target -> [sources]
    lineage_downstream = {}  # source -> [targets]
    for lr in lineage_rows:
        src = lr.get("source_table_full_name", "")
        tgt = lr.get("target_table_full_name", "")
        if src and tgt:
            lineage_upstream.setdefault(tgt, set()).add(src)
            lineage_downstream.setdefault(src, set()).add(tgt)

    # Index privileges
    privilege_index = {}
    for pr in privilege_rows:
        key = (pr.get("table_catalog", ""), pr.get("table_schema", ""), pr.get("table_name", ""))
        privilege_index.setdefault(key, []).append({
            "grantee": pr.get("grantee", ""),
            "privilege_type": pr.get("privilege_type", ""),
        })

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

        # Lineage for this table
        upstream = sorted(lineage_upstream.get(full_name, set()))
        downstream = sorted(lineage_downstream.get(full_name, set()))

        # Privileges for this table
        privileges = privilege_index.get(key, [])

        # Sample data
        samples = sample_data.get(full_name, [])

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
            "upstream": upstream,
            "downstream": downstream,
            "privileges": privileges,
            "sample_data": samples[:5],
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


def _build_volumes(volume_rows: list[dict], sp_client) -> list[dict]:
    """Build volume records with file listings."""
    volumes = []
    for r in volume_rows:
        full_name = f"{r['catalog_name']}.{r['schema_name']}.{r['volume_name']}"
        vol = {
            "catalog_name": r.get("catalog_name", ""),
            "schema_name": r.get("schema_name", ""),
            "name": r.get("volume_name", ""),
            "full_name": full_name,
            "volume_type": r.get("volume_type", ""),
            "comment": r.get("comment") or "",
            "storage_location": r.get("storage_location") or "",
            "owner": r.get("created_by") or "",
            "created": r.get("created") or "",
            "files": [],
        }

        # List files in volume via Files API (best-effort)
        try:
            file_list = sp_client.files.list_directory_contents(
                f"/Volumes/{r['catalog_name']}/{r['schema_name']}/{r['volume_name']}"
            )
            for f in file_list:
                vol["files"].append({
                    "name": f.name,
                    "path": f.path,
                    "size": getattr(f, "file_size", None),
                    "is_directory": getattr(f, "is_directory", False),
                    "last_modified": str(getattr(f, "last_modified", "")),
                })
        except Exception as e:
            logger.info(f"Could not list files in {full_name}: {e}")

        volumes.append(vol)
    return volumes


def _index_volume_contents(volumes: list[dict], sp_client) -> None:
    """Extract text content from documents in volumes for indexing."""
    for vol in volumes:
        for f in vol.get("files", []):
            if f.get("is_directory") or not f.get("path"):
                continue

            name = f.get("name", "").lower()
            content = ""

            try:
                if name.endswith(".csv"):
                    # Read first few lines as preview
                    resp = sp_client.files.download(f["path"])
                    raw = resp.contents.read(4096).decode("utf-8", errors="replace")
                    content = raw[:2000]

                elif name.endswith(".txt") or name.endswith(".md"):
                    resp = sp_client.files.download(f["path"])
                    content = resp.contents.read(8192).decode("utf-8", errors="replace")[:4000]

                elif name.endswith(".json"):
                    resp = sp_client.files.download(f["path"])
                    content = resp.contents.read(4096).decode("utf-8", errors="replace")[:2000]

                elif name.endswith(".pdf"):
                    # PDF text extraction — requires the file to be text-based
                    try:
                        import io
                        resp = sp_client.files.download(f["path"])
                        pdf_bytes = resp.contents.read(1_000_000)  # 1MB max
                        try:
                            from PyPDF2 import PdfReader
                            reader = PdfReader(io.BytesIO(pdf_bytes))
                            pages = []
                            for page in reader.pages[:10]:  # First 10 pages
                                pages.append(page.extract_text() or "")
                            content = "\n".join(pages)[:4000]
                        except ImportError:
                            # PyPDF2 not available — store filename only
                            content = f"PDF document: {name}"
                    except Exception:
                        content = f"PDF document: {name}"

            except Exception as e:
                logger.info(f"Could not read {f['path']}: {e}")

            f["content_preview"] = content

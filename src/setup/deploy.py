"""Deploy generated artifacts to the Databricks workspace.

Creates Delta tables, VS indexes, metric views, materialized tables,
updates Genie Space tables.
"""

import hashlib
import json
import os
import time
import logging

logger = logging.getLogger(__name__)


def deploy(config: dict, w) -> None:
    """Deploy all generated artifacts."""
    infra = config.get("infrastructure", {})
    config.get("generated", {})
    warehouse_id = infra.get("warehouse_id", "")

    print("=" * 60)
    print("Deploying Artifacts")
    print("=" * 60)

    # Step 1: Metadata docs Delta table + VS index
    _deploy_metadata_table(config, w, warehouse_id)

    # Step 2: Knowledge base Delta table + VS index
    _deploy_knowledge_base(config, w, warehouse_id)

    # Step 3-4: Metric views + materialized tables (optional)
    if config.get("enable_metric_views", False):
        _deploy_metric_views(config, w, warehouse_id)
        _deploy_metric_tables(config, w, warehouse_id)
    else:
        print("  [metric-views] Skipped (enable_metric_views: false)")
        print("  [metric-tables] Skipped (enable_metric_views: false)")

    # Step 5: Update Genie Space
    _deploy_genie_space(config, w)

    print()
    print("=" * 60)
    print("Deployment complete")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Step 1: Metadata docs
# ---------------------------------------------------------------------------

def _deploy_metadata_table(config, w, warehouse_id):
    """Write UC metadata to Delta table and create VS index."""
    infra = config.get("infrastructure", {})
    audit = config.get("generated", {}).get("audit", {})
    catalog = infra["advisor_catalog"]
    schema = infra.get("advisor_schema", "default")
    table = f"{catalog}.{schema}.uc_metadata_docs"
    vs_index = f"{catalog}.{schema}.uc_metadata_vs_index"
    vs_endpoint = infra.get("vs_endpoint", "")
    embedding_model = config.get("embedding_model", "databricks-bge-large-en")

    print(f"  [metadata] Writing {len(audit.get('tables', []))} tables to {table}...")

    # Create table with enriched columns
    _run_sql(w, warehouse_id, f"DROP TABLE IF EXISTS {table}")
    _run_sql(w, warehouse_id, f"""
        CREATE TABLE {table} (
            doc_id STRING NOT NULL,
            catalog_name STRING, schema_name STRING, table_name STRING,
            full_table_name STRING, table_comment STRING, table_type STRING,
            table_owner STRING, data_source_format STRING,
            created_at STRING, last_altered STRING,
            columns_json STRING, tags_json STRING, constraints_json STRING,
            description_text STRING
        ) USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)

    # Insert rows in batches
    rows = []
    for tbl in audit.get("tables", []):
        desc_parts = _build_table_description(tbl)
        description_text = "\n".join(desc_parts)

        tags = tbl.get("tags", [])
        constraints = tbl.get("constraints", [])

        doc_id = hashlib.md5(tbl["full_name"].encode()).hexdigest()[:16]
        rows.append({
            "doc_id": doc_id,
            "catalog_name": tbl["catalog_name"],
            "schema_name": tbl["schema_name"],
            "table_name": tbl["name"],
            "full_table_name": tbl["full_name"],
            "table_comment": tbl.get("comment", ""),
            "table_type": tbl.get("table_type", ""),
            "table_owner": tbl.get("owner", ""),
            "data_source_format": tbl.get("data_source_format", ""),
            "created_at": str(tbl.get("created", "")),
            "last_altered": str(tbl.get("last_altered", "")),
            "columns_json": json.dumps([{"name": c["name"], "type": c["type"], "comment": c.get("comment", "")} for c in tbl.get("columns", [])]),
            "tags_json": json.dumps(tags),
            "constraints_json": json.dumps(constraints),
            "description_text": description_text,
        })

    # Add volume docs to the same metadata table
    for vol in audit.get("volumes", []):
        desc_parts = [
            f"Volume: {vol['full_name']}",
            f"Catalog: {vol['catalog_name']}",
            f"Schema: {vol['catalog_name']}.{vol['schema_name']}",
            f"Type: {vol.get('volume_type', 'MANAGED')}",
        ]
        if vol.get("comment"):
            desc_parts.append(f"Description: {vol['comment']}")
        if vol.get("storage_location"):
            desc_parts.append(f"Location: {vol['storage_location']}")

        files = vol.get("files", [])
        if files:
            file_names = [f.get("name", "") for f in files if not f.get("is_directory")]
            desc_parts.append(f"Files ({len(file_names)}): " + ", ".join(file_names[:20]))

            # Include content previews if indexed
            for f in files:
                preview = f.get("content_preview", "")
                if preview and len(preview) > 20:
                    desc_parts.append(f"Content of {f['name']}: {preview[:500]}")

        doc_id = hashlib.md5(vol["full_name"].encode()).hexdigest()[:16]
        rows.append({
            "doc_id": doc_id,
            "catalog_name": vol["catalog_name"],
            "schema_name": vol["schema_name"],
            "table_name": vol["name"],
            "full_table_name": vol["full_name"],
            "table_comment": vol.get("comment", ""),
            "table_type": "VOLUME",
            "table_owner": vol.get("owner", ""),
            "data_source_format": vol.get("volume_type", ""),
            "created_at": str(vol.get("created", "")),
            "last_altered": "",
            "columns_json": json.dumps([{"name": f.get("name", ""), "type": "file", "comment": ""} for f in files]),
            "tags_json": "[]",
            "constraints_json": "[]",
            "description_text": "\n".join(desc_parts),
        })

    # Batch insert
    BATCH = 20
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i+BATCH]
        values = ",\n".join(
            f"('{_esc(r['doc_id'])}', '{_esc(r['catalog_name'])}', '{_esc(r['schema_name'])}', "
            f"'{_esc(r['table_name'])}', '{_esc(r['full_table_name'])}', '{_esc(r['table_comment'])}', "
            f"'{_esc(r['table_type'])}', '{_esc(r['table_owner'])}', '{_esc(r['data_source_format'])}', "
            f"'{_esc(r['created_at'])}', '{_esc(r['last_altered'])}', "
            f"'{_esc(r['columns_json'])}', '{_esc(r['tags_json'])}', '{_esc(r['constraints_json'])}', "
            f"'{_esc(r['description_text'])}')"
            for r in batch
        )
        _run_sql(w, warehouse_id, f"INSERT INTO {table} VALUES\n{values}")

    print(f"    Inserted {len(rows)} rows")

    # Create VS index
    _create_vs_index(w, vs_index, vs_endpoint, table, "doc_id", "description_text", embedding_model)

    infra["vs_index_metadata"] = vs_index


def _deploy_knowledge_base(config, w, warehouse_id):
    """Write knowledge base FAQ to Delta table and create VS index."""
    infra = config.get("infrastructure", {})
    kb = config.get("generated", {}).get("knowledge_base", [])
    catalog = infra["advisor_catalog"]
    schema = infra.get("advisor_schema", "default")
    table = f"{catalog}.{schema}.knowledge_base"
    vs_index = f"{catalog}.{schema}.knowledge_vs_index"
    vs_endpoint = infra.get("vs_endpoint", "")
    embedding_model = config.get("embedding_model", "databricks-bge-large-en")

    print(f"  [knowledge] Writing {len(kb)} FAQs to {table}...")

    _run_sql(w, warehouse_id, f"DROP TABLE IF EXISTS {table}")
    _run_sql(w, warehouse_id, f"""
        CREATE TABLE {table} (
            id INT, question STRING, answer STRING, category STRING, source STRING,
            search_text STRING
        ) USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)

    # Insert FAQs with search_text = question + answer
    BATCH = 10
    for i in range(0, len(kb), BATCH):
        batch = kb[i:i+BATCH]
        values = ",\n".join(
            f"({i+j+1}, '{_esc(faq['question'])}', '{_esc(faq['answer'])}', '{_esc(faq.get('category',''))}', '{_esc(faq.get('source',''))}', '{_esc(faq['question'] + ' ' + faq['answer'])}')"
            for j, faq in enumerate(batch)
        )
        _run_sql(w, warehouse_id, f"INSERT INTO {table} VALUES\n{values}")

    print(f"    Inserted {len(kb)} FAQs")

    # Create VS index on search_text column
    _create_vs_index(w, vs_index, vs_endpoint, table, "id", "search_text", embedding_model)

    infra["vs_index_knowledge"] = vs_index


# ---------------------------------------------------------------------------
# Step 3: Metric views
# ---------------------------------------------------------------------------

def _deploy_metric_views(config, w, warehouse_id):
    """Create Databricks Metric Views."""
    views = config.get("generated", {}).get("metric_views", {})
    print(f"  [metric-views] Creating {len(views)} metric views...")

    succeeded = 0
    for name, sql in views.items():
        try:
            _run_sql(w, warehouse_id, sql)
            succeeded += 1
        except Exception as e:
            logger.warning(f"    {name}: FAILED ({e})")

    print(f"    {succeeded}/{len(views)} created")


# ---------------------------------------------------------------------------
# Step 4: Materialize metric tables
# ---------------------------------------------------------------------------

def _deploy_metric_tables(config, w, warehouse_id):
    """Materialize metric tables from metric views."""
    refreshes = config.get("generated", {}).get("metric_refreshes", [])

    # Ensure metrics schema exists (derive from first refresh target table)
    if refreshes:
        first_table = refreshes[0]["target_table"]
        metric_catalog_schema = ".".join(first_table.split(".")[:2])
        _run_sql(w, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {metric_catalog_schema}")

    print(f"  [metric-tables] Materializing {len(refreshes)} tables...")
    succeeded = 0
    for r in refreshes:
        try:
            _run_sql(w, warehouse_id, f"CREATE OR REPLACE TABLE {r['target_table']} AS {r['query']}")
            succeeded += 1
        except Exception as e:
            logger.warning(f"    {r['target_table']}: FAILED ({e})")

    print(f"    {succeeded}/{len(refreshes)} materialized")


# ---------------------------------------------------------------------------
# Step 5: Genie Space
# ---------------------------------------------------------------------------

def _deploy_genie_space(config, w):
    """Update Genie Space with generated table list."""
    infra = config.get("infrastructure", {})
    genie_tables = config.get("generated", {}).get("genie_tables", [])
    space_id = infra.get("genie_space_id", "")

    if not space_id or not genie_tables:
        print("  [genie] Skipping (no space ID or tables)")
        return

    print(f"  [genie] Updating space {space_id} with {len(genie_tables)} tables...")

    serialized = json.dumps({
        "version": 2,
        "data_sources": {
            "tables": [{"identifier": t} for t in sorted(genie_tables)]
        },
    })

    try:
        w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body={
            "serialized_space": serialized,
        })
        print("    Updated")
    except Exception as e:
        logger.warning(f"  [genie] Skipping — update failed ({len(genie_tables)} tables): {e}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_vs_index(w, index_name, endpoint_name, source_table, primary_key, embedding_col, embedding_model):
    """Create or recreate a Delta Sync VS index."""
    print(f"    Creating VS index {index_name}...", end=" ", flush=True)

    # Delete existing and wait for full deletion
    try:
        w.vector_search_indexes.get_index(index_name)
        w.vector_search_indexes.delete_index(index_name)
        print("deleting old...", end=" ", flush=True)
        for _ in range(60):
            time.sleep(5)
            try:
                w.vector_search_indexes.get_index(index_name)
            except Exception:
                break
        else:
            print("deletion still pending, will retry later")
            return
    except Exception:
        pass  # Index doesn't exist, proceed

    try:
        from databricks.sdk.service.vectorsearch import (
            VectorIndexType, DeltaSyncVectorIndexSpecRequest,
            EmbeddingSourceColumn, PipelineType,
        )
        w.vector_search_indexes.create_index(
            name=index_name,
            endpoint_name=endpoint_name,
            primary_key=primary_key,
            index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                source_table=source_table,
                pipeline_type=PipelineType.TRIGGERED,
                embedding_source_columns=[
                    EmbeddingSourceColumn(name=embedding_col, embedding_model_endpoint_name=embedding_model),
                ],
            ),
        )
        print("created (provisioning in background)")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("already exists")
        else:
            print(f"FAILED: {e}")


def _build_table_description(tbl: dict) -> list[str]:
    """Build comprehensive description_text for VS embedding from all metadata."""
    desc = [
        f"Table: {tbl['full_name']}",
        f"Catalog: {tbl['catalog_name']}",
        f"Schema: {tbl['catalog_name']}.{tbl['schema_name']}",
        f"Type: {tbl.get('table_type', 'MANAGED')}",
    ]
    if tbl.get("comment"):
        desc.append(f"Description: {tbl['comment']}")
    if tbl.get("data_source_format"):
        desc.append(f"Format: {tbl['data_source_format']}")
    if tbl.get("owner"):
        desc.append(f"Owner: {tbl['owner']}")
    if tbl.get("created"):
        desc.append(f"Created: {tbl['created']}")
    if tbl.get("last_altered"):
        desc.append(f"Last modified: {tbl['last_altered']}")

    # Tags
    tags = tbl.get("tags", [])
    if tags:
        desc.append(f"Table tags: " + ", ".join(f"{t['tag_name']}={t['tag_value']}" for t in tags))

    # Constraints
    for con in tbl.get("constraints", []):
        desc.append(f"{con['constraint_type']}: {', '.join(con.get('columns', []))}")

    # Lineage
    upstream = tbl.get("upstream", [])
    if upstream:
        desc.append(f"Upstream (feeds into this table): {', '.join(upstream[:10])}")
    downstream = tbl.get("downstream", [])
    if downstream:
        desc.append(f"Downstream (consumes this table): {', '.join(downstream[:10])}")

    # Privileges
    privileges = tbl.get("privileges", [])
    if privileges:
        priv_strs = [f"{p['grantee']}:{p['privilege_type']}" for p in privileges[:20]]
        desc.append(f"Access: {', '.join(priv_strs)}")

    # Columns with full detail
    col_descs = []
    for c in tbl.get("columns", []):
        parts = [f"  - {c['name']} ({c['type']})"]
        if c.get("comment"):
            parts.append(f": {c['comment']}")
        if c.get("column_default"):
            parts.append(f" [default: {c['column_default']}]")
        if c.get("numeric_precision"):
            parts.append(f" [precision: {c['numeric_precision']}]")
        if c.get("character_maximum_length") and str(c["character_maximum_length"]) != "0":
            parts.append(f" [max_len: {c['character_maximum_length']}]")
        col_tags = c.get("tags", [])
        if col_tags:
            parts.append(f" [tags: {', '.join(t['tag_name'] + '=' + t['tag_value'] for t in col_tags)}]")
        col_descs.append("".join(parts))
    desc.append("Columns:\n" + "\n".join(col_descs))

    return desc


def _run_sql(w, warehouse_id, statement, timeout=60):
    """Execute SQL via the Statements API."""
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=statement, wait_timeout="30s",
    )
    if resp.status.state.value == "FAILED":
        raise RuntimeError(f"SQL failed: {resp.status.error}")
    if resp.status.state.value == "SUCCEEDED":
        return resp
    stmt_id = resp.statement_id
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(2)
        resp = w.statement_execution.get_statement(stmt_id)
        if resp.status.state.value == "SUCCEEDED":
            return resp
        if resp.status.state.value == "FAILED":
            raise RuntimeError(f"SQL failed: {resp.status.error}")
    raise TimeoutError(f"SQL timed out after {timeout}s")


def _esc(s):
    """Escape single quotes for SQL."""
    if s is None:
        return ""
    return str(s).replace("\\", "\\\\").replace("'", "''")

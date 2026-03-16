"""UC Metadata Sync — Collect metadata from all UC tables and write to Delta + VS index.

Collects metadata from all accessible UC catalogs/schemas/tables using the Databricks SDK,
writes to enbridge_operations.uc_advisor.uc_metadata_docs Delta table via SQL Statements API,
and creates a Vector Search index on that table.

Run (fish shell):
  set -x DATABRICKS_CONFIG_PROFILE enbridge
  uv run --project /Users/allan.cao/Dev/uc-data-advisor \
    python src/notebooks/sync_uc_metadata.py
"""

import json
import os
import time
import hashlib

os.environ["DATABRICKS_CONFIG_PROFILE"] = "enbridge"

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    VectorIndexType,
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    PipelineType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CATALOG = "enbridge_operations"
SCHEMA = "uc_advisor"
TABLE = f"{CATALOG}.{SCHEMA}.uc_metadata_docs"
VS_INDEX = f"{CATALOG}.{SCHEMA}.uc_metadata_vs_index"
VS_ENDPOINT = "uc-advisor-vs"
WAREHOUSE_ID = "23bf2a865893d648"
EMBEDDING_MODEL = "databricks-bge-large-en"

# Catalogs to skip
SKIP_CATALOGS = {
    "system",
    "__databricks_internal",
    "samples",
    "information_schema",
    "image_gen_lakebase",  # Lakebase/Postgres foreign catalog, very slow to enumerate
}

# Schemas to skip (system/internal schemas)
SKIP_SCHEMAS = {
    "information_schema",
    "default",
    "pg_catalog",
    "__db_system",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

w = WorkspaceClient()


def run_sql(statement: str, timeout: int = 60):
    """Execute SQL via the Statements API and wait for result."""
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        wait_timeout="30s",
    )
    status = resp.status
    if status.state.value == "FAILED":
        raise RuntimeError(f"SQL failed: {status.error}")
    if status.state.value == "SUCCEEDED":
        return resp
    # Poll if still pending
    stmt_id = resp.statement_id
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(2)
        resp = w.statement_execution.get_statement(stmt_id)
        if resp.status.state.value == "SUCCEEDED":
            return resp
        if resp.status.state.value == "FAILED":
            raise RuntimeError(f"SQL failed: {resp.status.error}")
    raise TimeoutError(f"SQL statement {stmt_id} did not complete in {timeout}s")


def escape_sql(s: str) -> str:
    """Escape single quotes for SQL string literals."""
    if s is None:
        return ""
    return s.replace("\\", "\\\\").replace("'", "''")


# ---------------------------------------------------------------------------
# Step 1: Collect UC metadata via SDK
# ---------------------------------------------------------------------------

def collect_metadata() -> list[dict]:
    """Walk catalogs -> schemas -> tables and collect metadata."""
    rows = []
    catalogs = list(w.catalogs.list())
    print(f"Found {len(catalogs)} catalogs")

    for cat in catalogs:
        if cat.name in SKIP_CATALOGS or cat.name.startswith("__"):
            print(f"  Skipping catalog: {cat.name}")
            continue

        print(f"  Scanning catalog: {cat.name}")
        try:
            schemas = list(w.schemas.list(catalog_name=cat.name))
        except Exception as e:
            print(f"    ERROR listing schemas in {cat.name}: {e}")
            continue

        for schema in schemas:
            if schema.name in SKIP_SCHEMAS:
                continue

            full_schema = f"{cat.name}.{schema.name}"
            print(f"    Schema: {full_schema}")

            try:
                tables = list(w.tables.list(catalog_name=cat.name, schema_name=schema.name))
            except Exception as e:
                print(f"      ERROR listing tables in {full_schema}: {e}")
                continue

            for tbl in tables:
                full_name = tbl.full_name or f"{cat.name}.{schema.name}.{tbl.name}"

                # Get detailed table info only if list didn't include columns
                if tbl.columns:
                    detail = tbl
                else:
                    try:
                        detail = w.tables.get(full_name=full_name)
                    except Exception as e:
                        print(f"      WARN: Could not get details for {full_name}: {e}")
                        detail = tbl

                # Build column descriptions
                col_descriptions = []
                columns = detail.columns or []
                for col in columns:
                    type_text = col.type_text or (str(col.type_name) if col.type_name else "unknown")
                    col_desc = f"  - {col.name} ({type_text})"
                    if col.comment:
                        col_desc += f": {col.comment}"
                    col_descriptions.append(col_desc)

                columns_text = "\n".join(col_descriptions) if col_descriptions else "No column metadata available"

                # Build rich description text for embedding
                parts = [
                    f"Table: {full_name}",
                    f"Catalog: {cat.name}" + (f" -- {cat.comment}" if cat.comment else ""),
                    f"Schema: {full_schema}" + (f" -- {schema.comment}" if schema.comment else ""),
                ]
                if detail.comment:
                    parts.append(f"Description: {detail.comment}")
                if detail.table_type:
                    ttype = detail.table_type.value if hasattr(detail.table_type, "value") else str(detail.table_type)
                    parts.append(f"Type: {ttype}")
                if detail.data_source_format:
                    fmt = detail.data_source_format.value if hasattr(detail.data_source_format, "value") else str(detail.data_source_format)
                    parts.append(f"Format: {fmt}")
                parts.append(f"Columns:\n{columns_text}")

                description_text = "\n".join(parts)

                # Stable doc_id from full table name
                doc_id = hashlib.md5(full_name.encode()).hexdigest()[:16]

                row = {
                    "doc_id": doc_id,
                    "catalog_name": cat.name,
                    "schema_name": schema.name,
                    "table_name": tbl.name,
                    "full_table_name": full_name,
                    "table_comment": detail.comment or "",
                    "table_type": str(detail.table_type.value if detail.table_type and hasattr(detail.table_type, "value") else (detail.table_type or "")),
                    "columns_json": json.dumps(
                        [{"name": c.name,
                          "type": c.type_text or str(c.type_name or ""),
                          "comment": c.comment or ""}
                         for c in columns],
                    ),
                    "description_text": description_text,
                }
                rows.append(row)
                print(f"      {tbl.name} ({len(columns)} cols)")

    return rows


# ---------------------------------------------------------------------------
# Step 2: Write to Delta table via SQL Statements API
# ---------------------------------------------------------------------------

def write_to_delta(rows: list[dict]):
    """Create/replace the metadata Delta table and INSERT rows."""
    print(f"\n{'='*60}")
    print(f"Step 2: Writing {len(rows)} rows to {TABLE}")
    print(f"{'='*60}")

    # Ensure schema exists
    run_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    print(f"  Schema {CATALOG}.{SCHEMA} ensured")

    # Drop and recreate for clean state
    run_sql(f"DROP TABLE IF EXISTS {TABLE}")
    print(f"  Dropped existing table (if any)")

    run_sql(f"""
        CREATE TABLE {TABLE} (
            doc_id STRING NOT NULL,
            catalog_name STRING,
            schema_name STRING,
            table_name STRING,
            full_table_name STRING,
            table_comment STRING,
            table_type STRING,
            columns_json STRING,
            description_text STRING
        )
        USING DELTA
        COMMENT 'UC metadata documents for Vector Search -- auto-generated by sync_uc_metadata.py'
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true'
        )
    """)
    print(f"  Created table {TABLE} with Change Data Feed enabled")

    # Insert in batches
    BATCH_SIZE = 20
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        values_parts = []
        for r in batch:
            vals = (
                f"('{escape_sql(r['doc_id'])}', "
                f"'{escape_sql(r['catalog_name'])}', "
                f"'{escape_sql(r['schema_name'])}', "
                f"'{escape_sql(r['table_name'])}', "
                f"'{escape_sql(r['full_table_name'])}', "
                f"'{escape_sql(r['table_comment'])}', "
                f"'{escape_sql(r['table_type'])}', "
                f"'{escape_sql(r['columns_json'])}', "
                f"'{escape_sql(r['description_text'])}')"
            )
            values_parts.append(vals)

        insert_sql = f"INSERT INTO {TABLE} VALUES\n" + ",\n".join(values_parts)
        run_sql(insert_sql)
        print(f"  Inserted batch {i // BATCH_SIZE + 1}/{(len(rows) + BATCH_SIZE - 1) // BATCH_SIZE} ({len(batch)} rows)")

    # Verify count
    resp = run_sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}")
    count = resp.result.data_array[0][0] if resp.result and resp.result.data_array else "?"
    print(f"  Verified: {count} rows in {TABLE}")


# ---------------------------------------------------------------------------
# Step 3: Create Vector Search index via SDK
# ---------------------------------------------------------------------------

def create_vs_index():
    """Create a Delta Sync Vector Search index on the metadata table."""
    print(f"\n{'='*60}")
    print(f"Step 3: Creating Vector Search index {VS_INDEX}")
    print(f"{'='*60}")

    # Check if index already exists and wait for any pending deletion
    try:
        existing = w.vector_search_indexes.get_index(VS_INDEX)
        print(f"  Index already exists. Deleting to recreate...")
        w.vector_search_indexes.delete_index(VS_INDEX)
        print("  Delete requested. Waiting for deletion to complete...")
    except Exception:
        print("  No existing index found.")

    # Wait until the index is fully gone before creating
    for attempt in range(24):  # up to 2 minutes
        time.sleep(5)
        try:
            w.vector_search_indexes.get_index(VS_INDEX)
            print(f"    Still deleting... ({(attempt + 1) * 5}s)")
        except Exception:
            print("  Index fully deleted.")
            break
    else:
        print("  WARNING: Index deletion may not be complete yet, attempting create anyway...")

    # Create the index
    w.vector_search_indexes.create_index(
        name=VS_INDEX,
        endpoint_name=VS_ENDPOINT,
        primary_key="doc_id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=TABLE,
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[
                EmbeddingSourceColumn(
                    name="description_text",
                    embedding_model_endpoint_name=EMBEDDING_MODEL,
                ),
            ],
        ),
    )
    print(f"  Index creation initiated:")
    print(f"    Endpoint: {VS_ENDPOINT}")
    print(f"    Embedding model: {EMBEDDING_MODEL}")
    print(f"    Source table: {TABLE}")
    print(f"    Primary key: doc_id")

    # Poll for readiness (up to 10 minutes)
    print("  Waiting for index to become ready...")
    deadline = time.time() + 600
    while time.time() < deadline:
        time.sleep(15)
        try:
            idx = w.vector_search_indexes.get_index(VS_INDEX)
            status = idx.status
            if status and status.ready:
                print(f"  Index is READY!")
                return
            msg = status.message if status else "unknown"
            print(f"    ... {msg}")
        except Exception as e:
            print(f"    Polling error: {e}")

    print("  Index not ready after 10 minutes -- it will continue provisioning in the background.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("UC Metadata Sync")
    print("=" * 60)
    print(f"Workspace: {w.config.host}")
    print(f"Target table: {TABLE}")
    print(f"Target VS index: {VS_INDEX}")
    print(f"SQL Warehouse: {WAREHOUSE_ID}")
    print()

    # Step 1
    print("=" * 60)
    print("Step 1: Collecting UC metadata via SDK")
    print("=" * 60)
    rows = collect_metadata()
    print(f"\nCollected {len(rows)} table metadata documents")

    if not rows:
        print("ERROR: No tables found. Check catalog access permissions.")
        return

    # Step 2
    write_to_delta(rows)

    # Step 3
    create_vs_index()

    print(f"\n{'='*60}")
    print("UC Metadata Sync complete!")
    print(f"  Table: {TABLE} ({len(rows)} documents)")
    print(f"  VS Index: {VS_INDEX}")
    print("=" * 60)


if __name__ == "__main__":
    main()

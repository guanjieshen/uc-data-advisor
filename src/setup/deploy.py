"""Deploy generated artifacts to the Databricks workspace.

Creates Delta tables, VS indexes, metric views, materialized tables,
updates Genie Space, generates app.yaml, and deploys the app.
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

    # Step 6: Generate app.yaml and deploy app
    _deploy_app(config, w)

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

    # Create table
    _run_sql(w, warehouse_id, f"DROP TABLE IF EXISTS {table}")
    _run_sql(w, warehouse_id, f"""
        CREATE TABLE {table} (
            doc_id STRING NOT NULL,
            catalog_name STRING, schema_name STRING, table_name STRING,
            full_table_name STRING, table_comment STRING, table_type STRING,
            columns_json STRING, description_text STRING
        ) USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)

    # Insert rows in batches
    rows = []
    for tbl in audit.get("tables", []):
        col_descs = []
        for c in tbl.get("columns", []):
            desc = f"  - {c['name']} ({c['type']})"
            if c.get("comment"):
                desc += f": {c['comment']}"
            col_descs.append(desc)

        description_text = "\n".join([
            f"Table: {tbl['full_name']}",
            f"Catalog: {tbl['catalog_name']}",
            f"Schema: {tbl['catalog_name']}.{tbl['schema_name']}",
            f"Description: {tbl.get('comment', '')}",
            "Columns:\n" + "\n".join(col_descs),
        ])

        doc_id = hashlib.md5(tbl["full_name"].encode()).hexdigest()[:16]
        rows.append({
            "doc_id": doc_id,
            "catalog_name": tbl["catalog_name"],
            "schema_name": tbl["schema_name"],
            "table_name": tbl["name"],
            "full_table_name": tbl["full_name"],
            "table_comment": tbl.get("comment", ""),
            "table_type": tbl.get("table_type", ""),
            "columns_json": json.dumps([{"name": c["name"], "type": c["type"], "comment": c.get("comment", "")} for c in tbl.get("columns", [])]),
            "description_text": description_text,
        })

    # Batch insert
    BATCH = 20
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i+BATCH]
        values = ",\n".join(
            f"('{_esc(r['doc_id'])}', '{_esc(r['catalog_name'])}', '{_esc(r['schema_name'])}', "
            f"'{_esc(r['table_name'])}', '{_esc(r['full_table_name'])}', '{_esc(r['table_comment'])}', "
            f"'{_esc(r['table_type'])}', '{_esc(r['columns_json'])}', '{_esc(r['description_text'])}')"
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
        logger.warning(f"    Genie Space update failed: {e}")


# ---------------------------------------------------------------------------
# Step 6: App deployment
# ---------------------------------------------------------------------------

def _deploy_app(config, w):
    """Generate app.yaml from config and deploy the app."""
    infra = config.get("infrastructure", {})
    app_name = infra.get("app_name", "uc-data-advisor")
    lb = infra.get("lakebase", {})

    print("  [app] Generating app.yaml...")

    app_yaml = f"""command:
  - "python"
  - "-m"
  - "uvicorn"
  - "app:app"
  - "--host"
  - "0.0.0.0"
  - "--port"
  - "8000"

env:
  - name: SERVING_ENDPOINT
    value: {infra.get('serving_endpoint', 'uc-advisor-llm')}
  - name: GENIE_SPACE_ID
    value: "{infra.get('genie_space_id', '')}"
  - name: VS_INDEX_METADATA
    value: "{infra.get('vs_index_metadata', '')}"
  - name: VS_INDEX_KNOWLEDGE
    value: "{infra.get('vs_index_knowledge', '')}"
  - name: PGHOST
    value: "{lb.get('host', '')}"
  - name: PGPORT
    value: "{lb.get('port', 5432)}"
  - name: PGDATABASE
    value: "{lb.get('database', '')}"
  - name: PGUSER
    value: "{infra.get('app_sp_client_id', '')}"
  - name: LAKEBASE_INSTANCE
    value: "{lb.get('instance', '')}"
  - name: ADVISOR_CONFIG_PATH
    value: "config/advisor_config.yaml"
"""


    # Write app.yaml
    app_dir = os.path.join(os.path.dirname(__file__), "..", "..", "app")
    yaml_path = os.path.join(app_dir, "app.yaml")
    with open(yaml_path, "w") as f:
        f.write(app_yaml)
    print(f"    Written to {yaml_path}")

    import subprocess
    workspace = config.get("workspace", {})
    profile = workspace.get("profile", "")
    token = workspace.get("token", "")
    host = workspace.get("host", "")

    def _cli(cmd_args, description=""):
        cmd = ["databricks"] + cmd_args
        env = None
        if token and host:
            env = {**os.environ, "DATABRICKS_HOST": host, "DATABRICKS_TOKEN": token}
        elif profile:
            cmd += ["-p", profile]
        elif host:
            env = {**os.environ, "DATABRICKS_HOST": host}
        result = subprocess.run(cmd, capture_output=True, text=True, env=env)
        if result.returncode != 0:
            logger.warning(f"{description} failed: {result.stderr[:300]}")
        return result

    # Step 1: Create the app if it doesn't exist
    print(f"  [app] Creating app {app_name}...", end=" ", flush=True)
    result = _cli(["apps", "get", app_name], "app get")
    if result.returncode != 0:
        result = _cli([
            "apps", "create", app_name,
            "--description", f"UC Data Advisor — {infra.get('advisor_catalog', '')}",
        ], "app create")
        if result.returncode == 0:
            print("created")
        else:
            print(f"FAILED (create the app manually: databricks apps create {app_name})")
            return
    else:
        print("already exists")

    # Step 2: Upload app files to workspace
    deployer_user = w.current_user.me().user_name
    workspace_path = f"/Users/{deployer_user}/{app_name}"

    print(f"  [app] Uploading to {workspace_path}...")
    result = _cli(["workspace", "import-dir", app_dir, workspace_path, "--overwrite"], "app upload")
    if result.returncode != 0:
        return

    # Upload the specific config file as config/advisor_config.yaml
    # (matches the ADVISOR_CONFIG_PATH env var in app.yaml)
    config_src = os.path.abspath(config.get("_config_path", "config/advisor_config.yaml"))
    _cli(["workspace", "mkdirs", f"{workspace_path}/config"], "config dir")
    _cli(["workspace", "import", f"{workspace_path}/config/advisor_config.yaml", "--file", config_src, "--format", "AUTO", "--overwrite"], "config upload")

    # Step 3: Deploy the app
    print(f"  [app] Deploying {app_name}...")
    result = _cli(["apps", "deploy", app_name, "--source-code-path", f"/Workspace{workspace_path}"], "app deploy")
    if result.returncode == 0:
        try:
            resp = json.loads(result.stdout)
            state = resp.get("status", {}).get("state", "UNKNOWN")
            print(f"    {state}")
            # Print the app URL
            app_info = _cli(["apps", "get", app_name, "-o", "json"], "app get")
            if app_info.returncode == 0:
                try:
                    app_data = json.loads(app_info.stdout)
                    url = app_data.get("url", "")
                    if url:
                        print(f"    URL: {url}")
                except Exception:
                    pass
        except Exception:
            print("    Deployed")
    else:
        print(f"    Deploy failed — check: databricks apps get {app_name}")


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

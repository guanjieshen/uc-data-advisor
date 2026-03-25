"""Provision all Databricks infrastructure for the UC Data Advisor.

Auto-creates: catalog, schema, warehouse discovery, VS endpoint, Lakebase instance,
serving endpoint with AI Gateway, Genie Space, Databricks App, and permissions.

Idempotent — safe to re-run. Checks if each resource exists before creating.
"""

import json
import os
import time
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# System schemas/catalogs to skip
SYSTEM_SCHEMAS = {"information_schema", "default", "pg_catalog", "__db_system"}


def _derive_app_name(config: dict) -> str:
    """Derive a unique app name from catalog names if not explicitly set."""
    if config.get("app_name"):
        return config["app_name"]
    # Derive from source catalog common prefix
    catalogs = config.get("source_catalogs", [])
    if not catalogs:
        return "uc-data-advisor"
    # Find common prefix, e.g. ["enbridge_operations", "enbridge_commercial"] -> "enbridge"
    parts_list = [name.split("_") for name in catalogs]
    common = []
    for i, part in enumerate(parts_list[0]):
        if all(len(parts) > i and parts[i] == part for parts in parts_list):
            common.append(part)
        else:
            break
    prefix = "-".join(common) if common else catalogs[0].replace("_", "-")
    return f"{prefix}-advisor"


def provision(config: dict, w) -> dict:
    """Provision all infrastructure. Returns updated infrastructure dict."""
    infra = config.get("infrastructure", {})
    app_name = _derive_app_name(config)
    # All resource names are derived from app_name for uniqueness
    advisor_catalog = config.get("advisor_catalog", f"{app_name.replace('-', '_')}_catalog")
    serving_model = config.get("serving_model", "databricks-claude-opus-4-6")
    embedding_model = config.get("embedding_model", "databricks-bge-large-en")
    identity = config.get("app_identity", {"type": "service_principal", "name": ""})

    infra["app_name"] = app_name
    infra["advisor_catalog"] = advisor_catalog
    infra["advisor_schema"] = "default"

    print("=" * 60)
    print("UC Data Advisor — Infrastructure Provisioning")
    print("=" * 60)
    print(f"  App name:        {app_name}")
    print(f"  Advisor catalog: {advisor_catalog}")
    print(f"  App identity:    {identity['type']} ({identity['name']})")
    print()

    # Step 1: Discover warehouse
    infra["warehouse_id"] = _discover_warehouse(w, infra)

    # Step 2: Create advisor catalog + schema
    _create_catalog_and_schema(w, infra, config)

    # Step 3: Create Vector Search endpoint
    infra["vs_endpoint"] = _create_vs_endpoint(w, infra, app_name)

    # Step 4: Create Lakebase instance
    _create_lakebase(w, infra, app_name, identity)

    # Step 5: Create serving endpoint with AI Gateway
    infra["serving_endpoint"] = _create_serving_endpoint(w, infra, app_name, serving_model)
    infra["secret_scope"] = app_name

    # Step 6: Create Genie Space
    infra["genie_space_id"] = _create_genie_space(w, infra, app_name, config)

    # Step 7: Grant permissions
    _grant_permissions(w, infra, config, identity)

    print()
    print("=" * 60)
    print("Infrastructure provisioning complete")
    print("=" * 60)

    return infra


# ---------------------------------------------------------------------------
# Step 1: Discover warehouse
# ---------------------------------------------------------------------------

def _discover_warehouse(w, infra: dict) -> str:
    """Find first available serverless SQL warehouse."""
    existing = infra.get("warehouse_id", "")
    if existing:
        print(f"  [warehouse] Using existing: {existing}")
        return existing

    print("  [warehouse] Discovering...", end=" ", flush=True)
    warehouses = list(w.warehouses.list())
    for wh in warehouses:
        state_str = str(wh.state).upper()
        if "RUNNING" in state_str or "STOPPED" in state_str or "STARTING" in state_str:
            print(f"found: {wh.name} ({wh.id}, {wh.state})")
            return wh.id

    raise RuntimeError(
        "No SQL warehouse found. Create a serverless SQL warehouse in your workspace first."
    )


# ---------------------------------------------------------------------------
# Step 2: Create catalog + schema
# ---------------------------------------------------------------------------

def _resolve_storage_location(w, config: dict) -> str:
    """Resolve the storage URL for catalog creation.

    Priority:
    1. config.external_location — name or ID of a UC external location
    2. Auto-detect from source catalog's storage_root
    """
    ext_loc_name = config.get("external_location", "")
    if ext_loc_name:
        try:
            ext_loc = w.external_locations.get(ext_loc_name)
            url = ext_loc.url
            logger.info(f"Resolved external location '{ext_loc_name}' -> {url}")
            return url
        except Exception as e:
            logger.warning(f"Could not resolve external location '{ext_loc_name}': {e}")
            # Fall through to auto-detect

    # Auto-detect from source catalog
    source_catalogs = config.get("source_catalogs", [])
    for sc in source_catalogs:
        try:
            sc_info = w.catalogs.get(sc)
            if getattr(sc_info, "storage_root", None):
                return sc_info.storage_root
        except Exception:
            continue

    return ""


def _create_catalog_and_schema(w, infra: dict, config: dict = None) -> None:
    """Create the advisor catalog and default schema."""
    catalog = infra["advisor_catalog"]
    warehouse_id = infra["warehouse_id"]

    print(f"  [catalog] Creating {catalog}...", end=" ", flush=True)

    # Check if catalog already exists
    try:
        existing_catalogs = list(w.catalogs.list())
        for cat in existing_catalogs:
            if cat.name == catalog:
                print("already exists")
                return
    except Exception:
        pass

    # Resolve storage location for catalog creation
    # Priority: external_location config > auto-detect from source catalog
    storage_location = _resolve_storage_location(w, config or {})

    try:
        if storage_location:
            _run_sql(w, warehouse_id, f"CREATE CATALOG IF NOT EXISTS {catalog} MANAGED LOCATION '{storage_location}'")
        else:
            _run_sql(w, warehouse_id, f"CREATE CATALOG IF NOT EXISTS {catalog}")
        print("created")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("already exists")
        else:
            raise

    # Create default schema
    print(f"  [schema] Creating {catalog}.default...", end=" ", flush=True)
    try:
        _run_sql(w, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {catalog}.default")
        print("OK")
    except Exception:
        print("already exists")


# ---------------------------------------------------------------------------
# Step 3: Vector Search endpoint
# ---------------------------------------------------------------------------

def _create_vs_endpoint(w, infra: dict, app_name: str) -> str:
    """Create a Vector Search endpoint."""
    vs_name = infra.get("vs_endpoint", f"{app_name}-vs")

    print(f"  [vs-endpoint] Creating {vs_name}...", end=" ", flush=True)
    try:
        existing = w.vector_search_endpoints.get_endpoint(vs_name)
        if existing:
            print(f"already exists ({existing.endpoint_status.state})")
            return vs_name
    except Exception:
        pass

    try:
        from databricks.sdk.service.vectorsearch import EndpointType
        w.vector_search_endpoints.create_endpoint(name=vs_name, endpoint_type=EndpointType.STANDARD)
        print("created, waiting for ONLINE...", end=" ", flush=True)

        # Poll until online
        for _ in range(60):
            time.sleep(10)
            ep = w.vector_search_endpoints.get_endpoint(vs_name)
            if ep.endpoint_status.state == "ONLINE":
                print("ONLINE")
                return vs_name
        print("still provisioning (may take a few minutes)")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("already exists")
        else:
            raise

    return vs_name


# ---------------------------------------------------------------------------
# Step 4: Lakebase
# ---------------------------------------------------------------------------

def _create_lakebase(w, infra: dict, app_name: str, identity: dict) -> None:
    """Create Lakebase instance and database."""
    lb = infra.setdefault("lakebase", {})
    instance_name = lb.get("instance", f"{app_name}-sessions")
    lb["instance"] = instance_name
    lb["port"] = 5432

    # Check if instance exists
    print(f"  [lakebase] Creating instance {instance_name}...", end=" ", flush=True)
    try:
        existing = w.database.get_database_instance(instance_name)
        lb["host"] = existing.read_write_dns
        print(f"already exists ({existing.state})")
    except Exception:
        # Create instance
        try:
            from databricks.sdk.service.database import DatabaseInstance
            w.database.create_database_instance(
                database_instance=DatabaseInstance(name=instance_name, capacity="CU_1")
            )
            print("created, waiting for AVAILABLE...", end=" ", flush=True)
            for _ in range(60):
                time.sleep(10)
                inst = w.database.get_database_instance(instance_name)
                if str(inst.state) == "DatabaseInstanceState.AVAILABLE":
                    lb["host"] = inst.read_write_dns
                    print("AVAILABLE")
                    break
            else:
                inst = w.database.get_database_instance(instance_name)
                lb["host"] = inst.read_write_dns
                print(f"state: {inst.state}")
        except Exception as e:
            print(f"FAILED: {e}")
            return

    # Database name (underscores, not hyphens)
    db_name = instance_name.replace("-", "_")
    lb["database"] = db_name

    # Create database inside the instance
    _create_lakebase_database(w, lb, instance_name, db_name)

    # Add identity as instance role
    if identity["type"] == "service_principal" and identity["name"]:
        _add_lakebase_role(w, instance_name, identity["name"], "SERVICE_PRINCIPAL")


def _create_lakebase_database(w, lb: dict, instance_name: str, db_name: str) -> None:
    """Create a database inside the Lakebase instance via psql."""
    import subprocess

    print(f"  [lakebase] Creating database {db_name}...", end=" ", flush=True)
    try:
        cred = w.database.generate_database_credential(instance_names=[instance_name])
        deployer_user = w.current_user.me().user_name
        result = subprocess.run(
            ["psql",
             f"host={lb['host']} port={lb['port']} dbname=postgres "
             f"user={deployer_user} password={cred.token} sslmode=require",
             "-c", f"CREATE DATABASE {db_name}"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            print("created")
        elif "already exists" in result.stderr:
            print("already exists")
        else:
            print(f"warning: {result.stderr[:100]}")
    except FileNotFoundError:
        logger.warning("psql not found — create the database manually")
    except Exception as e:
        print(f"failed: {e}")


def _add_lakebase_role(w, instance_name: str, principal_name: str, identity_type: str) -> None:
    """Add a principal as a Lakebase instance role."""
    from databricks.sdk.service.database import DatabaseInstanceRole, DatabaseInstanceRoleIdentityType

    type_enum = (
        DatabaseInstanceRoleIdentityType.SERVICE_PRINCIPAL
        if identity_type == "SERVICE_PRINCIPAL"
        else DatabaseInstanceRoleIdentityType.USER
    )

    print(f"  [lakebase] Adding role {principal_name}...", end=" ", flush=True)
    try:
        w.database.create_database_instance_role(
            instance_name=instance_name,
            database_instance_role=DatabaseInstanceRole(
                name=principal_name,
                identity_type=type_enum,
            ),
        )
        print("added")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("already exists")
        else:
            print(f"failed: {e}")


# ---------------------------------------------------------------------------
# Step 5: Serving endpoint with AI Gateway
# ---------------------------------------------------------------------------

def _create_serving_endpoint(w, infra: dict, app_name: str, serving_model: str) -> str:
    """Create an external model endpoint with AI Gateway config."""
    ep_name = infra.get("serving_endpoint", f"{app_name}-llm")
    scope_name = app_name

    # Check if endpoint exists
    print(f"  [serving] Creating {ep_name}...", end=" ", flush=True)
    try:
        existing = w.serving_endpoints.get(ep_name)
        if existing:
            print(f"already exists ({existing.state.ready})")
            return ep_name
    except Exception:
        pass

    # Create secret scope + PAT
    print("creating secret scope...", end=" ", flush=True)
    try:
        w.secrets.create_scope(scope_name)
    except Exception:
        pass  # Already exists

    # Generate and store a PAT
    token_resp = w.tokens.create(comment=f"{app_name}-gateway", lifetime_seconds=0)
    token_value = token_resp.token_value

    # Store the PAT as a secret (use printf-style to avoid newline)
    w.secrets.put_secret(scope=scope_name, key="serving-token", string_value=token_value)
    print("secret stored...", end=" ", flush=True)

    # Create the endpoint
    api = w.api_client
    api.do("POST", "/api/2.0/serving-endpoints", body={
        "name": ep_name,
        "config": {
            "served_entities": [{
                "external_model": {
                    "name": serving_model,
                    "provider": "databricks-model-serving",
                    "task": "llm/v1/chat",
                    "databricks_model_serving_config": {
                        "databricks_workspace_url": w.config.host,
                        "databricks_api_token": f"{{{{secrets/{scope_name}/serving-token}}}}",
                    },
                },
            }],
        },
        "ai_gateway": {
            "usage_tracking_config": {"enabled": True},
            "rate_limits": [
                {"calls": 120, "key": "user", "renewal_period": "minute"},
                {"calls": 500, "key": "endpoint", "renewal_period": "minute"},
            ],
            "guardrails": {
                "input": {"safety": True, "pii": {"behavior": "NONE"}},
                "output": {"safety": False, "pii": {"behavior": "NONE"}},
            },
        },
        "tags": [
            {"key": "app", "value": app_name},
        ],
    })
    print("created (READY)")
    return ep_name


# ---------------------------------------------------------------------------
# Step 6: Genie Space
# ---------------------------------------------------------------------------

def _create_genie_space(w, infra: dict, app_name: str, config: dict) -> str:
    """Create a Genie Space."""
    existing_id = infra.get("genie_space_id", "")
    if existing_id:
        print(f"  [genie] Using existing space: {existing_id}")
        return existing_id

    domain = config.get("generated", {}).get("domain", {})
    org_name = domain.get("organization_name", "")
    title = f"{org_name} UC Data Advisor ({app_name})" if org_name else f"UC Data Advisor ({app_name})"

    print(f"  [genie] Creating space '{title}'...", end=" ", flush=True)
    import json as _json
    api = w.api_client
    resp = api.do("POST", "/api/2.0/genie/spaces", body={
        "title": title,
        "description": "UC Data Advisor Genie Space — source tables and pre-computed metrics for natural language queries.",
        "warehouse_id": infra["warehouse_id"],
        "serialized_space": _json.dumps({"version": 2, "data_sources": {"tables": []}}),
    })
    space_id = resp.get("space_id", "")
    print(f"created ({space_id})")
    return space_id


# ---------------------------------------------------------------------------
# Step 7: Permissions
# ---------------------------------------------------------------------------

def _grant_permissions(w, infra: dict, config: dict, identity: dict) -> None:
    """Grant permissions based on app identity type."""
    identity_type = identity.get("type", "service_principal")
    identity_name = identity.get("name", "")

    if not identity_name:
        print("  [permissions] No app_identity.name set, skipping grants")
        return

    source_catalogs = config.get("source_catalogs", [])
    advisor_catalog = infra["advisor_catalog"]
    warehouse_id = infra["warehouse_id"]

    if identity_type == "service_principal":
        _grant_sp_permissions(w, infra, identity_name, source_catalogs, advisor_catalog, warehouse_id)
    else:
        _print_user_grants(config, infra, identity_name, source_catalogs, advisor_catalog)


def _grant_sp_permissions(w, infra, sp_id, source_catalogs, advisor_catalog, warehouse_id):
    """Auto-grant all permissions to a service principal."""
    print(f"  [permissions] Granting SP {sp_id[:20]}... access")

    # UC grants on source catalogs
    for catalog in source_catalogs:
        for stmt in [
            f"GRANT USE CATALOG ON CATALOG {catalog} TO `{sp_id}`",
            f"GRANT SELECT ON CATALOG {catalog} TO `{sp_id}`",
        ]:
            try:
                _run_sql(w, warehouse_id, stmt)
            except Exception as e:
                logger.warning(f"Grant failed (may be OK): {e}")
        # Grant USE SCHEMA per-schema (wildcard not supported)
        try:
            schemas = list(w.schemas.list(catalog_name=catalog))
            for schema in schemas:
                if schema.name not in SYSTEM_SCHEMAS:
                    try:
                        _run_sql(w, warehouse_id, f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema.name} TO `{sp_id}`")
                    except Exception:
                        pass
        except Exception:
            pass

    # UC grants on advisor catalog
    try:
        _run_sql(w, warehouse_id, f"GRANT ALL PRIVILEGES ON CATALOG {advisor_catalog} TO `{sp_id}`")
    except Exception as e:
        logger.warning(f"Grant failed: {e}")

    # Serving endpoint permission
    ep_name = infra.get("serving_endpoint", "")
    if ep_name:
        try:
            # Get endpoint ID
            ep = w.serving_endpoints.get(ep_name)
            ep_id = ep.id
            w.api_client.do("PATCH", f"/api/2.0/permissions/serving-endpoints/{ep_id}", body={
                "access_control_list": [{
                    "service_principal_name": sp_id,
                    "permission_level": "CAN_QUERY",
                }],
            })
            print(f"    Granted CAN_QUERY on {ep_name}")
        except Exception as e:
            logger.warning(f"Serving endpoint permission failed: {e}")

    # Lakebase database grants via psql subprocess
    lb = infra.get("lakebase", {})
    if lb.get("host") and lb.get("database"):
        try:
            import subprocess
            cred = w.database.generate_database_credential(instance_names=[lb["instance"]])
            deployer_user = w.current_user.me().user_name
            grant_sql = "; ".join([
                f'GRANT ALL ON DATABASE {lb["database"]} TO "{sp_id}"',
                f'GRANT ALL ON SCHEMA public TO "{sp_id}"',
                f'GRANT ALL ON ALL TABLES IN SCHEMA public TO "{sp_id}"',
                f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "{sp_id}"',
            ])
            result = subprocess.run(
                ["psql", f"host={lb['host']} port={lb['port']} dbname={lb['database']} "
                 f"user={deployer_user} password={cred.token} sslmode=require",
                 "-c", grant_sql],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0:
                print(f"    Granted Lakebase access")
            else:
                logger.warning(f"Lakebase grants via psql: {result.stderr[:200]}")
        except FileNotFoundError:
            logger.warning("psql not found — Lakebase grants require manual application")
        except Exception as e:
            logger.warning(f"Lakebase grants failed (may need manual): {e}")

    print("    SP permissions complete")


def _print_user_grants(config, infra, user_name, source_catalogs, advisor_catalog):
    """Print required grants for a user identity."""
    ep_name = infra.get("serving_endpoint", "")
    vs_name = infra.get("vs_endpoint", "")
    lb = infra.get("lakebase", {})

    lines = [
        f"=== Required grants for app_identity user: {user_name} ===",
        "",
        "Unity Catalog (run as metastore admin or catalog owner):",
    ]

    for catalog in source_catalogs:
        lines.append(f"  GRANT USE CATALOG ON CATALOG {catalog} TO `{user_name}`;")
        lines.append(f"  GRANT SELECT ON CATALOG {catalog} TO `{user_name}`;")
        lines.append(f"  -- Grant USE SCHEMA on each schema in {catalog} as needed")

    lines.append(f"  GRANT ALL PRIVILEGES ON CATALOG {advisor_catalog} TO `{user_name}`;")
    lines.append("")

    if ep_name:
        lines.append("Serving endpoint:")
        lines.append(f'  Grant CAN_QUERY on endpoint "{ep_name}" via workspace UI or API')
        lines.append("")

    if vs_name:
        lines.append("Vector Search:")
        lines.append(f'  Grant CAN_USE on endpoint "{vs_name}" via workspace UI or API')
        lines.append("")

    if lb.get("instance"):
        lines.append("Lakebase:")
        lines.append(f'  Add user as instance role on "{lb["instance"]}"')
        lines.append(f'  GRANT ALL ON DATABASE {lb.get("database", "")} TO "{user_name}";')
        lines.append(f'  GRANT ALL ON SCHEMA public TO "{user_name}";')
        lines.append(f'  GRANT ALL ON ALL TABLES IN SCHEMA public TO "{user_name}";')

    grant_text = "\n".join(lines)
    print()
    print(grant_text)
    print()

    # Write to file
    output_dir = Path(config.get("_config_path", "config/advisor_config.yaml")).parent / ".generated"
    output_dir.mkdir(parents=True, exist_ok=True)
    grant_file = output_dir / "required_grants.sql"
    with open(grant_file, "w") as f:
        f.write(grant_text + "\n")
    print(f"  Grants saved to {grant_file}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_sql(w, warehouse_id: str, statement: str, timeout: int = 60):
    """Execute SQL via the Statements API."""
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="30s",
    )
    if resp.status.state.value == "FAILED":
        raise RuntimeError(f"SQL failed: {resp.status.error}")
    if resp.status.state.value == "SUCCEEDED":
        return resp
    # Poll if pending
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

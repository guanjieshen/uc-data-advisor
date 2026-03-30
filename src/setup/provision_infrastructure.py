"""Provision all Databricks infrastructure for the UC Data Advisor.

Auto-creates: catalog, schema, warehouse discovery, VS endpoint, Lakebase instance,
serving endpoint with AI Gateway, Genie Space, Databricks App, and permissions.

Idempotent — safe to re-run. Checks if each resource exists before creating.
"""

import json
import os
import time
import logging

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
    # Find common prefix, e.g. ["acme_operations", "acme_commercial"] -> "acme"
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
    advisor_catalog = config.get("advisor_catalog", f"{app_name.replace('-', '_')}_catalog")
    serving_model = config.get("serving_model", "databricks-claude-opus-4-6")

    infra["app_name"] = app_name
    infra["advisor_catalog"] = advisor_catalog
    infra["advisor_schema"] = "default"
    if config.get("warehouse_id"):
        infra["warehouse_id"] = config["warehouse_id"]

    print("=" * 60)
    print("UC Data Advisor — Infrastructure Provisioning")
    print("=" * 60)
    print(f"  App name:        {app_name}")
    print(f"  Advisor catalog: {advisor_catalog}")
    print()

    # Step 1: Discover warehouse
    infra["warehouse_id"] = _discover_warehouse(w, infra)

    # Step 2: Create advisor catalog + schema
    _create_catalog_and_schema(w, infra, config)

    # Step 3: Create Vector Search endpoint + set index names
    infra["vs_endpoint"] = _create_vs_endpoint(w, infra, app_name)
    infra["vs_index_metadata"] = f"{advisor_catalog}.default.uc_metadata_vs_index"
    infra["vs_index_knowledge"] = f"{advisor_catalog}.default.knowledge_vs_index"

    # Step 4: Create Databricks App (early — captures auto-created SP for grants)
    _create_app(w, infra, app_name, config)

    app_sp = infra.get("app_sp_client_id", "")
    if not app_sp:
        raise RuntimeError(
            "Databricks App did not return a service_principal_client_id. "
            "Check that the app was created successfully."
        )

    # Step 5: Create Lakebase instance
    _create_lakebase(w, infra, app_name)

    # Step 6: Add app SP to Lakebase + grant database permissions
    lb = infra.get("lakebase", {})
    if lb.get("instance"):
        _add_lakebase_role(w, lb["instance"], app_sp, "SERVICE_PRINCIPAL")
    grant_lakebase_permissions(config, w)

    # Step 7: Set LLM serving endpoint
    infra["serving_endpoint"] = serving_model
    print(f"  [serving] Using foundation model: {serving_model}")

    # Step 8: Create Genie Space
    infra["genie_space_id"] = _create_genie_space(w, infra, app_name, config)

    # Step 9: Create agent SP with OAuth secret for Model Serving auth
    # (Can't generate secrets for the app auto-SP — Databricks owns it.)
    _create_agent_sp_secrets(w, infra, app_name)

    # Step 10: Add agent SP to Lakebase + grant it Lakebase access
    agent_sp = infra.get("agent_sp_client_id", "")
    if agent_sp and lb.get("instance"):
        _add_lakebase_role(w, lb["instance"], agent_sp, "SERVICE_PRINCIPAL")
    grant_lakebase_permissions(config, w)

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
            if "ONLINE" in str(ep.endpoint_status.state).upper():
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

def _create_lakebase(w, infra: dict, app_name: str) -> None:
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



def _lakebase_execute(w, lb: dict, instance_name: str, statements: list[str], dbname: str = "postgres"):
    """Execute SQL statements against a Lakebase instance via asyncpg."""
    import asyncio
    import asyncpg
    import ssl

    cred = w.database.generate_database_credential(instance_names=[instance_name])
    deployer_user = w.current_user.me().user_name
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    async def _run():
        conn = await asyncpg.connect(
            host=lb["host"], port=int(lb.get("port", 5432)),
            database=dbname, user=deployer_user,
            password=cred.token, ssl=ssl_ctx,
        )
        for stmt in statements:
            try:
                await conn.execute(stmt)
            except Exception:
                pass
        await conn.close()

    asyncio.run(_run())


def _create_lakebase_database(w, lb: dict, instance_name: str, db_name: str) -> None:
    """Create a database inside the Lakebase instance."""
    print(f"  [lakebase] Creating database {db_name}...", end=" ", flush=True)
    try:
        _lakebase_execute(w, lb, instance_name, [f"CREATE DATABASE {db_name}"], dbname="postgres")
        print("created")
    except Exception as e:
        if "already exists" in str(e):
            print("already exists")
        else:
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

def _create_app(w, infra: dict, app_name: str, config: dict) -> None:
    """Create Databricks App and capture its auto-created SP."""
    import subprocess

    workspace = config.get("workspace", {})
    profile = workspace.get("profile", "")
    token = workspace.get("token", "")
    host = workspace.get("host", "")
    print(f"  [app] Creating app {app_name}...", end=" ", flush=True)

    def _cli(cmd_args):
        cmd = ["databricks"] + cmd_args
        env = None
        if token and host:
            env = {**os.environ, "DATABRICKS_HOST": host, "DATABRICKS_TOKEN": token}
        elif profile:
            cmd += ["-p", profile]
        elif host:
            env = {**os.environ, "DATABRICKS_HOST": host}
        return subprocess.run(cmd, capture_output=True, text=True, env=env)

    # Check if app exists
    result = _cli(["apps", "get", app_name, "-o", "json"])
    if result.returncode == 0:
        try:
            app_data = json.loads(result.stdout)
            sp_id = app_data.get("service_principal_client_id", "")
            if sp_id:
                infra["app_sp_client_id"] = sp_id
                print(f"already exists (SP: {sp_id[:20]}...)")
            else:
                print("already exists")
        except Exception:
            print("already exists")
        return

    # Create app
    result = _cli(["apps", "create", app_name, "--description",
                    f"UC Data Advisor — {infra.get('advisor_catalog', '')}"])
    if result.returncode == 0:
        # Fetch the created app to get its SP
        result2 = _cli(["apps", "get", app_name, "-o", "json"])
        if result2.returncode == 0:
            try:
                app_data = json.loads(result2.stdout)
                sp_id = app_data.get("service_principal_client_id", "")
                infra["app_sp_client_id"] = sp_id
                print(f"created (SP: {sp_id[:20]}...)")
            except Exception:
                print("created")
        else:
            print("created")
    else:
        print(f"FAILED: {result.stderr[:200]}")


def _create_agent_sp_secrets(w, infra: dict, app_name: str) -> None:
    """Create an SP for Model Serving auth, generate OAuth secret, store in secret scope.

    The app's auto-SP can't have secrets generated (owned by Databricks).
    This creates a deployer-owned SP that agent endpoints authenticate as.
    """
    sp_name = f"{app_name}-agent-sp"
    scope_name = infra.get("secret_scope", app_name)
    infra["secret_scope"] = scope_name

    # Check if already done
    if infra.get("sp_secrets_stored"):
        print(f"  [agent-sp] Using existing: {infra.get('agent_sp_client_id', '')[:20]}...")
        return

    print(f"  [agent-sp] Creating {sp_name}...", end=" ", flush=True)

    # Find or create the SP
    sp_id = None
    app_id = None
    try:
        for sp in w.service_principals.list():
            if sp.display_name == sp_name:
                sp_id = sp.id
                app_id = sp.application_id
                print(f"exists ({app_id})", end=" ", flush=True)
                break
    except Exception:
        pass

    if not sp_id:
        try:
            sp = w.service_principals.create(display_name=sp_name)
            sp_id = sp.id
            app_id = sp.application_id
            print(f"created ({app_id})", end=" ", flush=True)
        except Exception as e:
            print(f"FAILED: {e}")
            return

    infra["agent_sp_client_id"] = app_id

    # Grant workspace-access entitlement (required for outbound API calls)
    try:
        from databricks.sdk.service.iam import PatchOp, Patch, PatchSchema
        w.service_principals.patch(
            id=str(sp_id),
            operations=[Patch(op=PatchOp.ADD, path="entitlements", value=[{"value": "workspace-access"}])],
            schemas=[PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
        )
        print("entitled", end=" ", flush=True)
    except Exception as e:
        logger.warning(f"Failed to set workspace-access entitlement: {e}")

    # Generate OAuth secret
    try:
        secret_resp = w.service_principal_secrets_proxy.create(service_principal_id=sp_id)
    except Exception as e:
        print(f"secret failed: {e}")
        return

    # Create secret scope (idempotent)
    try:
        w.secrets.create_scope(scope=scope_name)
        print("scope created", end=" ", flush=True)
    except Exception as e:
        if "already exists" in str(e).lower():
            print("scope exists", end=" ", flush=True)
        else:
            print(f"scope failed: {e}")
            return

    # Store client_id + secret in scope
    try:
        w.secrets.put_secret(scope=scope_name, key="sp-client-id", string_value=app_id)
        w.secrets.put_secret(scope=scope_name, key="sp-client-secret", string_value=secret_resp.secret)
        infra["sp_secrets_stored"] = True
        print("secrets stored")
    except Exception as e:
        print(f"put_secret failed: {e}")


def _get_sp_list(config: dict) -> set[str]:
    """Return all SPs that need grants (app auto-SP + agent SP)."""
    infra = config.get("infrastructure", {})
    sp_list = set()
    for key in ["app_sp_client_id", "agent_sp_client_id"]:
        sp = infra.get(key, "")
        if sp:
            sp_list.add(sp)
    return sp_list


def grant_uc_permissions(config: dict, w) -> None:
    """Grant UC catalog/schema permissions to the app SP. Run before metadata audit."""
    infra = config.get("infrastructure", {})
    source_catalogs = config.get("source_catalogs", [])
    advisor_catalog = infra.get("advisor_catalog", "")
    warehouse_id = infra.get("warehouse_id", "")

    sp_list = _get_sp_list(config)
    if not sp_list:
        print("  [permissions] No app SP found — skipping UC grants")
        return

    print("  [permissions] Granting UC access...")
    for sp_id in sp_list:
        print(f"    Principal: {sp_id}")
        for catalog in source_catalogs:
            for stmt in [
                f"GRANT USE CATALOG ON CATALOG {catalog} TO `{sp_id}`",
                f"GRANT SELECT ON CATALOG {catalog} TO `{sp_id}`",
            ]:
                try:
                    _run_sql(w, warehouse_id, stmt)
                    print(f"      {stmt}")
                except Exception:
                    pass
            try:
                for schema in w.schemas.list(catalog_name=catalog):
                    if schema.name not in SYSTEM_SCHEMAS:
                        stmt = f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema.name} TO `{sp_id}`"
                        try:
                            _run_sql(w, warehouse_id, stmt)
                            print(f"      {stmt}")
                        except Exception:
                            pass
            except Exception:
                pass
        stmt = f"GRANT ALL PRIVILEGES ON CATALOG {advisor_catalog} TO `{sp_id}`"
        try:
            _run_sql(w, warehouse_id, stmt)
            print(f"      {stmt}")
        except Exception:
            pass
    # Grant CAN_USE on SQL warehouse
    if warehouse_id:
        for sp_id in sp_list:
            try:
                w.api_client.do("PATCH", f"/api/2.0/permissions/sql/warehouses/{warehouse_id}", body={
                    "access_control_list": [
                        {"service_principal_name": sp_id, "permission_level": "CAN_USE"}
                    ]
                })
                print(f"      CAN_USE on warehouse {warehouse_id} → {sp_id}")
            except Exception:
                pass

    # Grant CAN_RUN on Genie Space
    genie_space_id = infra.get("genie_space_id", "")
    if genie_space_id:
        for sp_id in sp_list:
            try:
                w.api_client.do("PATCH", f"/api/2.0/permissions/genie/{genie_space_id}", body={
                    "access_control_list": [
                        {"service_principal_name": sp_id, "permission_level": "CAN_RUN"}
                    ]
                })
                print(f"      CAN_RUN on Genie space → {sp_id}")
            except Exception:
                pass

    print("    UC grants complete")


def grant_lakebase_permissions(config: dict, w) -> None:
    """Grant Lakebase database permissions. Run after Lakebase instance is available."""
    infra = config.get("infrastructure", {})
    lb = infra.get("lakebase", {})

    if not lb.get("host") or not lb.get("database"):
        return

    print("  [permissions] Granting Lakebase access...")

    for sp_id in _get_sp_list(config):
        stmts = [
            f'GRANT ALL ON DATABASE {lb["database"]} TO "{sp_id}"',
            f'GRANT ALL ON SCHEMA public TO "{sp_id}"',
            f'GRANT ALL ON ALL TABLES IN SCHEMA public TO "{sp_id}"',
            f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "{sp_id}"',
        ]
        try:
            _lakebase_execute(w, lb, lb["instance"], stmts, dbname=lb["database"])
            print(f"    Principal: {sp_id}")
            for stmt in stmts:
                print(f"      {stmt}")
        except Exception as e:
            logger.warning(f"Lakebase grants failed for {sp_id[:20]}: {e}")


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

"""Provision all Databricks infrastructure for the UC Data Advisor.

Auto-creates: catalog, schema, warehouse discovery, VS endpoint,
Genie Space, and SP OAuth secrets.

Idempotent — safe to re-run. Checks if each resource exists before creating.
"""

import time
import logging

logger = logging.getLogger(__name__)

# System schemas/catalogs to skip
SYSTEM_SCHEMAS = {"information_schema", "default", "pg_catalog", "__db_system"}


def _derive_app_name(config: dict) -> str:
    """Derive a unique deployment name from catalog names if not explicitly set."""
    if config.get("app_name"):
        return config["app_name"]
    catalogs = config.get("source_catalogs", [])
    if not catalogs:
        return "uc-data-advisor"
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
    sp = config.get("service_principal", "")

    if not sp:
        raise ValueError(
            "service_principal is required in config. "
            "Provide the application (client) ID of a service principal you own."
        )

    infra["app_name"] = app_name
    infra["advisor_catalog"] = advisor_catalog
    infra["advisor_schema"] = "default"
    if config.get("warehouse_id"):
        infra["warehouse_id"] = config["warehouse_id"]

    print("=" * 60)
    print("UC Data Advisor — Infrastructure Provisioning")
    print("=" * 60)
    print(f"  Deployment:      {app_name}")
    print(f"  Advisor catalog: {advisor_catalog}")
    print(f"  Service principal: {sp}")
    print()

    # Step 1: Discover warehouse
    infra["warehouse_id"] = _discover_warehouse(w, infra)

    # Step 2: Create advisor catalog + schema
    _create_catalog_and_schema(w, infra, config)

    # Step 3: Create Vector Search endpoint + set index names
    infra["vs_endpoint"] = _create_vs_endpoint(w, infra, app_name)
    infra["vs_index_metadata"] = f"{advisor_catalog}.default.uc_metadata_vs_index"
    infra["vs_index_knowledge"] = f"{advisor_catalog}.default.knowledge_vs_index"

    # Step 4: Set LLM serving endpoint
    infra["serving_endpoint"] = serving_model
    print(f"  [serving] Using foundation model: {serving_model}")

    # Step 5: Create Genie Space
    infra["genie_space_id"] = _create_genie_space(w, infra, app_name, config)

    # Step 6: Generate OAuth secret for configured SP + store in secret scope
    _store_sp_secrets(w, infra, sp)

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
    """Resolve the storage URL for catalog creation."""
    ext_loc_name = config.get("external_location", "")
    if ext_loc_name:
        try:
            ext_loc = w.external_locations.get(ext_loc_name)
            return ext_loc.url
        except Exception:
            pass

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

    try:
        existing_catalogs = list(w.catalogs.list())
        for cat in existing_catalogs:
            if cat.name == catalog:
                print("already exists")
                return
    except Exception:
        pass

    storage_location = _resolve_storage_location(w, config or {})

    try:
        if storage_location:
            try:
                _run_sql(w, warehouse_id, f"CREATE CATALOG IF NOT EXISTS {catalog} MANAGED LOCATION '{storage_location}'")
            except Exception:
                _run_sql(w, warehouse_id, f"CREATE CATALOG IF NOT EXISTS {catalog}")
        else:
            _run_sql(w, warehouse_id, f"CREATE CATALOG IF NOT EXISTS {catalog}")
        print("created")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("already exists")
        else:
            raise

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
# Step 5: Genie Space
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
# Step 6: SP OAuth secrets
# ---------------------------------------------------------------------------

def _store_sp_secrets(w, infra: dict, sp_client_id: str) -> None:
    """Generate OAuth secret for the configured SP and store in a Databricks secret scope."""
    app_name = infra.get("app_name", "uc-data-advisor")
    scope_name = infra.get("secret_scope", app_name)
    infra["secret_scope"] = scope_name

    if infra.get("sp_secrets_stored"):
        print(f"  [secrets] Using existing scope: {scope_name}")
        return

    print(f"  [secrets] Setting up OAuth for SP {sp_client_id[:20]}...", end=" ", flush=True)

    # Look up the SP to get its internal numeric ID
    sp_id = None
    try:
        for sp in w.service_principals.list():
            if sp.application_id == sp_client_id:
                sp_id = sp.id
                break
    except Exception as e:
        print(f"FAILED to list SPs: {e}")
        return

    if not sp_id:
        print(f"FAILED: SP '{sp_client_id}' not found in workspace")
        return

    print(f"found (id={sp_id})", end=" ", flush=True)

    # Grant workspace-access entitlement
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
        w.secrets.put_secret(scope=scope_name, key="sp-client-id", string_value=sp_client_id)
        w.secrets.put_secret(scope=scope_name, key="sp-client-secret", string_value=secret_resp.secret)
        infra["sp_secrets_stored"] = True
        print("secrets stored", end=" ", flush=True)
    except Exception as e:
        print(f"put_secret failed: {e}")
        return

    # Grant READ on scope to the configured SP (for runtime secret resolution)
    try:
        from databricks.sdk.service.workspace import AclPermission
        w.secrets.put_acl(scope=scope_name, principal=sp_client_id, permission=AclPermission.READ)
        print("ACL granted")
    except Exception as e:
        logger.warning(f"Failed to grant scope ACL: {e}")
        print("(ACL grant skipped)")


# ---------------------------------------------------------------------------
# Grants
# ---------------------------------------------------------------------------

def _get_sp_list(config: dict) -> set[str]:
    """Return the configured service principal."""
    sp = config.get("service_principal", "")
    return {sp} if sp else set()


def grant_uc_permissions(config: dict, w) -> None:
    """Grant UC catalog/schema/warehouse/Genie permissions to the configured SP."""
    infra = config.get("infrastructure", {})
    source_catalogs = config.get("source_catalogs", [])
    advisor_catalog = infra.get("advisor_catalog", "")
    warehouse_id = infra.get("warehouse_id", "")

    sp_list = _get_sp_list(config)
    if not sp_list:
        print("  [permissions] No service_principal configured — skipping grants")
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

    print("    Grants complete")


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

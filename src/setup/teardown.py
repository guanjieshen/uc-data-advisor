"""Tear down all Databricks resources created by the UC Data Advisor setup pipeline.

Deletes: agent endpoints, registered models, VS indexes, Genie spaces,
secret scope, agent SP, Databricks App, Lakebase instance, VS endpoint,
and advisor catalog.

Usage:
  uv run python -m src.setup.run --config config/my_config.yaml --step teardown
"""

import json
import subprocess
import logging

logger = logging.getLogger(__name__)


def teardown(config: dict, w) -> None:
    """Delete all resources for this deployment."""
    infra = config.get("infrastructure", {})
    app_name = infra.get("app_name", "")
    catalog = infra.get("advisor_catalog", "")
    warehouse_id = infra.get("warehouse_id", "")
    workspace = config.get("workspace", {})
    profile = workspace.get("profile", "")

    if not app_name:
        print("  No app_name in infrastructure config — nothing to tear down")
        return

    print("=" * 60)
    print("UC Data Advisor — Full Teardown")
    print("=" * 60)
    print(f"  App name: {app_name}")
    print(f"  Catalog:  {catalog}")
    print()

    # 1. Delete agent deployments + endpoints
    _teardown_agent_endpoints(infra, catalog, app_name)

    # 2. Delete VS indexes
    _teardown_vs_indexes(w, catalog)

    # 3. Delete Genie spaces
    _teardown_genie_spaces(w, app_name)

    # 4. Delete secret scope
    _teardown_secret_scope(w, infra, app_name)

    # 5. Delete agent SP
    _teardown_agent_sp(w, app_name)

    # 6. Delete Databricks App
    _teardown_app(w, app_name, profile, workspace)

    # 7. Delete Lakebase instance
    _teardown_lakebase(w, infra)

    # 8. Delete VS endpoint
    _teardown_vs_endpoint(w, infra)

    # 9. Drop advisor catalog
    _teardown_catalog(w, catalog, warehouse_id)

    print()
    print("=" * 60)
    print("Teardown complete")
    print("=" * 60)


def _teardown_agent_endpoints(infra: dict, catalog: str, app_name: str) -> None:
    """Delete agent deployments and serving endpoints."""
    from databricks import agents

    print("  [1/9] Agent endpoints")
    for agent in ["discovery", "metrics", "qa", "orchestrator"]:
        model_name = f"{catalog}.default.{app_name.replace('-', '_')}_{agent}_agent"
        ep_name = f"{app_name}-{agent}-agent"
        try:
            agents.delete_deployment(model_name=model_name)
            print(f"    Deleted deployment: {model_name}")
        except Exception as e:
            print(f"    Deployment {agent}: {_short_err(e)}")
        try:
            from databricks.sdk import WorkspaceClient
            WorkspaceClient().serving_endpoints.delete(ep_name)
            print(f"    Deleted endpoint: {ep_name}")
        except Exception as e:
            print(f"    Endpoint {agent}: {_short_err(e)}")


def _teardown_vs_indexes(w, catalog: str) -> None:
    """Delete Vector Search indexes."""
    print("  [2/9] Vector Search indexes")
    for idx in ["uc_metadata_vs_index", "knowledge_vs_index"]:
        fqn = f"{catalog}.default.{idx}"
        try:
            w.vector_search_indexes.delete_index(fqn)
            print(f"    Deleted: {fqn}")
        except Exception as e:
            print(f"    {idx}: {_short_err(e)}")


def _teardown_genie_spaces(w, app_name: str) -> None:
    """Delete all Genie spaces matching the app name."""
    print("  [3/9] Genie spaces")
    try:
        resp = w.api_client.do("GET", "/api/2.0/genie/spaces")
        deleted = 0
        for space in resp.get("spaces", []):
            title = space.get("title", "").lower()
            if app_name in title or "uc data advisor" in title:
                try:
                    w.api_client.do("DELETE", f"/api/2.0/genie/spaces/{space['space_id']}")
                    print(f"    Deleted: {space['title']}")
                    deleted += 1
                except Exception as e:
                    print(f"    Failed: {space['title']}: {_short_err(e)}")
        if deleted == 0:
            print("    None found")
    except Exception as e:
        print(f"    {_short_err(e)}")


def _teardown_secret_scope(w, infra: dict, app_name: str) -> None:
    """Delete the secret scope."""
    print("  [4/9] Secret scope")
    scope = infra.get("secret_scope", app_name)
    try:
        w.secrets.delete_scope(scope=scope)
        print(f"    Deleted scope: {scope}")
    except Exception as e:
        print(f"    {_short_err(e)}")


def _teardown_agent_sp(w, app_name: str) -> None:
    """Delete the agent service principal."""
    print("  [5/9] Agent SP")
    sp_name = f"{app_name}-agent-sp"
    try:
        for sp in w.service_principals.list():
            if sp.display_name == sp_name:
                w.service_principals.delete(id=sp.id)
                print(f"    Deleted: {sp_name} ({sp.application_id})")
                return
        print("    Not found")
    except Exception as e:
        print(f"    {_short_err(e)}")


def _teardown_app(w, app_name: str, profile: str, workspace: dict) -> None:
    """Delete the Databricks App."""
    print("  [6/9] Databricks App")
    token = workspace.get("token", "")
    host = workspace.get("host", "")

    cmd = ["databricks", "apps", "delete", app_name]
    env = None
    if token and host:
        import os
        env = {**os.environ, "DATABRICKS_HOST": host, "DATABRICKS_TOKEN": token}
    elif profile:
        cmd += ["-p", profile]
    elif host:
        import os
        env = {**os.environ, "DATABRICKS_HOST": host}

    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode == 0:
        print(f"    Deleted app: {app_name}")
    else:
        print(f"    {result.stderr.strip()[:100] or 'not found'}")


def _teardown_lakebase(w, infra: dict) -> None:
    """Delete the Lakebase instance."""
    print("  [7/9] Lakebase")
    lb = infra.get("lakebase", {})
    instance = lb.get("instance", "")
    if not instance:
        print("    Not configured")
        return
    try:
        w.database.delete_database_instance(instance)
        print(f"    Deleted instance: {instance}")
    except Exception as e:
        print(f"    {_short_err(e)}")


def _teardown_vs_endpoint(w, infra: dict) -> None:
    """Delete the Vector Search endpoint."""
    print("  [8/9] Vector Search endpoint")
    vs_ep = infra.get("vs_endpoint", "")
    if not vs_ep:
        print("    Not configured")
        return
    try:
        w.vector_search_endpoints.delete_endpoint(vs_ep)
        print(f"    Deleted: {vs_ep}")
    except Exception as e:
        print(f"    {_short_err(e)}")


def _teardown_catalog(w, catalog: str, warehouse_id: str) -> None:
    """Drop the advisor catalog."""
    print("  [9/9] Advisor catalog")
    if not catalog or not warehouse_id:
        print("    Not configured")
        return
    try:
        w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"DROP CATALOG IF EXISTS {catalog} CASCADE",
            wait_timeout="30s",
        )
        print(f"    Dropped catalog: {catalog}")
    except Exception as e:
        print(f"    {_short_err(e)}")


def _short_err(e: Exception) -> str:
    """Return a short error message."""
    msg = str(e)
    if "does not exist" in msg.lower() or "not found" in msg.lower():
        return "not found"
    return msg[:120]

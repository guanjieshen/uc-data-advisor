"""Tear down all Databricks resources created by the UC Data Advisor setup pipeline.

Deletes: agent endpoints, registered models, VS indexes, Genie spaces,
secret scope, VS endpoint, and advisor catalog.

Usage:
  uv run python -m src.setup.run --config config/my_config.yaml --step teardown
"""

import logging

logger = logging.getLogger(__name__)


def teardown(config: dict, w) -> None:
    """Delete all resources for this deployment."""
    infra = config.get("infrastructure", {})
    app_name = infra.get("app_name", "")
    catalog = infra.get("advisor_catalog", "")
    warehouse_id = infra.get("warehouse_id", "")

    if not app_name:
        print("  No app_name in infrastructure config — nothing to tear down")
        return

    print("=" * 60)
    print("UC Data Advisor — Full Teardown")
    print("=" * 60)
    print(f"  Deployment: {app_name}")
    print(f"  Catalog:    {catalog}")
    print()

    # 1. Delete agent deployments + endpoints
    _teardown_agent_endpoints(infra, catalog, app_name)

    # 2. Delete VS indexes
    _teardown_vs_indexes(w, catalog)

    # 3. Delete Genie spaces
    _teardown_genie_spaces(w, app_name)

    # 4. Delete secret scope
    _teardown_secret_scope(w, infra, app_name)

    # 5. Delete VS endpoint
    _teardown_vs_endpoint(w, infra)

    # 6. Drop advisor catalog
    _teardown_catalog(w, catalog, warehouse_id)

    # Clear infrastructure state so next deploy starts fresh
    config["infrastructure"] = {}
    config.pop("generated", None)

    print()
    print("=" * 60)
    print("Teardown complete")
    print("=" * 60)


def _teardown_agent_endpoints(infra: dict, catalog: str, app_name: str) -> None:
    """Delete agent serving endpoints."""
    from databricks.sdk import WorkspaceClient

    print("  [1/6] Agent endpoints")
    w = WorkspaceClient()
    for agent in ["discovery", "metrics", "qa", "orchestrator"]:
        ep_name = f"{app_name}-{agent}-agent"
        try:
            w.serving_endpoints.delete(ep_name)
            print(f"    Deleted endpoint: {ep_name}")
        except Exception as e:
            print(f"    Endpoint {agent}: {_short_err(e)}")


def _teardown_vs_indexes(w, catalog: str) -> None:
    """Delete Vector Search indexes."""
    print("  [2/6] Vector Search indexes")
    for idx in ["uc_metadata_vs_index", "knowledge_vs_index"]:
        fqn = f"{catalog}.default.{idx}"
        try:
            w.vector_search_indexes.delete_index(fqn)
            print(f"    Deleted: {fqn}")
        except Exception as e:
            print(f"    {idx}: {_short_err(e)}")


def _teardown_genie_spaces(w, app_name: str) -> None:
    """Delete all Genie spaces matching the deployment name."""
    print("  [3/6] Genie spaces")
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
    print("  [4/6] Secret scope")
    scope = infra.get("secret_scope", app_name)
    try:
        w.secrets.delete_scope(scope=scope)
        print(f"    Deleted scope: {scope}")
    except Exception as e:
        print(f"    {_short_err(e)}")


def _teardown_vs_endpoint(w, infra: dict) -> None:
    """Delete the Vector Search endpoint."""
    print("  [5/6] Vector Search endpoint")
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
    print("  [6/6] Advisor catalog")
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

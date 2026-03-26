"""Service principal authentication for all Databricks platform calls.

Remote (Databricks App): Uses auto-injected app SPN credentials.
Local: Uses OAuth M2M with the uc-data-advisor SPN via env vars
       DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET.
"""

import os
from databricks.sdk import WorkspaceClient

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))
IS_MODEL_SERVING = bool(os.environ.get("DATABRICKS_SERVING_ENDPOINT"))


def get_workspace_client() -> WorkspaceClient:
    """Get WorkspaceClient authenticated as a service principal."""
    if IS_DATABRICKS_APP or IS_MODEL_SERVING:
        # Auto-injected SPN credentials (App or Model Serving)
        return WorkspaceClient()

    # Local dev: OAuth M2M with SPN credentials from env vars
    host = os.environ.get("DATABRICKS_HOST")
    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")

    if not all([host, client_id, client_secret]):
        raise RuntimeError(
            "Local dev requires DATABRICKS_HOST, DATABRICKS_CLIENT_ID, "
            "and DATABRICKS_CLIENT_SECRET env vars for SPN auth"
        )

    return WorkspaceClient(
        host=host,
        client_id=client_id,
        client_secret=client_secret,
    )


def get_oauth_token() -> str:
    """Get OAuth token from the SPN-authenticated client."""
    client = get_workspace_client()
    auth_headers = client.config.authenticate()
    if auth_headers and "Authorization" in auth_headers:
        return auth_headers["Authorization"].replace("Bearer ", "")
    raise RuntimeError("Failed to get OAuth token from SPN")


def get_workspace_host() -> str:
    """Get workspace host URL with https:// prefix."""
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        return host

    host = os.environ.get("DATABRICKS_HOST", "")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    return host

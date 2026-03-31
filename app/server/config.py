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
    """Get WorkspaceClient with automatic credential detection."""
    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")

    # 1. OAuth M2M with explicit SP credentials (Model Serving with secret scope)
    if host and client_id and client_secret:
        return WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)

    # 2. Databricks App or Model Serving — auto-injected credentials
    if IS_DATABRICKS_APP or IS_MODEL_SERVING:
        return WorkspaceClient()

    # 3. Explicit host + token from env
    if host and token:
        return WorkspaceClient(host=host, token=token)

    # 4. Default SDK auth (CLI profile, env vars, etc.)
    return WorkspaceClient()


def get_oauth_token() -> str:
    """Get OAuth token from the SPN-authenticated client."""
    client = get_workspace_client()
    auth_headers = client.config.authenticate()
    if auth_headers and "Authorization" in auth_headers:
        return auth_headers["Authorization"].replace("Bearer ", "")
    raise RuntimeError("Failed to get OAuth token from SPN")


def get_workspace_host() -> str:
    """Get workspace host URL with https:// prefix."""
    host = os.environ.get("DATABRICKS_HOST", "")

    # Fall back to SDK config host (resolves from profile, env, etc.)
    if not host:
        try:
            host = get_workspace_client().config.host or ""
        except Exception:
            pass

    if host and not host.startswith("http"):
        host = f"https://{host}"
    return host

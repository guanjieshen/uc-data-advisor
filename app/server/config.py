"""Service principal authentication for all Databricks platform calls.

Model Serving: Uses OAuth M2M with SP credentials injected as env vars at deploy time.
  Credentials are stored in a Databricks secret scope; the deploy pipeline reads them
  and passes them as DATABRICKS_CLIENT_ID/SECRET env vars.
Local: Uses OAuth M2M, PAT, or CLI profile via env vars.
"""

import os
from databricks.sdk import WorkspaceClient

IS_MODEL_SERVING = bool(os.environ.get("DATABRICKS_SERVING_ENDPOINT"))


def get_workspace_client() -> WorkspaceClient:
    """Get WorkspaceClient with automatic credential detection."""
    host = os.environ.get("DATABRICKS_HOST", "")
    client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")

    # 1. Model Serving — use SP credentials from env vars
    if IS_MODEL_SERVING and host and client_id and client_secret:
        return WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
    if IS_MODEL_SERVING:
        return WorkspaceClient()

    # 2. Explicit host + token from env
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if host and token:
        return WorkspaceClient(host=host, token=token)

    # 3. OAuth M2M with SPN credentials
    if host and client_id and client_secret:
        return WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)

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

    if not host:
        try:
            host = get_workspace_client().config.host or ""
        except Exception:
            pass

    if host and not host.startswith("http"):
        host = f"https://{host}"
    return host

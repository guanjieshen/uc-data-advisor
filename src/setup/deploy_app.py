"""Optional Databricks App deployment.

Skipped unless `enable_dbx_app: true` is set in the advisor config.

Steps:
  1. Render app.yaml with the orchestrator endpoint name as an env var.
  2. Upload app/ui/* to a workspace path.
  3. Create the Databricks App if missing (idempotent).
  4. Grant the app's service principal CAN_QUERY on the orchestrator endpoint.
  5. Deploy from the workspace path.
  6. Record the app URL into config["infrastructure"]["app_url"].
"""

from __future__ import annotations

import base64
import logging
import os
import time
from datetime import timedelta
from pathlib import Path

import yaml
from databricks.sdk.errors import NotFound, ResourceAlreadyExists
from databricks.sdk.service.workspace import ImportFormat

logger = logging.getLogger(__name__)

APP_SOURCE_DIR = Path(__file__).parent.parent.parent / "app" / "ui"
APP_FILES = ("main.py", "requirements.txt", "app.yaml", "static/index.html")


def deploy_app(config: dict, w) -> dict | None:
    """Deploy a Databricks App with a chat UI in front of the orchestrator endpoint.

    Returns the app's public URL on success, or None if skipped/failed.
    """
    if not config.get("enable_dbx_app", False):
        print("  enable_dbx_app: false — skipping Databricks App deployment")
        return None

    infra = config.get("infrastructure", {}) or {}
    app_name = infra.get("app_name", "")
    agent_endpoints = infra.get("agent_endpoints", {}) or {}
    orchestrator_ep = agent_endpoints.get("orchestrator", "")

    if not app_name or not orchestrator_ep:
        print("  Missing infrastructure.app_name or orchestrator endpoint — run deploy-agents first")
        return None

    print("=" * 60)
    print("Deploying Databricks App")
    print("=" * 60)
    print(f"  App name:     {app_name}")
    print(f"  Orchestrator: {orchestrator_ep}")

    # 1. Render app.yaml with the right ORCHESTRATOR_ENDPOINT value.
    rendered_yaml = _render_app_yaml(orchestrator_ep)

    # 2. Upload source files to a workspace path the app can read.
    me = w.current_user.me()
    user = me.user_name
    workspace_dir = f"/Workspace/Users/{user}/uc-data-advisor-apps/{app_name}"

    print(f"  Uploading source to: {workspace_dir}")
    _upload_app_source(w, workspace_dir, rendered_yaml)

    # 3. Create the app if missing.
    app_obj = _ensure_app(w, app_name)
    app_sp = app_obj.get("service_principal_client_id", "")
    print(f"  App SP client id: {app_sp or '(not yet provisioned)'}")

    # 4. Grant CAN_QUERY on the orchestrator (and sub-agents — same SP needed).
    if app_sp:
        _grant_can_query(w, agent_endpoints, app_sp)
    else:
        print("  Skipping CAN_QUERY grant — app SP not yet ready; rerun --step deploy-app after the app is created")

    # 5. Deploy from the workspace path.
    from databricks.sdk.service.apps import AppDeployment
    print("  Deploying source code...")
    deployment = w.apps.deploy(
        app_name=app_name,
        app_deployment=AppDeployment(source_code_path=workspace_dir),
    ).result(timeout=timedelta(minutes=10))  # blocks until SUCCEEDED or fails

    state = getattr(deployment, "status", None)
    print(f"  Deployment state: {state}")

    # 6. Read back the live app URL.
    fresh = w.apps.get(name=app_name)
    url = fresh.url or ""
    print(f"  App URL: https://{url}" if url and not url.startswith("http") else f"  App URL: {url}")

    infra["app_url"] = url
    config["infrastructure"] = infra
    return url


def _render_app_yaml(orchestrator_ep: str) -> str:
    """Read app.yaml template and substitute ORCHESTRATOR_ENDPOINT."""
    template = (APP_SOURCE_DIR / "app.yaml").read_text()
    cfg = yaml.safe_load(template) or {}
    env_vars = cfg.get("env", []) or []
    for entry in env_vars:
        if entry.get("name") == "ORCHESTRATOR_ENDPOINT":
            entry["value"] = orchestrator_ep
            break
    else:
        env_vars.append({"name": "ORCHESTRATOR_ENDPOINT", "value": orchestrator_ep})
        cfg["env"] = env_vars
    return yaml.safe_dump(cfg, sort_keys=False)


def _upload_app_source(w, workspace_dir: str, rendered_yaml: str) -> None:
    """Upload main.py, requirements.txt, rendered app.yaml, and static/* to the workspace."""
    w.workspace.mkdirs(workspace_dir)
    w.workspace.mkdirs(f"{workspace_dir}/static")

    for rel in APP_FILES:
        target = f"{workspace_dir}/{rel}"
        if rel == "app.yaml":
            content = rendered_yaml.encode()
        else:
            content = (APP_SOURCE_DIR / rel).read_bytes()
        w.workspace.upload(
            path=target,
            content=content,
            format=ImportFormat.AUTO,
            overwrite=True,
        )


def _ensure_app(w, app_name: str) -> dict:
    """Create the app if it doesn't exist; return its current state as a dict."""
    try:
        existing = w.apps.get(name=app_name)
        state = existing.app_status.state if existing.app_status else "UNKNOWN"
        print(f"  App {app_name} already exists (state: {state})")
        return _as_dict(existing)
    except NotFound:
        pass

    print(f"  Creating app {app_name}...")
    from databricks.sdk.service.apps import App
    created = w.apps.create(app=App(name=app_name)).result(timeout=timedelta(minutes=5))
    return _as_dict(created)


def _grant_can_query(w, agent_endpoints: dict, app_sp_client_id: str) -> None:
    """Grant the app SP CAN_QUERY on every agent endpoint so it can invoke them."""
    from databricks.sdk.service.serving import (
        ServingEndpointAccessControlRequest,
        ServingEndpointPermissionLevel,
    )

    for label, ep_name in agent_endpoints.items():
        try:
            w.serving_endpoints.update_permissions(
                serving_endpoint_id=_endpoint_id(w, ep_name),
                access_control_list=[
                    ServingEndpointAccessControlRequest(
                        service_principal_name=app_sp_client_id,
                        permission_level=ServingEndpointPermissionLevel.CAN_QUERY,
                    )
                ],
            )
            print(f"    CAN_QUERY on {ep_name} → app SP")
        except Exception as e:
            print(f"    WARN: could not grant CAN_QUERY on {ep_name}: {e}")


def _endpoint_id(w, ep_name: str) -> str:
    ep = w.serving_endpoints.get(name=ep_name)
    return ep.id


def _as_dict(obj) -> dict:
    """Coerce SDK object → dict for safe attribute access."""
    if hasattr(obj, "as_dict"):
        return obj.as_dict() or {}
    return {k: getattr(obj, k) for k in dir(obj) if not k.startswith("_")}


def teardown_app(config: dict, w) -> None:
    """Delete the Databricks App on teardown. Idempotent."""
    if not config.get("enable_dbx_app", False):
        return
    infra = config.get("infrastructure", {}) or {}
    app_name = infra.get("app_name", "")
    if not app_name:
        return
    try:
        w.apps.delete(name=app_name)
        print(f"  Deleted app {app_name}")
    except NotFound:
        pass
    except Exception as e:
        print(f"  WARN: could not delete app {app_name}: {e}")

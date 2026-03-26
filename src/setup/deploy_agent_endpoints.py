"""Deploy registered agent models to Model Serving endpoints.

Creates individual serving endpoints for each agent model,
with environment variables for tool access (VS, Genie, LLM).
"""

import time
import logging

logger = logging.getLogger(__name__)


def deploy_agent_endpoints(config: dict, w) -> dict:
    """Create serving endpoints for each registered agent model.

    Returns dict of {agent_name: endpoint_name}.
    """
    infra = config.get("infrastructure", {})
    app_name = infra.get("app_name", "uc-data-advisor")
    registered = infra.get("registered_models", {})

    if not registered:
        print("  No registered models found — run 'register' step first")
        return {}

    print("=" * 60)
    print("Deploying Agent Endpoints")
    print("=" * 60)

    # Environment vars that each agent needs at serving time
    env_vars = {
        "SERVING_ENDPOINT": infra.get("serving_endpoint", ""),
        "GENIE_SPACE_ID": infra.get("genie_space_id", ""),
        "VS_INDEX_METADATA": infra.get("vs_index_metadata", ""),
        "VS_INDEX_KNOWLEDGE": infra.get("vs_index_knowledge", ""),
    }

    endpoints = {}
    for agent_name, model_info in registered.items():
        ep_name = f"{app_name}-{agent_name}-agent"
        model_name = model_info["model_name"]
        version = model_info["version"]

        print(f"  [{agent_name}] Creating endpoint {ep_name}...", end=" ", flush=True)

        try:
            # Check if endpoint exists
            existing = _get_endpoint(w, ep_name)
            if existing:
                print(f"already exists, updating config...")
                _update_endpoint(w, ep_name, model_name, version, env_vars)
            else:
                _create_endpoint(w, ep_name, model_name, version, env_vars, app_name)

            endpoints[agent_name] = ep_name
            print("OK")

        except Exception as e:
            print(f"FAILED: {e}")
            logger.error(f"Failed to deploy {agent_name} endpoint: {e}", exc_info=True)

    # Grant app SP CAN_QUERY on agent endpoints
    app_sp = infra.get("app_sp_client_id", "")
    if app_sp and endpoints:
        _grant_app_permissions(w, endpoints, app_sp)

    print(f"  Deployed {len(endpoints)}/{len(registered)} agent endpoints")
    return endpoints


def _get_endpoint(w, ep_name: str):
    """Check if a serving endpoint exists."""
    try:
        return w.serving_endpoints.get(ep_name)
    except Exception:
        return None


def _create_endpoint(w, ep_name, model_name, version, env_vars, app_name):
    """Create a new Model Serving endpoint for an agent."""
    w.api_client.do("POST", "/api/2.0/serving-endpoints", body={
        "name": ep_name,
        "config": {
            "served_entities": [{
                "entity_name": model_name,
                "entity_version": str(version),
                "scale_to_zero_enabled": True,
                "workload_size": "Small",
                "environment_vars": {k: v for k, v in env_vars.items() if v},
            }],
        },
        "tags": [
            {"key": "app", "value": app_name},
            {"key": "type", "value": "agent"},
        ],
    })


def _update_endpoint(w, ep_name, model_name, version, env_vars):
    """Update an existing endpoint's served entity."""
    w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{ep_name}/config", body={
        "served_entities": [{
            "entity_name": model_name,
            "entity_version": str(version),
            "scale_to_zero_enabled": True,
            "workload_size": "Small",
            "environment_vars": {k: v for k, v in env_vars.items() if v},
        }],
    })


def _grant_app_permissions(w, endpoints, app_sp):
    """Grant the app SP CAN_QUERY on each agent endpoint."""
    for agent_name, ep_name in endpoints.items():
        try:
            ep = w.serving_endpoints.get(ep_name)
            w.api_client.do("PATCH", f"/api/2.0/permissions/serving-endpoints/{ep.id}", body={
                "access_control_list": [{
                    "service_principal_name": app_sp,
                    "permission_level": "CAN_QUERY",
                }],
            })
        except Exception as e:
            logger.warning(f"Failed to grant app SP on {ep_name}: {e}")

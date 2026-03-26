"""Deploy registered agent models using Databricks Agent Bricks SDK.

Uses databricks.agents.deploy() for proper Agent Bricks deployment
with built-in observability, Review App, and scaling.
"""

import logging

logger = logging.getLogger(__name__)


def deploy_agent_endpoints(config: dict, w) -> dict:
    """Deploy each registered agent model via Agent Bricks SDK.

    Returns dict of {agent_name: endpoint_name}.
    """
    from databricks import agents

    infra = config.get("infrastructure", {})
    app_name = infra.get("app_name", "uc-data-advisor")
    registered = infra.get("registered_models", {})

    if not registered:
        print("  No registered models found — run 'register' step first")
        return {}

    # Set MLflow experiment so agent traces are logged for real-time monitoring
    import mlflow
    experiment_name = f"/uc-data-advisor-{app_name}-traces"
    mlflow.set_experiment(experiment_name)

    print("=" * 60)
    print("Deploying Agent Endpoints (Agent Bricks)")
    print(f"  MLflow experiment: {experiment_name}")
    print("=" * 60)

    endpoints = {}
    for agent_name, model_info in registered.items():
        ep_name = f"{app_name}-{agent_name}-agent"
        model_name = model_info["model_name"]
        version = model_info["version"]
        model_uri = f"models:/{model_name}/{version}"

        print(f"  [{agent_name}] Deploying {ep_name}...", end=" ", flush=True)

        try:
            # Wait for any in-progress updates to finish
            _wait_for_endpoint_ready(w, ep_name)

            # Delete existing deployment to redeploy with new version
            try:
                agents.delete_deployment(model_name=model_name)
                print("replacing...", end=" ", flush=True)
            except Exception:
                pass  # No existing deployment

            # Environment vars the agent model needs at serving time
            workspace_host = config.get("workspace", {}).get("host", "")
            secret_scope = infra.get("secret_scope", app_name)
            env_vars = {
                "DATABRICKS_HOST": workspace_host,
                "DATABRICKS_TOKEN": f"{{{{secrets/{secret_scope}/serving-token}}}}",
                "SERVING_ENDPOINT": infra.get("serving_endpoint", ""),
                "GENIE_SPACE_ID": infra.get("genie_space_id", ""),
                "VS_INDEX_METADATA": infra.get("vs_index_metadata", ""),
                "VS_INDEX_KNOWLEDGE": infra.get("vs_index_knowledge", ""),
            }
            # Filter out empty values (but keep DATABRICKS_TOKEN even though it looks like a template)
            env_vars = {k: v for k, v in env_vars.items() if v}

            # Deploy using Agent Bricks SDK
            deployment = agents.deploy(
                model_name=model_name,
                model_version=int(version),
                endpoint_name=ep_name,
                scale_to_zero=True,
                environment_vars=env_vars,
                tags={"app": app_name, "agent": agent_name},
            )

            endpoints[agent_name] = ep_name
            print("deployed")

            # Add AI Gateway configuration (rate limits, usage tracking, guardrails)
            _configure_ai_gateway(w, ep_name, config)

            # Patch environment vars onto the endpoint (agents.deploy may not pass them through)
            _patch_endpoint_env_vars(w, ep_name, env_vars)

            # Enable trace reviews for observability
            try:
                agents.enable_trace_reviews(endpoint_name=ep_name)
            except Exception:
                pass  # Not critical

        except Exception as e:
            print(f"FAILED: {e}")
            logger.error(f"Failed to deploy {agent_name}: {e}", exc_info=True)

    # Grant app SP CAN_QUERY on agent endpoints
    app_sp = infra.get("app_sp_client_id", "")
    if app_sp and endpoints:
        _grant_app_permissions(w, endpoints, app_sp)

    print(f"  Deployed {len(endpoints)}/{len(registered)} agent endpoints")
    return endpoints


def _patch_endpoint_env_vars(w, ep_name: str, env_vars: dict):
    """Patch environment variables onto a serving endpoint's served entities."""
    try:
        ep = w.serving_endpoints.get(ep_name)
        entities = []
        for entity in ep.config.served_entities or []:
            existing_vars = {}
            if hasattr(entity, 'environment_vars') and entity.environment_vars:
                existing_vars = dict(entity.environment_vars)
            existing_vars.update(env_vars)
            entities.append({
                "entity_name": entity.entity_name,
                "entity_version": entity.entity_version,
                "environment_vars": existing_vars,
            })
        if entities:
            w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{ep_name}/config", body={
                "served_entities": entities,
            })
            print(f"    Env vars patched on {ep_name}")
    except Exception as e:
        logger.warning(f"Failed to patch env vars on {ep_name}: {e}")


def _configure_ai_gateway(w, ep_name: str, config: dict):
    """Add AI Gateway configuration to an agent serving endpoint."""
    ai_gateway = {
        "usage_tracking_config": {"enabled": True},
        "rate_limits": [
            {"calls": 120, "key": "user", "renewal_period": "minute"},
            {"calls": 500, "key": "endpoint", "renewal_period": "minute"},
        ],
    }

    if config.get("enable_ai_gateway_guardrails", False):
        ai_gateway["guardrails"] = {
            "input": {"safety": True, "pii": {"behavior": "NONE"}},
            "output": {"safety": False, "pii": {"behavior": "NONE"}},
        }

    try:
        w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{ep_name}/ai-gateway", body=ai_gateway)
        print(f"    AI Gateway configured on {ep_name}")
    except Exception as e:
        logger.warning(f"Failed to configure AI Gateway on {ep_name}: {e}")


def _wait_for_endpoint_ready(w, ep_name: str, timeout: int = 600):
    """Wait for an endpoint to finish any in-progress config updates."""
    import time
    try:
        ep = w.serving_endpoints.get(ep_name)
    except Exception:
        return  # Endpoint doesn't exist yet

    deadline = time.time() + timeout
    while time.time() < deadline:
        state = str(ep.state.config_update) if ep.state else ""
        if "IN_PROGRESS" not in state and "UPDATING" not in state:
            return
        print("waiting...", end=" ", flush=True)
        time.sleep(15)
        try:
            ep = w.serving_endpoints.get(ep_name)
        except Exception:
            return


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
            print(f"    Granted app SP CAN_QUERY on {ep_name}")
        except Exception as e:
            logger.warning(f"Failed to grant app SP on {ep_name}: {e}")

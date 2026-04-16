"""Deploy registered agent models as serving endpoints via the Databricks SDK.

Creates or updates Model Serving endpoints directly through the REST API,
avoiding the agents.deploy() artifact-download path that bypasses Unity Catalog.
Deploys sub-agents in parallel, then the orchestrator.
"""

import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


def deploy_agent_endpoints(config: dict, w) -> dict:
    """Deploy each registered agent model as a serving endpoint in parallel."""
    from databricks.sdk.service.serving import (
        EndpointCoreConfigInput,
        ServedEntityInput,
        EndpointTag,
        Route,
        TrafficConfig,
    )
    from databricks.sdk.errors import ResourceAlreadyExists

    infra = config.get("infrastructure", {})
    app_name = infra.get("app_name", "uc-data-advisor")
    registered = infra.get("registered_models", {})

    if not registered:
        print("  No registered models found — run 'register' step first")
        return {}

    import mlflow
    experiment_name = f"/uc-data-advisor-{app_name}-traces"
    mlflow.set_experiment(experiment_name)

    print("=" * 60)
    print("Deploying Agent Endpoints (parallel)")
    print(f"  MLflow experiment: {experiment_name}")
    print("=" * 60)

    workspace_host = config.get("workspace", {}).get("host", "")
    scope = infra.get("secret_scope", infra.get("app_name", ""))
    source_catalogs = ",".join(config.get("source_catalogs", []))
    scale_to_zero = config.get("scale_to_zero", True)

    # Ensure secret scope exists and has SP credentials.
    # Endpoints always deploy with SP credentials via {{secrets/...}} refs.
    from .provision_infrastructure import ensure_secret_scope
    sp_client_id = config.get("service_principal", "")
    ensure_secret_scope(w, scope, sp_client_id)

    env_vars = {
        "DATABRICKS_HOST": workspace_host,
        "SERVING_ENDPOINT": infra.get("serving_endpoint", ""),
        "GENIE_SPACE_ID": infra.get("genie_space_id", ""),
        "VS_INDEX_METADATA": infra.get("vs_index_metadata", ""),
        "VS_INDEX_KNOWLEDGE": infra.get("vs_index_knowledge", ""),
        "SOURCE_CATALOGS": source_catalogs,
    }
    env_vars = {k: v for k, v in env_vars.items() if v}

    # SP credentials resolved at endpoint runtime via secret refs
    secret_env_vars = dict(env_vars)
    secret_env_vars["DATABRICKS_CLIENT_ID"] = "{{secrets/" + scope + "/sp-client-id}}"
    secret_env_vars["DATABRICKS_CLIENT_SECRET"] = "{{secrets/" + scope + "/sp-client-secret}}"

    def _deploy_one(agent_name, model_info, deploy_env_vars=None):
        ep_name = f"{app_name}-{agent_name}-agent"
        model_name = model_info["model_name"]
        version = str(model_info["version"])
        ep_env = deploy_env_vars or secret_env_vars

        # Add static env vars that agents.deploy() used to inject
        full_env = dict(ep_env)
        full_env.setdefault("ENABLE_MLFLOW_TRACING", "true")
        full_env.setdefault("ENABLE_LANGCHAIN_STREAMING", "true")
        full_env.setdefault("RETURN_REQUEST_ID_IN_RESPONSE", "true")

        _wait_for_endpoint_ready(w, ep_name)

        # Served entity name: alphanumeric + dashes only, max 63 chars
        served_entity_name = f"{app_name}-{agent_name}-v{version}"

        served_entity = ServedEntityInput(
            name=served_entity_name,
            entity_name=model_name,
            entity_version=version,
            workload_size="Small",
            scale_to_zero_enabled=scale_to_zero,
            environment_vars=full_env,
        )

        endpoint_tags = [
            EndpointTag(key="app", value=app_name),
            EndpointTag(key="agent", value=agent_name),
        ]

        traffic = TrafficConfig(
            routes=[Route(served_model_name=served_entity_name, traffic_percentage=100)]
        )

        # Create endpoint; if it already exists, update its config
        try:
            w.serving_endpoints.create(
                name=ep_name,
                config=EndpointCoreConfigInput(
                    name=ep_name,
                    served_entities=[served_entity],
                    traffic_config=traffic,
                ),
                tags=endpoint_tags,
            )
        except ResourceAlreadyExists:
            logger.info(f"Endpoint {ep_name} exists, updating config")
            w.serving_endpoints.update_config(
                name=ep_name,
                served_entities=[served_entity],
                traffic_config=traffic,
            )

        # Wait for endpoint to be READY before patching config
        _wait_for_endpoint_ready(w, ep_name)
        _configure_ai_gateway(w, ep_name, config)
        _patch_endpoint_env_vars(w, ep_name, ep_env)

        return agent_name, ep_name

    # Deploy sub-agents first (parallel), then orchestrator (needs their endpoint names)
    sub_agents = {k: v for k, v in registered.items() if k != "orchestrator"}
    orchestrator_model = registered.get("orchestrator")

    # Phase 1: Deploy sub-agents in parallel
    endpoints = {}
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {
            pool.submit(_deploy_one, name, info): name
            for name, info in sub_agents.items()
        }
        for future in as_completed(futures):
            agent_name = futures[future]
            try:
                name, ep_name = future.result()
                endpoints[name] = ep_name
                print(f"  [{name}] deployed → {ep_name}")
            except Exception as e:
                print(f"  [{agent_name}] FAILED: {e}")
                logger.error(f"Failed to deploy {agent_name}: {e}", exc_info=True)

    # Phase 2: Deploy orchestrator with sub-agent endpoint names
    if orchestrator_model:
        orch_env = dict(env_vars)
        for agent_name, ep_name in endpoints.items():
            orch_env[f"{agent_name.upper()}_AGENT_ENDPOINT"] = ep_name
        try:
            name, ep_name = _deploy_one("orchestrator", orchestrator_model, deploy_env_vars=orch_env)
            endpoints[name] = ep_name
            print(f"  [{name}] deployed → {ep_name}")
        except Exception as e:
            print(f"  [orchestrator] FAILED: {e}")
            logger.error(f"Failed to deploy orchestrator: {e}", exc_info=True)

    print(f"  Deployed {len(endpoints)}/{len(registered)} agent endpoints")
    print("  Note: run '--step grant-agent-permissions' after endpoints are READY")
    return endpoints


def grant_agent_permissions(config: dict, w) -> None:
    """Grant configured SP CAN_QUERY on agent endpoints. Run after endpoints are provisioned."""
    infra = config.get("infrastructure", {})
    endpoints = infra.get("agent_endpoints", {})
    sp = config.get("service_principal", "")

    if not endpoints:
        print("  No agent endpoints configured")
        return
    if not sp:
        print("  No service_principal configured")
        return

    print("=" * 60)
    print("Granting Agent Endpoint Permissions")
    print("=" * 60)

    # Wait for all endpoints to be READY before granting permissions
    print("  Waiting for endpoints to be READY...", end=" ", flush=True)
    start = time.time()
    for _ in range(60):
        ready_count = 0
        for ep_name in endpoints.values():
            try:
                resp = w.api_client.do("GET", f"/api/2.0/serving-endpoints/{ep_name}")
                if resp.get("state", {}).get("ready") == "READY":
                    ready_count += 1
            except Exception:
                pass
        if ready_count == len(endpoints):
            print(f"all ready ({int(time.time() - start)}s)")
            break
        elapsed = int(time.time() - start)
        print(f"\r  Waiting for endpoints to be READY ({elapsed}s)...{' ' * 20}", end="", flush=True)
        time.sleep(15)
    else:
        print(f"\r  Timed out waiting for endpoints ({int(time.time() - start)}s){' ' * 20}")

    _grant_endpoint_permissions(w, infra, config, endpoints, sp)


def _patch_endpoint_env_vars(w, ep_name: str, env_vars: dict):
    """Patch environment variables onto a serving endpoint's served entities."""
    try:
        ep = w.serving_endpoints.get(ep_name)
        if not ep.config or not ep.config.served_entities:
            return
        entities = []
        for entity in ep.config.served_entities:
            existing_vars = {}
            if hasattr(entity, 'environment_vars') and entity.environment_vars:
                existing_vars = dict(entity.environment_vars)
            existing_vars.update(env_vars)
            entities.append({
                "entity_name": entity.entity_name,
                "entity_version": entity.entity_version,
                "environment_vars": existing_vars,
                "workload_size": "Small",
                "scale_to_zero_enabled": True,
            })
        if entities:
            w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{ep_name}/config", body={
                "served_entities": entities,
            })
    except Exception as e:
        logger.warning(f"Failed to patch env vars on {ep_name}: {e}")


def _configure_ai_gateway(w, ep_name: str, config: dict):
    """Configure AI Gateway on an agent serving endpoint."""
    ai_gateway = {}

    if config.get("enable_ai_gateway_guardrails", False):
        ai_gateway["guardrails"] = {
            "input": {"safety": True, "pii": {"behavior": "NONE"}},
            "output": {"safety": False, "pii": {"behavior": "NONE"}},
        }

    rate_limits = config.get("rate_limits")
    if rate_limits:
        ai_gateway["rate_limits"] = rate_limits

    if not ai_gateway:
        return

    try:
        w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{ep_name}/ai-gateway", body=ai_gateway)
    except Exception as e:
        logger.warning(f"Failed to configure AI Gateway on {ep_name}: {e}")


def _wait_for_endpoint_ready(w, ep_name: str, timeout: int = 600):
    """Wait for an endpoint to finish any in-progress config updates."""
    try:
        ep = w.serving_endpoints.get(ep_name)
    except Exception:
        return

    start = time.time()
    deadline = start + timeout
    while time.time() < deadline:
        state = str(ep.state.config_update) if ep.state else ""
        if "IN_PROGRESS" not in state and "UPDATING" not in state:
            return
        elapsed = int(time.time() - start)
        print(f"\r  [{ep_name.split('-')[-2]}] waiting ({elapsed}s)...{' ' * 20}", end="", flush=True)
        time.sleep(15)
        try:
            ep = w.serving_endpoints.get(ep_name)
        except Exception:
            return
    print()


def _grant_endpoint_permissions(w, infra, config, endpoints, sp):
    """Grant configured SP CAN_QUERY on each agent endpoint via SDK."""
    from databricks.sdk.service.serving import ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel

    for agent_name, ep_name in endpoints.items():
        try:
            ep = w.serving_endpoints.get(ep_name)
            w.serving_endpoints.update_permissions(
                serving_endpoint_id=ep.id,
                access_control_list=[
                    ServingEndpointAccessControlRequest(
                        service_principal_name=sp,
                        permission_level=ServingEndpointPermissionLevel.CAN_QUERY,
                    ),
                ],
            )
            print(f"    CAN_QUERY on endpoint '{ep_name}' → SP {sp}")
        except Exception as e:
            logger.warning(f"Failed to grant permissions on {ep_name}: {e}")

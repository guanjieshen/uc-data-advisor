"""Register each agent as an MLflow model in Unity Catalog.

Uses MLflow's model-from-code pattern: creates a thin model definition file
per agent that MLflow loads at serving time. Registers all agents in parallel.
"""

import os
import logging
import textwrap
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

AGENTS = ["discovery", "metrics", "qa", "orchestrator"]

MODEL_DEF_TEMPLATE = textwrap.dedent("""\
    import mlflow
    from mlflow.models import set_model
    from server.agents.{module} import {class_name}

    set_model({class_name}())
""")

AGENT_DEFS = {
    "discovery": ("discovery", "DiscoveryAgent"),
    "metrics": ("metrics", "MetricsAgent"),
    "qa": ("qa", "QAAgent"),
    "orchestrator": ("orchestrator_agent", "OrchestratorAgent"),
}


def register_agent_models(config: dict, w) -> dict:
    """Log and register all agent models in UC model registry in parallel."""
    import mlflow

    infra = config.get("infrastructure", {})
    app_name = infra.get("app_name", "uc-data-advisor")
    catalog = infra.get("advisor_catalog", "uc_data_advisor")
    schema = infra.get("advisor_schema", "default")

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    app_dir = os.path.join(project_root, "app")
    server_dir = os.path.join(app_dir, "server")
    config_dir = os.path.join(project_root, "config")

    # Ensure MLflow can authenticate — on serverless clusters the SDK
    # picks up notebook context automatically but MLflow does not.
    if not os.environ.get("DATABRICKS_HOST"):
        os.environ["DATABRICKS_HOST"] = w.config.host
    if not os.environ.get("DATABRICKS_TOKEN"):
        headers = w.config.authenticate()
        bearer = headers.get("Authorization", "")
        if bearer.startswith("Bearer "):
            os.environ["DATABRICKS_TOKEN"] = bearer[len("Bearer "):]

    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(f"/uc-data-advisor-{app_name}-traces")

    req_path = os.path.join(app_dir, "requirements.txt")
    pip_reqs = []
    if os.path.exists(req_path):
        with open(req_path) as f:
            pip_reqs = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    try:
        import openai  # noqa: F401
    except ImportError:
        print("ERROR: 'openai' package required for model registration.")
        return {}

    import sys
    if app_dir not in sys.path:
        sys.path.insert(0, app_dir)

    # Set config path so agents don't warn during MLflow validation
    config_path = os.path.abspath(config.get("_config_path", "config/advisor_config.yaml"))
    os.environ.setdefault("ADVISOR_CONFIG_PATH", config_path)

    print("=" * 60)
    print("Registering Agent Models (parallel)")
    print("=" * 60)

    def _register_one(agent_name):
        module_name, class_name = AGENT_DEFS[agent_name]
        model_name = f"{catalog}.{schema}.{app_name.replace('-', '_')}_{agent_name}_agent"

        model_def_code = MODEL_DEF_TEMPLATE.format(module=module_name, class_name=class_name)
        model_def_path = os.path.join(app_dir, f"{agent_name}_model.py")

        with open(model_def_path, "w") as f:
            f.write(model_def_code)

        try:
            with mlflow.start_run(run_name=f"register_{agent_name}_agent"):
                model_info = mlflow.pyfunc.log_model(
                    name=f"{agent_name}_agent",
                    python_model=model_def_path,
                    code_paths=[server_dir, config_dir],
                    pip_requirements=pip_reqs,
                    registered_model_name=model_name,
                )
            version = model_info.registered_model_version
            return agent_name, {
                "model_name": model_name,
                "version": version,
                "uri": f"models:/{model_name}/{version}",
            }
        finally:
            if os.path.exists(model_def_path):
                os.unlink(model_def_path)

    registered = {}
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(_register_one, name): name for name in AGENTS}
        for future in as_completed(futures):
            agent_name = futures[future]
            try:
                name, info = future.result()
                registered[name] = info
                print(f"  [{name}] v{info['version']}")
            except Exception as e:
                print(f"  [{agent_name}] FAILED: {e}")
                logger.error(f"Failed to register {agent_name}: {e}", exc_info=True)

    print(f"  Registered {len(registered)}/{len(AGENTS)} models")
    return registered

"""Register each agent as an MLflow model in Unity Catalog.

Uses MLflow's model-from-code pattern: creates a thin model definition file
per agent that MLflow loads at serving time. This avoids importing the full
agent code (and its dependencies like openai) at registration time.
"""

import os
import logging
import textwrap
import tempfile

logger = logging.getLogger(__name__)

AGENTS = ["discovery", "metrics", "qa"]

# Model definition templates — MLflow loads these at serving time
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
}


def register_agent_models(config: dict, w) -> dict:
    """Log and register all agent models in UC model registry.

    Uses MLflow's model-from-code pattern to avoid importing agent code
    (and heavyweight dependencies like openai) at registration time.

    Returns dict of {agent_name: {model_name, version, uri}}.
    """
    import mlflow

    infra = config.get("infrastructure", {})
    app_name = infra.get("app_name", "uc-data-advisor")
    catalog = infra.get("advisor_catalog", "uc_data_advisor")
    schema = infra.get("advisor_schema", "default")

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    app_dir = os.path.join(project_root, "app")
    config_dir = os.path.join(project_root, "config")

    mlflow.set_registry_uri("databricks-uc")

    # Read pip requirements from app/requirements.txt
    req_path = os.path.join(app_dir, "requirements.txt")
    pip_reqs = []
    if os.path.exists(req_path):
        with open(req_path) as f:
            pip_reqs = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    # MLflow validates models by running predict() at log time,
    # so all agent dependencies (openai, databricks-sdk, etc.) must be installed.
    try:
        import openai  # noqa: F401
    except ImportError:
        print("ERROR: 'openai' package required for model registration.")
        print("       Install with: pip install openai")
        print("       Or run this step from a Databricks notebook.")
        return {}

    print("=" * 60)
    print("Registering Agent Models")
    print("=" * 60)

    registered = {}
    for agent_name in AGENTS:
        module_name, class_name = AGENT_DEFS[agent_name]
        model_name = f"{catalog}.{schema}.{app_name.replace('-', '_')}_{agent_name}_agent"
        print(f"  [{agent_name}] Registering as {model_name}...", end=" ", flush=True)

        try:
            # Write model definition file inside the app directory
            # so that `from server.agents.X import Y` resolves at log time
            model_def_code = MODEL_DEF_TEMPLATE.format(module=module_name, class_name=class_name)
            model_def_path = os.path.join(app_dir, f"{agent_name}_model.py")

            with open(model_def_path, "w") as f:
                f.write(model_def_code)

            # Ensure app_dir is on sys.path for import resolution
            import sys
            if app_dir not in sys.path:
                sys.path.insert(0, app_dir)

            try:
                with mlflow.start_run(run_name=f"register_{agent_name}_agent"):
                    model_info = mlflow.pyfunc.log_model(
                        artifact_path=f"{agent_name}_agent",
                        python_model=model_def_path,
                        code_paths=[app_dir, config_dir],
                        pip_requirements=pip_reqs,
                        registered_model_name=model_name,
                    )
            finally:
                # Clean up the model definition file
                if os.path.exists(model_def_path):
                    os.unlink(model_def_path)

            version = model_info.registered_model_version
            uri = f"models:/{model_name}/{version}"
            registered[agent_name] = {
                "model_name": model_name,
                "version": version,
                "uri": uri,
            }
            print(f"v{version}")

        except Exception as e:
            print(f"FAILED: {e}")
            logger.error(f"Failed to register {agent_name}: {e}", exc_info=True)

    print(f"  Registered {len(registered)}/{len(AGENTS)} models")
    return registered

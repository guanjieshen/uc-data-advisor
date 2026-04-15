"""Register each agent as a model in Unity Catalog.

Uses mlflow.pyfunc.save_model() for local artifact packaging (no network),
then uploads to a UC Volume via the Files API. Model version creation uses
MlflowClient (the only supported way to create UC model versions — the
Databricks SDK does not expose this). No MLflow tracking, experiments, runs,
or logged models are created.
"""

import os
import logging
import textwrap
import tempfile
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

VOLUME_NAME = "model_artifacts"


def register_agent_models(config: dict, w) -> dict:
    """Package and register all agent models in UC via Volume upload + MlflowClient."""
    import mlflow.pyfunc
    from mlflow import MlflowClient
    from databricks.sdk.service.catalog import VolumeType
    from databricks.sdk.errors import ResourceAlreadyExists

    infra = config.get("infrastructure", {})
    app_name = infra.get("app_name", "uc-data-advisor")
    catalog = infra.get("advisor_catalog", "uc_data_advisor")
    schema = infra.get("advisor_schema", "default")

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    app_dir = os.path.join(project_root, "app")
    server_dir = os.path.join(app_dir, "server")
    config_dir = os.path.join(project_root, "config")

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

    # Set config path so agents don't warn during save_model validation
    config_path = os.path.abspath(config.get("_config_path", "config/advisor_config.yaml"))
    os.environ.setdefault("ADVISOR_CONFIG_PATH", config_path)

    # MlflowClient for UC model version creation (SDK doesn't expose this)
    if not os.environ.get("DATABRICKS_HOST"):
        os.environ["DATABRICKS_HOST"] = w.config.host
    if not os.environ.get("DATABRICKS_TOKEN"):
        headers = w.config.authenticate()
        bearer = headers.get("Authorization", "")
        if bearer.startswith("Bearer "):
            os.environ["DATABRICKS_TOKEN"] = bearer[len("Bearer "):]
    os.environ.pop("DATABRICKS_CONFIG_PROFILE", None)
    uc_client = MlflowClient(registry_uri="databricks-uc")

    # Ensure model artifacts volume exists
    try:
        w.volumes.create(
            catalog_name=catalog, schema_name=schema,
            name=VOLUME_NAME, volume_type=VolumeType.MANAGED,
        )
    except ResourceAlreadyExists:
        pass

    print("=" * 60)
    print("Registering Agent Models (parallel)")
    print("=" * 60)

    def _register_one(agent_name):
        module_name, class_name = AGENT_DEFS[agent_name]
        model_short = f"{app_name.replace('-', '_')}_{agent_name}_agent"
        model_fqn = f"{catalog}.{schema}.{model_short}"

        model_def_code = MODEL_DEF_TEMPLATE.format(module=module_name, class_name=class_name)
        model_def_path = os.path.join(app_dir, f"{agent_name}_model.py")

        with open(model_def_path, "w") as f:
            f.write(model_def_code)

        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                local_path = os.path.join(tmp_dir, "model")

                # 1. Package model locally (no network calls)
                mlflow.pyfunc.save_model(
                    path=local_path,
                    python_model=model_def_path,
                    code_paths=[server_dir, config_dir],
                    pip_requirements=pip_reqs,
                )

                # 2. Upload packaged artifacts to UC Volume
                volume_path = f"/Volumes/{catalog}/{schema}/{VOLUME_NAME}/{model_short}"
                _upload_directory(w, local_path, volume_path)

                # 3. Create registered model (if not exists)
                try:
                    uc_client.create_registered_model(model_fqn)
                except Exception as e:
                    if "ALREADY_EXISTS" not in str(e) and "RESOURCE_ALREADY_EXISTS" not in str(e):
                        raise

                # 4. Create model version from Volume source
                mv = uc_client.create_model_version(
                    name=model_fqn,
                    source=volume_path,
                )
                version = str(mv.version)

            return agent_name, {
                "model_name": model_fqn,
                "version": version,
                "uri": f"models:/{model_fqn}/{version}",
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


def _upload_directory(w, local_dir: str, volume_base: str) -> None:
    """Upload a local directory tree to a UC Volume path via Files API."""
    for root, _dirs, files in os.walk(local_dir):
        for fname in files:
            local_file = os.path.join(root, fname)
            rel_path = os.path.relpath(local_file, local_dir)
            remote_path = f"{volume_base}/{rel_path}"
            with open(local_file, "rb") as f:
                w.files.upload(remote_path, f, overwrite=True)



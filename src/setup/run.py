"""UC Data Advisor Setup Pipeline.

Orchestrates: provision → grant-uc → audit → generate → register →
  deploy-agents → grant-agent-permissions → deploy → verify.

Usage:
  uv run python -m src.setup.run --config config/my_config.yaml
  uv run python -m src.setup.run --config config/my_config.yaml --step provision
  uv run python -m src.setup.run --config config/my_config.yaml --step teardown
"""

import argparse
import os
import sys
import logging

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")


def main():
    parser = argparse.ArgumentParser(description="UC Data Advisor Setup Pipeline")
    parser.add_argument("--config", default="config/advisor_config.yaml", help="Path to config file")
    parser.add_argument("--step", choices=["provision", "grant-uc", "audit", "generate", "deploy", "register", "deploy-agents", "grant-agent-permissions", "verify", "teardown", "all"], default="all")
    args = parser.parse_args()

    from .config_loader import load_config, save_config

    config = load_config(args.config)
    config["_config_path"] = args.config

    # Set auth for SDK — token, profile, host-only, or default
    workspace = config.get("workspace", {})
    profile = workspace.get("profile", "")
    host = workspace.get("host", "")
    token = workspace.get("token", "")

    if token and host:
        os.environ["DATABRICKS_HOST"] = host
        os.environ["DATABRICKS_TOKEN"] = token
        auth_method = f"PAT ({host})"
    elif profile:
        os.environ["DATABRICKS_CONFIG_PROFILE"] = profile
        auth_method = f"profile ({profile})"
    elif host:
        os.environ["DATABRICKS_HOST"] = host
        auth_method = f"host-only ({host}) — using env/CLI auth"
    else:
        auth_method = "default (env/config)"

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()

    print()
    print("=" * 60)
    print("UC Data Advisor Setup Pipeline")
    print("=" * 60)
    print(f"  Config:   {args.config}")
    print(f"  Step:     {args.step}")
    print(f"  Catalogs: {config.get('source_catalogs', [])}")
    print(f"  Auth:     {auth_method}")
    print()

    steps = {
        "provision": _step_provision,
        "grant-uc": _step_grant_uc,
        "audit": _step_audit,
        "generate": _step_generate,
        "deploy": _step_deploy,
        "register": _step_register,
        "deploy-agents": _step_deploy_agents,
        "grant-agent-permissions": _step_grant_agent_permissions,
        "verify": _step_verify,
        "teardown": _step_teardown,
    }

    if args.step == "all":
        for step_name in ["provision", "grant-uc", "audit", "generate", "register", "deploy-agents", "grant-agent-permissions", "deploy", "verify"]:
            steps[step_name](config, w)
            save_config(config, args.config)
    else:
        steps[args.step](config, w)
        save_config(config, args.config)

    print()
    print("=" * 60)
    print("Setup complete!")
    print("=" * 60)
    app_name = config.get("infrastructure", {}).get("app_name", "uc-data-advisor")
    print(f"  App: {app_name}")
    print(f"  Config saved to: {args.config}")
    print()


def _step_provision(config, w):
    from .provision_infrastructure import provision
    config["infrastructure"] = provision(config, w)


def _step_audit(config, w):
    from .audit_metadata import audit
    config.setdefault("generated", {})
    config["generated"]["audit"] = audit(config, w)


def _step_generate(config, w):
    from .generate_domain import generate_domain
    from .generate_prompts import generate_prompts
    from .generate_knowledge_base import generate_knowledge_base
    from .generate_metric_views import generate_metric_views
    from .generate_benchmarks import generate_benchmarks, generate_ui, generate_genie_tables

    gen = config.setdefault("generated", {})

    print("=" * 60)
    print("Generating Content")
    print("=" * 60)

    gen["domain"] = generate_domain(config)
    print(f"  Domain: {gen['domain']['organization_name']} ({len(gen['domain']['data_domains'])} domains)")

    gen["prompts"] = generate_prompts(config)
    print(f"  Prompts: {list(gen['prompts'].keys())}")

    gen["knowledge_base"] = generate_knowledge_base(config)
    print(f"  Knowledge base: {len(gen['knowledge_base'])} FAQs")

    if config.get("enable_metric_views", False):
        views, refreshes = generate_metric_views(config)
        gen["metric_views"] = views
        gen["metric_refreshes"] = refreshes
        print(f"  Metric views: {len(views)} views, {len(refreshes)} refresh queries")
    else:
        gen["metric_views"] = {}
        gen["metric_refreshes"] = []
        print("  Metric views: skipped (enable_metric_views: false)")

    gen["ui"] = generate_ui(config)
    gen["genie_tables"] = generate_genie_tables(config)
    gen["benchmarks"] = generate_benchmarks(config)
    print(f"  Benchmarks: {len(gen['benchmarks'])} questions")
    print(f"  Genie tables: {len(gen['genie_tables'])}")
    print(f"  UI suggestions: {gen['ui']['suggestions']}")


def _step_deploy(config, w):
    from .deploy import deploy
    deploy(config, w)


def _step_register(config, w):
    from .register_models import register_agent_models
    config.setdefault("infrastructure", {})
    config["infrastructure"]["registered_models"] = register_agent_models(config, w)


def _step_deploy_agents(config, w):
    from .deploy_agent_endpoints import deploy_agent_endpoints
    config.setdefault("infrastructure", {})
    config["infrastructure"]["agent_endpoints"] = deploy_agent_endpoints(config, w)


def _step_grant_uc(config, w):
    from .provision_infrastructure import grant_uc_permissions
    grant_uc_permissions(config, w)


def _step_grant_agent_permissions(config, w):
    from .deploy_agent_endpoints import grant_agent_permissions
    grant_agent_permissions(config, w)


def _step_teardown(config, w):
    """Delete all resources created by the setup pipeline."""
    from .teardown import teardown
    teardown(config, w)


def _step_verify(config, w):
    """Run benchmark questions against the deployed app to verify it works."""
    import subprocess
    import json

    infra = config.get("infrastructure", {})
    app_name = infra.get("app_name", "")
    workspace = config.get("workspace", {})
    profile = workspace.get("profile", "")

    # Get the app URL
    cmd = ["databricks", "apps", "get", app_name, "-o", "json"]
    if profile:
        cmd += ["-p", profile]
    result = subprocess.run(cmd, capture_output=True, text=True)
    app_url = ""
    if result.returncode == 0:
        try:
            app_url = json.loads(result.stdout).get("url", "")
        except Exception:
            pass

    if not app_url:
        print("  Could not determine app URL, skipping verification")
        return

    print("=" * 60)
    print("Verification")
    print("=" * 60)
    print(f"  App URL: {app_url}")

    # Wait for agent endpoints to be ready
    agent_endpoints = infra.get("agent_endpoints", {})
    if agent_endpoints:
        import time
        print("  Waiting for agent endpoints to be ready...", end=" ", flush=True)
        start = time.time()
        all_ready = False
        for _ in range(60):
            try:
                ready_count = 0
                for ep_name in agent_endpoints.values():
                    resp = w.api_client.do("GET", f"/api/2.0/serving-endpoints/{ep_name}")
                    if resp.get("state", {}).get("ready") == "READY":
                        ready_count += 1
                if ready_count == len(agent_endpoints):
                    all_ready = True
                    break
            except Exception:
                pass
            elapsed = int(time.time() - start)
            print(f"\r  Waiting for agent endpoints ({elapsed}s)...{' ' * 20}", end="", flush=True)
            time.sleep(15)

        if all_ready:
            print(f"\r  Agent endpoints ready ({int(time.time() - start)}s){' ' * 30}")
        else:
            print(f"\r  Agent endpoints not ready after {int(time.time() - start)}s, running benchmarks anyway{' ' * 10}")

    # Run benchmarks — pass the token from our already-authenticated client
    config_path = os.path.abspath(config.get("_config_path", "config/advisor_config.yaml"))
    try:
        auth_token = w.config.authenticate().get("Authorization", "").replace("Bearer ", "")
    except Exception:
        auth_token = ""
    env = {
        **os.environ,
        "APP_URL": app_url,
        "ADVISOR_CONFIG_PATH": config_path,
    }
    if auth_token:
        env["DATABRICKS_TOKEN"] = auth_token
    if profile:
        env["DATABRICKS_PROFILE"] = profile

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    benchmark_script = os.path.join(project_root, "tests", "benchmark.py")

    print(f"  Running benchmarks...")
    result = subprocess.run(
        [sys.executable, benchmark_script],
        capture_output=False, text=True, env=env, cwd=project_root,
    )

    if result.returncode == 0:
        print("  Verification passed")
    else:
        print("  Verification failed — check benchmark output above")


if __name__ == "__main__":
    main()

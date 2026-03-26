"""UC Data Advisor Setup Pipeline.

Orchestrates: provision → audit → generate → deploy.

Usage:
  uv run python -m src.setup.run --config config/advisor_config.yaml
  uv run python -m src.setup.run --config config/advisor_config.yaml --step provision
  uv run python -m src.setup.run --config config/advisor_config.yaml --step audit
  uv run python -m src.setup.run --config config/advisor_config.yaml --step generate
  uv run python -m src.setup.run --config config/advisor_config.yaml --step deploy
"""

import argparse
import os
import logging

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")


def main():
    parser = argparse.ArgumentParser(description="UC Data Advisor Setup Pipeline")
    parser.add_argument("--config", default="config/advisor_config.yaml", help="Path to config file")
    parser.add_argument("--step", choices=["provision", "audit", "generate", "deploy", "register", "deploy-agents", "all"], default="all")
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
        "audit": _step_audit,
        "generate": _step_generate,
        "deploy": _step_deploy,
        "register": _step_register,
        "deploy-agents": _step_deploy_agents,
    }

    if args.step == "all":
        for step_name in ["provision", "audit", "generate", "deploy", "register", "deploy-agents"]:
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


if __name__ == "__main__":
    main()

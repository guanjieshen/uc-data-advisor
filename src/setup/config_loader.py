"""Load and save the UC Data Advisor configuration file."""

import yaml
from pathlib import Path
from typing import Any


DEFAULT_CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "advisor_config.yaml"


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load advisor config from YAML file."""
    config_path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path) as f:
        return yaml.safe_load(f) or {}


def save_config(config: dict[str, Any], path: str | Path | None = None) -> None:
    """Save advisor config back to YAML file."""
    config_path = Path(path) if path else DEFAULT_CONFIG_PATH
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True, width=120)


def get_infra(config: dict) -> dict:
    """Get infrastructure section with defaults."""
    return config.get("infrastructure", {})


def get_generated(config: dict) -> dict:
    """Get generated section."""
    return config.get("generated", {})


def get_app_name(config: dict) -> str:
    """Derive app name from config."""
    return config.get("app_name", get_infra(config).get("app_name", "uc-data-advisor"))


def get_advisor_catalog(config: dict) -> str:
    """Derive advisor catalog name."""
    return config.get("advisor_catalog", get_infra(config).get("advisor_catalog", "uc_data_advisor"))


def get_workspace_profile(config: dict) -> str:
    """Get Databricks CLI profile name."""
    return config.get("workspace", {}).get("profile", "")


def get_warehouse_id(config: dict) -> str:
    """Get SQL warehouse ID."""
    return get_infra(config).get("warehouse_id", "")


def get_app_identity(config: dict) -> dict:
    """Get app identity config."""
    return config.get("app_identity", {"type": "service_principal", "name": ""})

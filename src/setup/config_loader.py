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

"""Runtime config loader for the UC Data Advisor app.

Loads the generated section of advisor_config.yaml at startup.
All agent prompts, UI config, and domain context come from here.
"""

import os
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_CONFIG: dict | None = None


def _load() -> dict:
    """Load config from YAML file. Cached after first call."""
    import yaml

    # 1. Explicit env var
    explicit = os.environ.get("ADVISOR_CONFIG_PATH", "")
    if explicit and os.path.exists(explicit):
        return _read_yaml(explicit)

    # 2. Search config directories for any .yaml file with generated content
    config_dirs = [
        Path(__file__).parent.parent / "config",          # app/config/
        Path(__file__).parent.parent.parent / "config",    # project/config/
        Path("config"),                                     # cwd/config/
    ]
    for config_dir in config_dirs:
        if config_dir.is_dir():
            for yaml_file in sorted(config_dir.glob("*.yaml")):
                if yaml_file.name.endswith(".example.yaml"):
                    continue
                data = _read_yaml(str(yaml_file))
                if data and data.get("generated"):
                    return data

    logger.warning("No advisor config found, using defaults")
    return {}


def _read_yaml(path: str) -> dict:
    """Read a YAML file, return empty dict on failure."""
    import yaml
    try:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
            logger.info(f"Loaded advisor config from {path}")
            return data
    except Exception as e:
        logger.warning(f"Failed to load config from {path}: {e}")
        return {}


def get_config() -> dict[str, Any]:
    """Get the full config (cached singleton)."""
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = _load()
    return _CONFIG


def get_generated() -> dict[str, Any]:
    """Get the auto-generated section of the config."""
    return get_config().get("generated", {})


def get_prompts() -> dict[str, str]:
    """Get agent system prompts."""
    return get_generated().get("prompts", {})


def get_domain() -> dict[str, Any]:
    """Get domain context (org name, description, data domains)."""
    return get_generated().get("domain", {})


def get_ui() -> dict[str, Any]:
    """Get UI configuration (header subtitle, suggestions)."""
    return get_generated().get("ui", {})


def get_benchmarks() -> list[dict]:
    """Get benchmark test questions."""
    return get_generated().get("benchmarks", [])


def get_knowledge_base() -> list[dict]:
    """Get knowledge base FAQ entries."""
    return get_generated().get("knowledge_base", [])

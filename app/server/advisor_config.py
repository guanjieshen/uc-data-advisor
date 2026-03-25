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

    # Search paths: env var > app/config/ > project_root/config/
    candidates = [
        os.environ.get("ADVISOR_CONFIG_PATH", ""),
        str(Path(__file__).parent.parent / "config" / "advisor_config.yaml"),  # app/config/
        str(Path(__file__).parent.parent.parent / "config" / "advisor_config.yaml"),  # project/config/
        "config/advisor_config.yaml",  # cwd
    ]
    for config_path in candidates:
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path) as f:
                    data = yaml.safe_load(f) or {}
                    logger.info(f"Loaded advisor config from {config_path}")
                    return data
            except Exception as e:
                logger.warning(f"Failed to load config from {config_path}: {e}")
    logger.warning("No advisor config found, using defaults")
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

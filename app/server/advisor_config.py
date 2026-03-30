"""Runtime config loader for the UC Data Advisor app.

Loads the config YAML from the path specified by ADVISOR_CONFIG_PATH env var.
All agent prompts, UI config, and domain context come from here.
"""

import os
import logging
from typing import Any

logger = logging.getLogger(__name__)

_CONFIG: dict | None = None


def _load() -> dict:
    """Load config from the YAML file specified by ADVISOR_CONFIG_PATH."""
    import yaml

    config_path = os.environ.get("ADVISOR_CONFIG_PATH", "")
    if not config_path:
        logger.warning("ADVISOR_CONFIG_PATH not set, using defaults")
        return {}

    try:
        with open(config_path) as f:
            data = yaml.safe_load(f) or {}
            logger.info(f"Loaded advisor config from {config_path}")
            return data
    except Exception as e:
        logger.warning(f"Failed to load config from {config_path}: {e}")
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

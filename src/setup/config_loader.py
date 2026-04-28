"""Load and save the UC Data Advisor configuration file.

The config lives in two files side by side:

  <name>_config.yaml             — user-authored. Inputs only. Never written by the pipeline.
  <name>_config.generated.yaml   — pipeline-managed. Infrastructure IDs, generated content,
                                   agent endpoints, etc. Safe to delete to force a re-run.

`load_config()` merges both (generated wins on key collision). `save_config()` writes
only the generated half, so user edits to the input file are never clobbered.
"""

import yaml
from pathlib import Path
from typing import Any


DEFAULT_CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "advisor_config.yaml"

# Keys the user authors. Anything not listed here is treated as pipeline-generated and
# written to the .generated.yaml side file.
_INPUT_KEYS: frozenset[str] = frozenset({
    "source_catalogs",
    "workspace",
    "service_principal",
    "app_name",
    "warehouse_id",
    "advisor_catalog",
    "external_location",
    "serving_model",
    "embedding_model",
    "include_schemas",
    "exclude_schemas",
    "enable_metric_views",
    "enable_volume_indexing",
    "scale_to_zero",
    "enable_ai_gateway_guardrails",
    "rate_limits",
})


def _generated_path(input_path: Path) -> Path:
    """Sibling path: foo_config.yaml -> foo_config.generated.yaml."""
    return input_path.with_suffix(".generated" + input_path.suffix)


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load advisor config, merging the input file with its .generated sibling.

    For backward compatibility: if the input file contains generated keys (legacy single-file
    layout), they are still loaded — they just get migrated to the .generated file on the
    next save_config().
    """
    input_path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not input_path.exists():
        raise FileNotFoundError(f"Config file not found: {input_path}")

    with open(input_path) as f:
        merged: dict[str, Any] = yaml.safe_load(f) or {}

    gen_path = _generated_path(input_path)
    if gen_path.exists():
        with open(gen_path) as f:
            generated = yaml.safe_load(f) or {}
        merged.update(generated)  # generated wins on collision

    return merged


def save_config(config: dict[str, Any], path: str | Path | None = None) -> None:
    """Save the generated half of the config to the .generated sibling file.

    The input file (user-authored) is never modified. Internal-only keys (those starting with
    `_`) are dropped.
    """
    input_path = Path(path) if path else DEFAULT_CONFIG_PATH
    gen_path = _generated_path(input_path)

    generated = {
        k: v for k, v in config.items()
        if not k.startswith("_") and k not in _INPUT_KEYS
    }

    gen_path.parent.mkdir(parents=True, exist_ok=True)
    with open(gen_path, "w") as f:
        yaml.dump(generated, f, default_flow_style=False, sort_keys=False, allow_unicode=True, width=120)

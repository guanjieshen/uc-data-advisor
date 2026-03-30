"""MLflow tracing for multi-agent observability."""

import os
import logging

logger = logging.getLogger(__name__)


def init_tracing():
    """Initialize MLflow tracing. Call once at app startup."""
    try:
        import mlflow

        experiment_name = os.environ.get("MLFLOW_EXPERIMENT_NAME", "/uc-data-advisor-traces")
        mlflow.set_experiment(experiment_name)
        mlflow.tracing.enable()
        logger.info(f"MLflow tracing enabled with experiment: {experiment_name}")
    except Exception as e:
        logger.warning(f"MLflow tracing initialization failed: {e}")

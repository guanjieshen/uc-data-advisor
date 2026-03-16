"""MLflow tracing for multi-agent observability."""

import os
import logging
import functools
from typing import Optional

logger = logging.getLogger(__name__)

_initialized = False


def init_tracing():
    """Initialize MLflow tracing. Call once at app startup."""
    global _initialized
    if _initialized:
        return

    try:
        import mlflow

        experiment_name = os.environ.get("MLFLOW_EXPERIMENT_NAME", "/uc-data-advisor-traces")
        mlflow.set_experiment(experiment_name)
        mlflow.tracing.enable()
        _initialized = True
        logger.info(f"MLflow tracing enabled with experiment: {experiment_name}")
    except Exception as e:
        logger.warning(f"MLflow tracing initialization failed: {e}")


def trace_agent(agent_name: Optional[str] = None):
    """Decorator to trace agent execution with MLflow spans.

    Usage:
        @trace_agent("discovery")
        async def run(self, messages):
            ...
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            if not _initialized:
                return await func(*args, **kwargs)

            try:
                import mlflow

                name = agent_name or func.__qualname__
                with mlflow.start_span(name=name) as span:
                    span.set_attribute("agent.name", name)

                    # Log input message count
                    messages = args[1] if len(args) > 1 else kwargs.get("messages", [])
                    span.set_attribute("input.message_count", len(messages))

                    result = await func(*args, **kwargs)

                    # Log output length
                    if isinstance(result, str):
                        span.set_attribute("output.length", len(result))

                    return result
            except Exception:
                # Tracing failure should never break the app
                return await func(*args, **kwargs)

        return wrapper
    return decorator

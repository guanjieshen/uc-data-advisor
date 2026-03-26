"""Multi-agent architecture for UC Data Advisor.

Imports are lazy to avoid pulling in heavyweight dependencies (openai, etc.)
when only individual agent classes are needed (e.g., MLflow model registration).
"""


def __getattr__(name):
    if name == "Orchestrator":
        from .orchestrator import Orchestrator
        return Orchestrator
    if name == "DiscoveryAgent":
        from .discovery import DiscoveryAgent
        return DiscoveryAgent
    if name == "MetricsAgent":
        from .metrics import MetricsAgent
        return MetricsAgent
    if name == "QAAgent":
        from .qa import QAAgent
        return QAAgent
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["Orchestrator", "DiscoveryAgent", "MetricsAgent", "QAAgent"]

"""Generate agent system prompts from domain context and templates."""

from .templates import (
    CLASSIFY_TEMPLATE,
    GENERAL_TEMPLATE,
    DISCOVERY_TEMPLATE,
    METRICS_TEMPLATE,
    QA_TEMPLATE,
)


def generate_prompts(config: dict) -> dict[str, str]:
    """Build all 5 agent system prompts from domain context."""
    domain = config.get("generated", {}).get("domain", {})
    org_name = domain.get("organization_name", "")
    data_domains = domain.get("data_domains", [])

    org_clause = f" at {org_name}" if org_name else ""

    if data_domains:
        domain_context = "The workspace contains data including:\n" + "\n".join(
            f"- {d}" for d in data_domains
        )
    else:
        domain_context = ""

    return {
        "classify": CLASSIFY_TEMPLATE.format(org_clause=org_clause).strip(),
        "general": GENERAL_TEMPLATE.format(org_clause=org_clause).strip(),
        "discovery": DISCOVERY_TEMPLATE.format(org_clause=org_clause, domain_context=domain_context).strip(),
        "metrics": METRICS_TEMPLATE.format(org_clause=org_clause, domain_context=domain_context).strip(),
        "qa": QA_TEMPLATE.format(org_clause=org_clause).strip(),
    }

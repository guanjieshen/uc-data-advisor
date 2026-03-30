"""Generate benchmark test questions from metadata audit and generated content."""


def generate_benchmarks(config: dict) -> list[dict]:
    """Build benchmark questions from actual metadata."""
    audit = config.get("generated", {}).get("audit", {})
    metric_views = config.get("generated", {}).get("metric_views", {})
    knowledge_base = config.get("generated", {}).get("knowledge_base", [])
    tables = audit.get("tables", [])

    benchmarks = []

    # Discovery questions
    benchmarks.append({
        "question": "What catalogs are available in the workspace?",
        "expected_agent": "discovery",
        "expect_contains": ["catalog"],
        "category": "discovery",
    })

    # Table-specific discovery questions
    notable_tables = [t for t in tables if t.get("comment")]
    if notable_tables:
        t = notable_tables[0]
        benchmarks.append({
            "question": f"Do we have any data about {t['comment'].lower().split('.')[0]}?",
            "expected_agent": "discovery",
            "expect_contains": [],
            "category": "discovery",
        })

    if len(tables) > 1:
        t = tables[min(1, len(tables) - 1)]
        benchmarks.append({
            "question": f"Show me the columns in the {t['name']} table",
            "expected_agent": "discovery",
            "expect_contains": [],
            "category": "discovery",
        })

    # Metrics questions — from metric views if available, otherwise from source tables
    metrics_added = 0
    for view_name, view_sql in list(metric_views.items())[:2]:
        measure_name = _extract_first_count_measure(view_sql)
        if measure_name:
            benchmarks.append({
                "question": f"What is the {measure_name.lower()}?",
                "expected_agent": "metrics",
                "expect_contains": [],
                "category": "metrics",
            })
            metrics_added += 1

    if metrics_added == 0 and tables:
        # Generate metrics questions from source tables
        # Use analytical phrasing to avoid discovery misclassification
        numeric_cols = [c for c in tables[0].get("columns", []) if c.get("type") in ("bigint", "int", "double", "float", "decimal")]
        if numeric_cols:
            benchmarks.append({
                "question": f"What is the average {numeric_cols[0]['name']} across all records?",
                "expected_agent": "metrics",
                "expect_contains": [],
                "category": "metrics",
            })
        else:
            benchmarks.append({
                "question": f"Give me a count breakdown by month from {tables[0]['name']}",
                "expected_agent": "metrics",
                "expect_contains": [],
                "category": "metrics",
            })
        if len(tables) > 2:
            t = tables[2]
            cols = [c["name"] for c in t.get("columns", []) if c.get("type") in ("string", "varchar")]
            if cols:
                benchmarks.append({
                    "question": f"Show me the distinct values of {cols[0]} in {t['name']}",
                    "expected_agent": "metrics",
                    "expect_contains": [],
                    "category": "metrics",
                })

    # QA questions (from knowledge base)
    qa_questions = [faq for faq in knowledge_base if faq.get("category") in ("access", "quality", "general")]
    for faq in qa_questions[:3]:
        benchmarks.append({
            "question": faq["question"],
            "expected_agent": "qa",
            "expect_contains": [],
            "category": "qa",
        })

    # General
    benchmarks.append({
        "question": "Hello, what can you help me with?",
        "expected_agent": "general",
        "expect_contains": [],
        "category": "general",
    })

    return benchmarks


def generate_ui(config: dict) -> dict:
    """Generate UI configuration (header subtitle, suggestions)."""
    domain = config.get("generated", {}).get("domain", {})
    audit = config.get("generated", {}).get("audit", {})
    tables = audit.get("tables", [])

    org_name = domain.get("organization_name", "")
    header = f"{org_name} Unity Catalog" if org_name else "Unity Catalog"

    suggestions = ["What catalogs are available?"]
    notable = [t for t in tables if t.get("comment")][:3]
    for t in notable:
        suggestions.append(f"Tell me about the {t['name']} table")

    schemas = audit.get("schemas", [])
    non_empty_schemas = [s for s in schemas if s.get("table_count", 0) > 0]
    if non_empty_schemas:
        suggestions.append(f"What data is in {non_empty_schemas[0]['full_name']}?")

    return {
        "header_subtitle": header,
        "suggestions": suggestions[:5],
    }


def generate_genie_tables(config: dict) -> list[str]:
    """Generate sorted table list for Genie Space.

    Includes all source tables + summary and monthly metric tables only
    (not per-dimension tables, to keep the Genie Space focused).
    """
    audit = config.get("generated", {}).get("audit", {})
    metric_refreshes = config.get("generated", {}).get("metric_refreshes", [])

    tables = []

    # Add all source tables (non-views)
    for t in audit.get("tables", []):
        if t.get("table_type") not in ("VIEW", "FOREIGN"):
            tables.append(t["full_name"])

    # Add only summary and monthly metric tables (not per-dimension breakdowns)
    for r in metric_refreshes:
        name = r["target_table"]
        if name.endswith("_summary") or name.endswith("_monthly"):
            tables.append(name)

    return sorted(set(tables))


def _extract_first_count_measure(view_sql: str) -> str:
    """Extract the first COUNT-based measure name from metric view YAML."""
    import re
    # Look for "- name: Total Something\n    expr: COUNT(1)"
    matches = re.findall(r"- name: (Total \w[\w ]*)", view_sql)
    if matches:
        return matches[0]
    return ""

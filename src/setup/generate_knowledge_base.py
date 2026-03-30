"""Generate knowledge base FAQ entries from metadata audit."""


# Static governance FAQs — generic, work for any deployment
STATIC_FAQS = [
    {
        "question": "How do I request access to a dataset?",
        "answer": "Submit a data access request through the Unity Catalog access request portal. Your request will be routed to the data steward for the relevant catalog. Access is granted based on your role and business justification. Typical turnaround is 2-3 business days.",
        "category": "access",
        "source": "Data Governance Policy",
    },
    {
        "question": "What is Unity Catalog?",
        "answer": "Unity Catalog is Databricks' unified governance solution for all data and AI assets. It provides centralized access control, auditing, lineage, and discovery across all workspaces. All datasets are organized in a three-level namespace: catalog.schema.table.",
        "category": "general",
        "source": "Platform Documentation",
    },
    {
        "question": "What is a data steward?",
        "answer": "A data steward is the designated owner responsible for data quality, access governance, and metadata accuracy within a specific catalog or schema. Data stewards review access requests and ensure compliance with data policies.",
        "category": "governance",
        "source": "Data Governance Policy",
    },
    {
        "question": "What data quality checks are in place?",
        "answer": "All ingested datasets go through automated data quality checks including: null validation on required fields, range checks for sensor values, referential integrity across related tables, and timeliness checks. Quality metrics are published to the data quality dashboard.",
        "category": "quality",
        "source": "Data Quality Framework",
    },
    {
        "question": "Can I create my own tables in Unity Catalog?",
        "answer": "Users with the CREATE TABLE privilege can create tables in designated sandbox schemas. For production datasets, submit a request to the Data Platform team with your schema design and data source details. All production tables must have column descriptions and a data steward assigned.",
        "category": "access",
        "source": "Self-Service Data Guidelines",
    },
    {
        "question": "How do I report a data quality issue?",
        "answer": "Report data quality issues through the Data Quality portal or by contacting the data steward listed in the table's metadata. Include the fully qualified table name, specific columns affected, and a description of the issue.",
        "category": "quality",
        "source": "Data Incident Response",
    },
    {
        "question": "What is data lineage and how can I view it?",
        "answer": "Data lineage tracks how data flows from source to destination, including all transformations. In Unity Catalog, you can view lineage for any table by clicking the Lineage tab in the catalog explorer. This shows upstream sources and downstream consumers.",
        "category": "governance",
        "source": "Platform Documentation",
    },
    {
        "question": "How do I connect to the data from my tools?",
        "answer": "You can access Unity Catalog data through: Databricks notebooks and SQL editor (direct), Power BI and Tableau (via Databricks SQL connector), Python/R (via Databricks Connect or SQL connector), and REST APIs. All connections require authentication through your Databricks workspace credentials.",
        "category": "access",
        "source": "Data Access Guide",
    },
    {
        "question": "How is sensitive data protected?",
        "answer": "Sensitive data is protected through: column-level access controls in Unity Catalog, dynamic data masking for PII fields, encryption at rest and in transit, audit logging of all data access, and row-level security where required. Data classification labels are applied to all columns containing sensitive information.",
        "category": "security",
        "source": "Data Security Policy",
    },
    {
        "question": "Who do I contact for help with the data platform?",
        "answer": "For platform issues: contact the Data Platform team via ServiceNow or the data-platform Slack channel. For data access: contact the relevant data steward. For data quality issues: use the Data Quality portal. For urgent production issues: page the on-call data engineer.",
        "category": "support",
        "source": "Support Directory",
    },
]


def generate_knowledge_base(config: dict) -> list[dict]:
    """Generate FAQ entries from static templates + dynamic catalog metadata."""
    domain = config.get("generated", {}).get("domain", {})
    audit = config.get("generated", {}).get("audit", {})
    org_name = domain.get("organization_name", "")
    catalogs = audit.get("catalogs", [])
    schemas = audit.get("schemas", [])
    tables = audit.get("tables", [])

    faqs = list(STATIC_FAQS)

    # Dynamic: "How is the data organized?"
    if catalogs:
        catalog_desc_parts = []
        for cat in catalogs:
            summary = domain.get("catalog_summaries", {}).get(cat["name"], "")
            schema_names = [s["name"] for s in schemas if s["catalog_name"] == cat["name"]]
            if summary:
                catalog_desc_parts.append(f"{cat['name']} ({summary})")
            else:
                catalog_desc_parts.append(f"{cat['name']} ({', '.join(schema_names)})")

        org_clause = f"{org_name} data" if org_name else "Data"
        faqs.append({
            "question": "How is the data organized?",
            "answer": f"{org_clause} in Unity Catalog follows a domain-based catalog structure. The main catalogs are: {'; '.join(catalog_desc_parts)}.",
            "category": "general",
            "source": "Auto-generated from UC metadata",
        })

    # Dynamic: per-catalog FAQs
    for cat in catalogs:
        schema_entries = [s for s in schemas if s["catalog_name"] == cat["name"]]
        schema_list = ", ".join(
            f"{s['name']} ({s['comment']})" if s["comment"] else s["name"]
            for s in schema_entries
        )
        comment_clause = f" — {cat['comment']}" if cat["comment"] else ""
        faqs.append({
            "question": f"What data is in the {cat['name']} catalog?",
            "answer": f"The {cat['name']} catalog{comment_clause} contains {len(schema_entries)} schemas: {schema_list}. It has {cat['table_count']} tables total.",
            "category": "data_catalog",
            "source": "Auto-generated from UC metadata",
        })

    # Dynamic: per notable table FAQs (tables with comments, limit to 10)
    notable_tables = [t for t in tables if t["comment"]][:10]
    for table in notable_tables:
        col_summary = ", ".join(c["name"] for c in table["columns"][:5])
        if len(table["columns"]) > 5:
            col_summary += f", and {len(table['columns']) - 5} more"
        faqs.append({
            "question": f"What is the {table['name']} table?",
            "answer": f"{table['full_name']}: {table['comment']}. It has {len(table['columns'])} columns including: {col_summary}.",
            "category": "data_catalog",
            "source": "Auto-generated from UC metadata",
        })

    return faqs

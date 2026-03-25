"""Infer organization name and domain context from catalog metadata."""



def generate_domain(config: dict) -> dict:
    """Derive domain context from audit results."""
    audit = config.get("generated", {}).get("audit", {})
    catalogs = audit.get("catalogs", [])
    schemas = audit.get("schemas", [])
    source_catalogs = config.get("source_catalogs", [])

    # 1. Organization name: common prefix of catalog names
    org_name = _infer_org_name(source_catalogs)

    # 2. Domain description from catalog comments
    cat_comments = [c["comment"] for c in catalogs if c["comment"]]
    if cat_comments:
        domain_desc = "; ".join(cat_comments)
    else:
        domain_desc = f"data across {len(catalogs)} catalogs"

    # 3. Data domains from schema comments or humanized names
    data_domains = []
    for schema in schemas:
        if schema["comment"]:
            data_domains.append(schema["comment"])
        else:
            data_domains.append(_humanize(schema["name"]))

    # 4. Catalog summaries
    catalog_summaries = {}
    for cat in catalogs:
        if cat["comment"]:
            catalog_summaries[cat["name"]] = cat["comment"]
        else:
            schema_names = [s["name"] for s in schemas if s["catalog_name"] == cat["name"]]
            catalog_summaries[cat["name"]] = f"Contains {len(schema_names)} schemas: {', '.join(schema_names)}"

    return {
        "organization_name": org_name,
        "domain_description": domain_desc,
        "data_domains": data_domains,
        "catalog_summaries": catalog_summaries,
    }


def _infer_org_name(catalog_names: list[str]) -> str:
    """Find common prefix across catalog names to infer org name."""
    if not catalog_names:
        return ""

    # Split on underscores and find common prefix parts
    parts_list = [name.split("_") for name in catalog_names]
    if len(parts_list) == 1:
        # Single catalog: use first part
        return _titlecase(parts_list[0][0])

    # Find common prefix parts
    common = []
    for i, part in enumerate(parts_list[0]):
        if all(len(parts) > i and parts[i] == part for parts in parts_list):
            common.append(part)
        else:
            break

    if common:
        return _titlecase("_".join(common))

    # No common prefix — use first catalog's first part
    return _titlecase(parts_list[0][0])


def _titlecase(s: str) -> str:
    """Convert snake_case to Title Case."""
    return " ".join(word.capitalize() for word in s.split("_"))


def _humanize(schema_name: str) -> str:
    """Convert schema_name to human-readable description."""
    return schema_name.replace("_", " ").capitalize()

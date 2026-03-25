"""Generate Databricks Metric View YAML definitions from table column analysis.

Classifies columns as measures (numeric), dimensions (string/enum), or
time dimensions (date/timestamp). Detects FK relationships for joins.
"""

import re
import logging

logger = logging.getLogger(__name__)

MAX_MEASURES_PER_VIEW = 10
MAX_DIMENSIONS_PER_VIEW = 8

# Column name patterns that indicate IDs (not measures)
ID_PATTERNS = re.compile(r"^(id|.*_id)$", re.IGNORECASE)

# Column name patterns that indicate long text (not dimensions)
TEXT_PATTERNS = re.compile(r".*(description|notes|comment|text|remarks|raw_log|content)$", re.IGNORECASE)

# Numeric SQL types
NUMERIC_TYPES = {"INT", "LONG", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL", "DOUBLE", "FLOAT", "NUMERIC", "INTEGER"}

# Time SQL types
TIME_TYPES = {"DATE", "TIMESTAMP", "TIMESTAMP_NTZ"}


def generate_metric_views(config: dict) -> tuple[dict, list]:
    """Generate metric view YAML + materialization refresh queries.

    Returns (metric_views dict, metric_refreshes list).
    """
    audit = config.get("generated", {}).get("audit", {})
    tables = audit.get("tables", [])
    advisor_catalog = config.get("infrastructure", {}).get("advisor_catalog", "uc_data_advisor")
    advisor_schema = config.get("infrastructure", {}).get("advisor_schema", "default")

    metric_views = {}
    metric_refreshes = []

    for table in tables:
        # Skip views and foreign tables
        if table.get("table_type") in ("VIEW", "FOREIGN"):
            continue

        dims, measures, time_dims = _classify_columns(table)

        if not measures or not (dims or time_dims):
            continue

        # Always add a row count as first measure
        table_label = _humanize(table["name"])
        measures.insert(0, {"name": f"Total {table_label}", "expr": "COUNT(1)"})

        # Cap measures and dimensions
        measures = measures[:MAX_MEASURES_PER_VIEW]
        dims = dims[:MAX_DIMENSIONS_PER_VIEW]

        # Detect FK joins
        joins = _detect_joins(table, tables)

        view_name = f"mv_{table['name']}"
        view_fqn = f"{advisor_catalog}.{advisor_schema}.{view_name}"

        yaml_sql = _build_metric_view_sql(
            view_fqn=view_fqn,
            comment=f"Metrics for {table['full_name']}",
            source=table["full_name"],
            dimensions=dims + time_dims,
            measures=measures,
            joins=joins,
        )
        metric_views[view_name] = yaml_sql

        # Generate refresh queries
        refreshes = _build_refresh_queries(view_name, view_fqn, dims, measures, time_dims, config)
        metric_refreshes.extend(refreshes)

    return metric_views, metric_refreshes


def _classify_columns(table: dict) -> tuple[list, list, list]:
    """Classify table columns into dimensions, measures, and time dimensions."""
    dims = []
    measures = []
    time_dims = []

    for col in table.get("columns", []):
        col_name = col["name"]
        col_type = col.get("type", "STRING").upper().split("(")[0].strip()

        # Skip ID columns
        if ID_PATTERNS.match(col_name):
            continue

        # Time dimensions
        if col_type in TIME_TYPES:
            time_dims.append({
                "name": f"{_humanize(col_name)} Month",
                "expr": f"DATE_TRUNC('MONTH', {col_name})",
                "comment": col.get("comment", ""),
            })
            continue

        # Numeric → measures
        if col_type in NUMERIC_TYPES:
            label = _humanize(col_name)
            measures.append({"name": f"Total {label}", "expr": f"SUM({col_name})"})
            measures.append({"name": f"Avg {label}", "expr": f"ROUND(AVG({col_name}), 2)"})
            continue

        # Boolean → dimension + count filter
        if col_type == "BOOLEAN":
            label = _humanize(col_name)
            dims.append({"name": label, "expr": col_name, "comment": col.get("comment", "")})
            measures.append({
                "name": f"{label} Count",
                "expr": f"COUNT(1) FILTER (WHERE {col_name} = true)",
            })
            continue

        # String → dimension (skip long text fields)
        if col_type == "STRING" and not TEXT_PATTERNS.match(col_name):
            dims.append({
                "name": _humanize(col_name),
                "expr": col_name,
                "comment": col.get("comment", ""),
            })

    return dims, measures, time_dims


def _detect_joins(table: dict, all_tables: list) -> list:
    """Detect FK relationships from column comments and naming patterns."""
    joins = []
    seen_targets = set()

    for col in table.get("columns", []):
        col_name = col["name"]
        comment = col.get("comment", "")

        # Pattern 1: column comment contains "FK to {table}"
        if comment and "FK to" in comment:
            match = re.search(r"FK to\s+(\S+)", comment)
            if match:
                target = match.group(1).strip("()")
                # Find the target table
                for candidate in all_tables:
                    if candidate["full_name"] == target or candidate["name"] in target:
                        if candidate["full_name"] not in seen_targets:
                            seen_targets.add(candidate["full_name"])
                            joins.append({
                                "name": candidate["name"],
                                "source": candidate["full_name"],
                                "using": [col_name],
                            })
                        break

        # Pattern 2: column ending in _id → match to table
        elif col_name.endswith("_id") and col_name != "id":
            entity = col_name.removesuffix("_id")
            for candidate in all_tables:
                if candidate["full_name"] == table["full_name"]:
                    continue
                if candidate["name"] in (entity, f"{entity}s", f"{entity}es"):
                    if any(c["name"] == col_name for c in candidate.get("columns", [])):
                        if candidate["full_name"] not in seen_targets:
                            seen_targets.add(candidate["full_name"])
                            joins.append({
                                "name": candidate["name"],
                                "source": candidate["full_name"],
                                "using": [col_name],
                            })
                        break

    return joins


def _build_metric_view_sql(view_fqn, comment, source, dimensions, measures, joins):
    """Build CREATE VIEW ... WITH METRICS LANGUAGE YAML SQL."""
    yaml_lines = [
        "version: 1.1",
        f'comment: "{comment}"',
        f"source: {source}",
    ]

    if joins:
        yaml_lines.append("joins:")
        for j in joins:
            yaml_lines.append(f"  - name: {j['name']}")
            yaml_lines.append(f"    source: {j['source']}")
            yaml_lines.append(f"    using:")
            for col in j["using"]:
                yaml_lines.append(f"      - {col}")

    if dimensions:
        yaml_lines.append("dimensions:")
        for d in dimensions:
            yaml_lines.append(f"  - name: {d['name']}")
            expr = d["expr"]
            if any(c in expr for c in ":'\""):
                yaml_lines.append(f'    expr: "{expr}"')
            else:
                yaml_lines.append(f"    expr: {expr}")
            if d.get("comment"):
                yaml_lines.append(f'    comment: "{d["comment"]}"')

    if measures:
        yaml_lines.append("measures:")
        for m in measures:
            yaml_lines.append(f"  - name: {m['name']}")
            expr = m["expr"]
            if any(c in expr for c in ":'\""):
                yaml_lines.append(f'    expr: "{expr}"')
            else:
                yaml_lines.append(f"    expr: {expr}")

    yaml_body = "\n".join(yaml_lines)
    return f"CREATE OR REPLACE VIEW {view_fqn}\nWITH METRICS\nLANGUAGE YAML\nAS $$\n{yaml_body}\n$$"


def _build_refresh_queries(view_name, view_fqn, dims, measures, time_dims, config):
    """Build MEASURE() queries for materializing metric tables."""
    infra = config.get("infrastructure", {})
    # Metric tables go into a dedicated metrics schema in the first source catalog
    # (Genie can't read the advisor catalog, so we use source catalog namespace)
    source_catalogs = config.get("source_catalogs", [])
    app_name = infra.get("app_name", "advisor").replace("-", "_")
    metric_catalog = source_catalogs[0] if source_catalogs else infra.get("advisor_catalog", "uc_data_advisor")
    metric_schema = f"{app_name}_metrics"
    base_name = view_name.removeprefix("mv_")

    refreshes = []

    # Summary: all measures, no GROUP BY
    measure_cols = ", ".join(
        f"MEASURE(`{m['name']}`) AS {_snake_case(m['name'])}" for m in measures
    )
    refreshes.append({
        "target_table": f"{metric_catalog}.{metric_schema}.{base_name}_summary",
        "source_view": view_fqn,
        "query": f"SELECT {measure_cols} FROM {view_fqn}",
    })

    # Per string dimension
    for d in dims[:3]:  # Limit to top 3 dimensions
        dim_alias = _snake_case(d["name"])
        cols = f"`{d['name']}` AS {dim_alias}, " + ", ".join(
            f"MEASURE(`{m['name']}`) AS {_snake_case(m['name'])}" for m in measures
        )
        refreshes.append({
            "target_table": f"{metric_catalog}.{metric_schema}.{base_name}_by_{dim_alias}",
            "source_view": view_fqn,
            "query": f"SELECT {cols} FROM {view_fqn} GROUP BY `{d['name']}`",
        })

    # Monthly time series
    if time_dims:
        td = time_dims[0]
        td_alias = _snake_case(td["name"])
        cols = f"`{td['name']}` AS {td_alias}, " + ", ".join(
            f"MEASURE(`{m['name']}`) AS {_snake_case(m['name'])}" for m in measures
        )
        refreshes.append({
            "target_table": f"{metric_catalog}.{metric_schema}.{base_name}_monthly",
            "source_view": view_fqn,
            "query": f"SELECT {cols} FROM {view_fqn} GROUP BY `{td['name']}`",
        })

    return refreshes


def _humanize(name: str) -> str:
    """Convert snake_case to Title Case."""
    return " ".join(word.capitalize() for word in name.split("_"))


def _snake_case(name: str) -> str:
    """Convert Title Case or spaces to snake_case."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")

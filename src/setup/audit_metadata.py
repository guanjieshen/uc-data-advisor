"""Audit Unity Catalog metadata for source catalogs.

Walks catalogs → schemas → tables → columns via the Databricks SDK
and collects all metadata for downstream generation.
"""

import logging

logger = logging.getLogger(__name__)

SKIP_CATALOGS = {"system", "__databricks_internal", "samples", "information_schema"}
SKIP_SCHEMAS = {"information_schema", "pg_catalog", "__db_system", "default", "metrics"}


def audit(config: dict, w) -> dict:
    """Audit source catalogs and return metadata summary."""
    source_catalogs = config.get("source_catalogs", [])
    include_schemas = set(config.get("include_schemas", []))
    exclude_schemas = set(config.get("exclude_schemas", []))

    print("=" * 60)
    print("Metadata Audit")
    print("=" * 60)

    catalogs = []
    schemas = []
    tables = []

    for catalog_name in source_catalogs:
        try:
            cat = w.catalogs.get(catalog_name)
        except Exception as e:
            logger.warning(f"Cannot access catalog {catalog_name}: {e}")
            continue

        cat_record = {
            "name": cat.name,
            "comment": cat.comment or "",
            "owner": cat.owner or "",
            "schema_count": 0,
            "table_count": 0,
        }
        print(f"  Catalog: {cat.name}")

        try:
            cat_schemas = list(w.schemas.list(catalog_name=cat.name))
        except Exception as e:
            logger.warning(f"Cannot list schemas in {cat.name}: {e}")
            continue

        for schema in cat_schemas:
            if schema.name in SKIP_SCHEMAS:
                continue
            if include_schemas and schema.name not in include_schemas:
                continue
            if schema.name in exclude_schemas:
                continue

            full_schema = f"{cat.name}.{schema.name}"
            schema_record = {
                "catalog_name": cat.name,
                "name": schema.name,
                "full_name": full_schema,
                "comment": schema.comment or "",
                "table_count": 0,
            }

            try:
                schema_tables = list(w.tables.list(catalog_name=cat.name, schema_name=schema.name))
            except Exception as e:
                logger.warning(f"Cannot list tables in {full_schema}: {e}")
                schemas.append(schema_record)
                continue

            print(f"    Schema: {full_schema} ({len(schema_tables)} tables)")

            for tbl in schema_tables:
                full_name = tbl.full_name or f"{cat.name}.{schema.name}.{tbl.name}"

                # Get detailed table info for columns
                detail = tbl
                if not tbl.columns:
                    try:
                        detail = w.tables.get(full_name=full_name)
                    except Exception:
                        pass

                columns = []
                for col in (detail.columns or []):
                    columns.append({
                        "name": col.name,
                        "type": col.type_text or (str(col.type_name) if col.type_name else "STRING"),
                        "comment": col.comment or "",
                        "nullable": col.nullable if col.nullable is not None else True,
                    })

                table_type = ""
                if detail.table_type:
                    table_type = detail.table_type.value if hasattr(detail.table_type, "value") else str(detail.table_type)

                table_record = {
                    "catalog_name": cat.name,
                    "schema_name": schema.name,
                    "name": tbl.name,
                    "full_name": full_name,
                    "comment": detail.comment or "",
                    "table_type": table_type,
                    "columns": columns,
                }
                tables.append(table_record)

            schema_record["table_count"] = len(schema_tables)
            schemas.append(schema_record)
            cat_record["table_count"] += len(schema_tables)

        cat_record["schema_count"] = len([s for s in schemas if s["catalog_name"] == cat.name])
        catalogs.append(cat_record)

    # Compute coverage stats
    total_cols = sum(len(t["columns"]) for t in tables)
    cols_with_comments = sum(1 for t in tables for c in t["columns"] if c["comment"])
    tables_with_comments = sum(1 for t in tables if t["comment"])

    audit_result = {
        "catalogs": catalogs,
        "schemas": schemas,
        "tables": tables,
        "total_tables": len(tables),
        "total_columns": total_cols,
        "tables_with_comments": tables_with_comments,
        "columns_with_comments": cols_with_comments,
        "description_coverage_pct": round(
            100.0 * (tables_with_comments + cols_with_comments) / max(len(tables) + total_cols, 1), 1
        ),
    }

    print()
    print(f"  Total: {len(catalogs)} catalogs, {len(schemas)} schemas, {len(tables)} tables, {total_cols} columns")
    print(f"  Description coverage: {audit_result['description_coverage_pct']}%")

    return audit_result

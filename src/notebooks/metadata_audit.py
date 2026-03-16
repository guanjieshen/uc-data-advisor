#!/usr/bin/env python3
"""
UC Metadata Audit Script

Implements Story #22: Audit current Unity Catalog metadata coverage

This script analyzes the Unity Catalog metadata in the workspace to understand:
- Total catalogs, schemas, tables
- Description/comment coverage
- Ownership coverage
- Tag coverage

Output informs the design decisions in Story #21.
"""

import json
from collections import defaultdict
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def run_sql(w: WorkspaceClient, warehouse_id: str, query: str) -> list[dict]:
    """Execute SQL and return results as list of dicts."""
    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=query,
        wait_timeout="50s"
    )

    if response.status.state != StatementState.SUCCEEDED:
        print(f"Query failed: {response.status.error}")
        return []

    if not response.result or not response.result.data_array:
        return []

    columns = [col.name for col in response.manifest.schema.columns]
    rows = []
    for row in response.result.data_array:
        rows.append(dict(zip(columns, row)))
    return rows


def main():
    # Initialize client
    w = WorkspaceClient(profile="fevm-cjc")
    warehouse_id = "751fe324525584e5"

    print("=" * 60)
    print("UC METADATA AUDIT REPORT")
    print("=" * 60)
    print()

    # 1. Catalog overview
    print("## CATALOGS")
    print("-" * 40)

    catalogs = run_sql(w, warehouse_id, """
        SELECT
            catalog_name,
            catalog_owner,
            CASE WHEN comment IS NOT NULL AND comment != '' THEN 'Yes' ELSE 'No' END as has_description
        FROM system.information_schema.catalogs
        WHERE catalog_name NOT IN ('system', '__databricks_internal')
        ORDER BY catalog_name
    """)

    for cat in catalogs:
        print(f"  {cat['catalog_name']}")
        print(f"    Owner: {cat['catalog_owner']}")
        print(f"    Has Description: {cat['has_description']}")

    print(f"\nTotal catalogs: {len(catalogs)}")
    print()

    # 2. Schema overview
    print("## SCHEMAS")
    print("-" * 40)

    schemas = run_sql(w, warehouse_id, """
        SELECT
            catalog_name,
            schema_name,
            schema_owner,
            CASE WHEN comment IS NOT NULL AND comment != '' THEN 1 ELSE 0 END as has_description
        FROM system.information_schema.schemata
        WHERE catalog_name NOT IN ('system', '__databricks_internal')
          AND schema_name NOT IN ('information_schema', 'default')
        ORDER BY catalog_name, schema_name
    """)

    schemas_with_desc = sum(1 for s in schemas if s['has_description'] == '1')
    print(f"Total schemas: {len(schemas)}")
    print(f"With descriptions: {schemas_with_desc} ({100*schemas_with_desc/max(len(schemas),1):.1f}%)")
    print()

    # 3. Table overview
    print("## TABLES")
    print("-" * 40)

    tables = run_sql(w, warehouse_id, """
        SELECT
            table_catalog,
            table_schema,
            table_name,
            table_type,
            table_owner,
            CASE WHEN comment IS NOT NULL AND comment != '' THEN 1 ELSE 0 END as has_description,
            created,
            last_altered
        FROM system.information_schema.tables
        WHERE table_catalog NOT IN ('system', '__databricks_internal')
          AND table_schema NOT IN ('information_schema', 'default')
        ORDER BY table_catalog, table_schema, table_name
    """)

    tables_with_desc = sum(1 for t in tables if t['has_description'] == '1')
    tables_with_owner = sum(1 for t in tables if t['table_owner'])

    print(f"Total tables: {len(tables)}")
    print(f"With descriptions: {tables_with_desc} ({100*tables_with_desc/max(len(tables),1):.1f}%)")
    print(f"With owners: {tables_with_owner} ({100*tables_with_owner/max(len(tables),1):.1f}%)")

    # Group by type
    by_type = defaultdict(int)
    for t in tables:
        by_type[t['table_type']] += 1
    print("\nBy type:")
    for ttype, count in sorted(by_type.items()):
        print(f"  {ttype}: {count}")
    print()

    # 4. Column overview
    print("## COLUMNS")
    print("-" * 40)

    columns = run_sql(w, warehouse_id, """
        SELECT
            COUNT(*) as total_columns,
            SUM(CASE WHEN comment IS NOT NULL AND comment != '' THEN 1 ELSE 0 END) as with_description
        FROM system.information_schema.columns
        WHERE table_catalog NOT IN ('system', '__databricks_internal')
          AND table_schema NOT IN ('information_schema', 'default')
    """)

    if columns:
        total_cols = int(columns[0]['total_columns'] or 0)
        cols_with_desc = int(columns[0]['with_description'] or 0)
        print(f"Total columns: {total_cols}")
        print(f"With descriptions: {cols_with_desc} ({100*cols_with_desc/max(total_cols,1):.1f}%)")
    print()

    # 5. Tags overview
    print("## TAGS")
    print("-" * 40)

    try:
        tags = run_sql(w, warehouse_id, """
            SELECT
                securable_type,
                COUNT(DISTINCT CONCAT(catalog_name, '.', schema_name, '.', securable_name)) as tagged_objects,
                COUNT(DISTINCT tag_name) as unique_tags
            FROM system.information_schema.tag_assignments
            WHERE catalog_name NOT IN ('system', '__databricks_internal')
            GROUP BY securable_type
        """)

        if tags:
            for tag in tags:
                print(f"  {tag['securable_type']}: {tag['tagged_objects']} objects with {tag['unique_tags']} unique tags")
        else:
            print("  No tags found")
    except Exception as e:
        print(f"  Tags not available: {e}")
    print()

    # 6. Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print()
    print("Metadata Coverage:")
    print(f"  - Catalogs: {len(catalogs)}")
    print(f"  - Schemas: {len(schemas)} ({schemas_with_desc} with descriptions)")
    print(f"  - Tables: {len(tables)} ({tables_with_desc} with descriptions)")
    if columns:
        print(f"  - Columns: {total_cols} ({cols_with_desc} with descriptions)")
    print()

    # Calculate overall coverage
    total_objects = len(catalogs) + len(schemas) + len(tables)
    described_objects = sum(1 for c in catalogs if c['has_description'] == 'Yes') + schemas_with_desc + tables_with_desc

    print(f"Overall description coverage: {100*described_objects/max(total_objects,1):.1f}%")
    print()

    # Recommendations
    print("Recommendations for Design (#21):")
    if len(tables) == 0:
        print("  - No user tables found - consider creating sample data")
    if tables_with_desc / max(len(tables), 1) < 0.5:
        print("  - Low description coverage - consider AI-generated metadata (#20)")
    if columns and cols_with_desc / max(total_cols, 1) < 0.3:
        print("  - Low column description coverage - fuzzy search will be limited")
    print("  - Use SDK API for metadata access (system.information_schema available)")
    print()


if __name__ == "__main__":
    main()

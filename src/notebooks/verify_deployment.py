# Databricks notebook source
# MAGIC %md
# MAGIC # Verify UC Data Advisor Deployment
# MAGIC
# MAGIC This notebook verifies that the UC Data Advisor bundle deployed correctly.

# COMMAND ----------

# MAGIC %pip install databricks-sdk --quiet

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import json

# COMMAND ----------

# Initialize client
w = WorkspaceClient()

print("=" * 60)
print("UC DATA ADVISOR DEPLOYMENT VERIFICATION")
print("=" * 60)

# COMMAND ----------

# Check if test tables exist
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"\nChecking tables in {catalog}.{schema}...")

try:
    tables = list(w.tables.list(catalog_name=catalog, schema_name=schema))
    print(f"Found {len(tables)} tables:")
    for t in tables:
        print(f"  - {t.name}: {t.comment or '(no description)'}")
except Exception as e:
    print(f"Error listing tables: {e}")

# COMMAND ----------

# Test metadata access
print("\nTesting metadata access...")

try:
    for table in tables[:3]:  # Check first 3 tables
        details = w.tables.get(full_name=table.full_name)
        print(f"\n{details.full_name}:")
        print(f"  Owner: {details.owner}")
        print(f"  Type: {details.table_type}")
        print(f"  Columns: {len(details.columns or [])}")
except Exception as e:
    print(f"Error getting table details: {e}")

# COMMAND ----------

# Summary
print("\n" + "=" * 60)
print("VERIFICATION COMPLETE")
print("=" * 60)

if len(tables) > 0:
    print("✅ Deployment verified successfully")
    dbutils.notebook.exit("SUCCESS")
else:
    print("⚠️ No tables found - check deployment")
    dbutils.notebook.exit("NO_TABLES")

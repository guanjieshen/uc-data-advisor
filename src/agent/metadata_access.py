"""Secure access layer for Unity Catalog metadata.

This module provides the data access layer for querying UC metadata
including catalogs, schemas, tables, columns, and lineage information.

Implements Story #3: Set up secure access to Unity Catalog metadata
"""

from databricks.sdk import WorkspaceClient
from pydantic import BaseModel


class UCMetadataClient:
    """Client for accessing Unity Catalog metadata securely."""

    def __init__(self, profile: str | None = None):
        """Initialize the UC metadata client.

        Args:
            profile: Databricks profile name. If None, uses default.
        """
        self.client = WorkspaceClient(profile=profile)

    def list_catalogs(self) -> list[dict]:
        """List all accessible catalogs.

        Returns:
            List of catalog metadata dictionaries.
        """
        catalogs = []
        for catalog in self.client.catalogs.list():
            catalogs.append({
                "name": catalog.name,
                "owner": catalog.owner,
                "comment": catalog.comment,
            })
        return catalogs

    def list_schemas(self, catalog_name: str) -> list[dict]:
        """List schemas in a catalog.

        Args:
            catalog_name: Name of the catalog to query.

        Returns:
            List of schema metadata dictionaries.
        """
        schemas = []
        for schema in self.client.schemas.list(catalog_name=catalog_name):
            schemas.append({
                "name": schema.name,
                "catalog_name": schema.catalog_name,
                "owner": schema.owner,
                "comment": schema.comment,
            })
        return schemas

    def list_tables(self, catalog_name: str, schema_name: str) -> list[dict]:
        """List tables in a schema.

        Args:
            catalog_name: Name of the catalog.
            schema_name: Name of the schema.

        Returns:
            List of table metadata dictionaries.
        """
        tables = []
        for table in self.client.tables.list(
            catalog_name=catalog_name,
            schema_name=schema_name
        ):
            tables.append({
                "name": table.name,
                "full_name": table.full_name,
                "table_type": str(table.table_type),
                "owner": table.owner,
                "comment": table.comment,
            })
        return tables

    def get_table_details(self, full_name: str) -> dict | None:
        """Get detailed metadata for a specific table.

        Args:
            full_name: Fully qualified table name (catalog.schema.table).

        Returns:
            Table metadata dictionary or None if not found.
        """
        try:
            table = self.client.tables.get(full_name=full_name)
            return {
                "name": table.name,
                "full_name": table.full_name,
                "catalog_name": table.catalog_name,
                "schema_name": table.schema_name,
                "table_type": str(table.table_type),
                "owner": table.owner,
                "comment": table.comment,
                "created_at": str(table.created_at) if table.created_at else None,
                "updated_at": str(table.updated_at) if table.updated_at else None,
                "columns": [
                    {
                        "name": col.name,
                        "type": col.type_text,
                        "comment": col.comment,
                        "nullable": col.nullable,
                    }
                    for col in (table.columns or [])
                ],
            }
        except Exception:
            return None

"""Agent tools for UC Data Advisor.

Defines the tool functions that the agent can invoke to answer
user questions about Unity Catalog datasets.

Implements Stories #6 and #7: Dataset existence and detail flows
"""

from .metadata_access import UCMetadataClient
from .response_model import (
    DatasetMetadata,
    DatasetExistsResponse,
    DatasetSearchResult,
    ColumnMetadata,
)


def check_dataset_exists(
    query: str,
    client: UCMetadataClient,
    catalog_scope: str | None = None,
) -> DatasetExistsResponse:
    """Check if a dataset exists matching the query.

    Args:
        query: Natural language query or dataset name.
        client: UC metadata client instance.
        catalog_scope: Optional catalog to limit search.

    Returns:
        DatasetExistsResponse with match status and details.
    """
    # Simple implementation - exact match on table name
    # TODO: Add fuzzy matching in Story #5

    query_lower = query.lower().strip()

    # Check if query looks like a full path
    if "." in query:
        parts = query.split(".")
        if len(parts) == 3:
            details = client.get_table_details(query)
            if details:
                dataset = DatasetMetadata(
                    name=details["name"],
                    full_name=details["full_name"],
                    catalog_name=details["catalog_name"],
                    schema_name=details["schema_name"],
                    table_type=details.get("table_type"),
                    owner=details.get("owner"),
                    comment=details.get("comment"),
                    created_at=details.get("created_at"),
                    updated_at=details.get("updated_at"),
                    columns=[
                        ColumnMetadata(**col) for col in details.get("columns", [])
                    ],
                )
                return DatasetExistsResponse(
                    query=query,
                    exists=True,
                    dataset=dataset,
                    message=f"Found dataset: {dataset.full_name}",
                )

    # Search across catalogs for matching table names
    alternatives = []
    catalogs = client.list_catalogs()

    for catalog in catalogs:
        if catalog_scope and catalog["name"] != catalog_scope:
            continue

        try:
            schemas = client.list_schemas(catalog["name"])
            for schema in schemas:
                tables = client.list_tables(catalog["name"], schema["name"])
                for table in tables:
                    if query_lower in table["name"].lower():
                        alternatives.append(
                            DatasetMetadata(
                                name=table["name"],
                                full_name=table["full_name"],
                                catalog_name=catalog["name"],
                                schema_name=schema["name"],
                                table_type=table.get("table_type"),
                                owner=table.get("owner"),
                                comment=table.get("comment"),
                            )
                        )
                        if len(alternatives) >= 5:
                            break
        except Exception:
            continue

    if alternatives:
        return DatasetExistsResponse(
            query=query,
            exists=True,
            dataset=alternatives[0] if len(alternatives) == 1 else None,
            alternatives=alternatives,
            message=f"Found {len(alternatives)} dataset(s) matching '{query}'",
        )

    return DatasetExistsResponse(
        query=query,
        exists=False,
        message=f"No datasets found matching '{query}'. Try refining your search.",
    )


def get_dataset_details(
    full_name: str,
    client: UCMetadataClient,
) -> DatasetMetadata | None:
    """Get detailed metadata for a specific dataset.

    Args:
        full_name: Fully qualified table name.
        client: UC metadata client instance.

    Returns:
        DatasetMetadata or None if not found.
    """
    details = client.get_table_details(full_name)
    if not details:
        return None

    return DatasetMetadata(
        name=details["name"],
        full_name=details["full_name"],
        catalog_name=details["catalog_name"],
        schema_name=details["schema_name"],
        table_type=details.get("table_type"),
        owner=details.get("owner"),
        comment=details.get("comment"),
        created_at=details.get("created_at"),
        updated_at=details.get("updated_at"),
        columns=[
            ColumnMetadata(**col) for col in details.get("columns", [])
        ],
    )

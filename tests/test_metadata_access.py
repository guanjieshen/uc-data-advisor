"""Tests for metadata access layer."""

import pytest
from unittest.mock import MagicMock, patch
from src.agent.metadata_access import UCMetadataClient


class MockCatalog:
    def __init__(self, name, owner, comment):
        self.name = name
        self.owner = owner
        self.comment = comment


class MockSchema:
    def __init__(self, name, catalog_name, owner, comment):
        self.name = name
        self.catalog_name = catalog_name
        self.owner = owner
        self.comment = comment


class MockTable:
    def __init__(self, name, full_name, table_type, owner, comment):
        self.name = name
        self.full_name = full_name
        self.table_type = table_type
        self.owner = owner
        self.comment = comment


class MockColumn:
    def __init__(self, name, type_text, comment, nullable):
        self.name = name
        self.type_text = type_text
        self.comment = comment
        self.nullable = nullable


class MockTableDetail:
    def __init__(self):
        self.name = "customers"
        self.full_name = "main.sales.customers"
        self.catalog_name = "main"
        self.schema_name = "sales"
        self.table_type = "MANAGED"
        self.owner = "data_team@example.com"
        self.comment = "Customer master data"
        self.created_at = "2024-01-01T00:00:00Z"
        self.updated_at = "2024-06-01T00:00:00Z"
        self.columns = [
            MockColumn("id", "BIGINT", "Primary key", False),
            MockColumn("name", "STRING", None, True),
        ]


@patch("src.agent.metadata_access.WorkspaceClient")
def test_list_catalogs(mock_ws_class):
    """Test listing catalogs."""
    mock_ws = MagicMock()
    mock_ws_class.return_value = mock_ws

    mock_ws.catalogs.list.return_value = [
        MockCatalog("main", "admin@example.com", "Main catalog"),
        MockCatalog("dev", "dev@example.com", None),
    ]

    client = UCMetadataClient()
    catalogs = client.list_catalogs()

    assert len(catalogs) == 2
    assert catalogs[0]["name"] == "main"
    assert catalogs[0]["owner"] == "admin@example.com"
    assert catalogs[0]["comment"] == "Main catalog"
    assert catalogs[1]["comment"] is None


@patch("src.agent.metadata_access.WorkspaceClient")
def test_list_schemas(mock_ws_class):
    """Test listing schemas in a catalog."""
    mock_ws = MagicMock()
    mock_ws_class.return_value = mock_ws

    mock_ws.schemas.list.return_value = [
        MockSchema("sales", "main", "sales_team@example.com", "Sales data"),
        MockSchema("marketing", "main", "marketing@example.com", None),
    ]

    client = UCMetadataClient()
    schemas = client.list_schemas("main")

    assert len(schemas) == 2
    assert schemas[0]["name"] == "sales"
    assert schemas[0]["catalog_name"] == "main"
    mock_ws.schemas.list.assert_called_once_with(catalog_name="main")


@patch("src.agent.metadata_access.WorkspaceClient")
def test_list_tables(mock_ws_class):
    """Test listing tables in a schema."""
    mock_ws = MagicMock()
    mock_ws_class.return_value = mock_ws

    mock_ws.tables.list.return_value = [
        MockTable("customers", "main.sales.customers", "MANAGED", "owner@example.com", "Customer data"),
        MockTable("orders", "main.sales.orders", "MANAGED", "owner@example.com", None),
    ]

    client = UCMetadataClient()
    tables = client.list_tables("main", "sales")

    assert len(tables) == 2
    assert tables[0]["name"] == "customers"
    assert tables[0]["full_name"] == "main.sales.customers"
    mock_ws.tables.list.assert_called_once_with(catalog_name="main", schema_name="sales")


@patch("src.agent.metadata_access.WorkspaceClient")
def test_get_table_details(mock_ws_class):
    """Test getting detailed table metadata."""
    mock_ws = MagicMock()
    mock_ws_class.return_value = mock_ws

    mock_ws.tables.get.return_value = MockTableDetail()

    client = UCMetadataClient()
    details = client.get_table_details("main.sales.customers")

    assert details is not None
    assert details["name"] == "customers"
    assert details["full_name"] == "main.sales.customers"
    assert details["owner"] == "data_team@example.com"
    assert len(details["columns"]) == 2
    assert details["columns"][0]["name"] == "id"
    assert details["columns"][0]["type"] == "BIGINT"


@patch("src.agent.metadata_access.WorkspaceClient")
def test_get_table_details_not_found(mock_ws_class):
    """Test getting details for non-existent table."""
    mock_ws = MagicMock()
    mock_ws_class.return_value = mock_ws

    mock_ws.tables.get.side_effect = Exception("Table not found")

    client = UCMetadataClient()
    details = client.get_table_details("main.sales.nonexistent")

    assert details is None

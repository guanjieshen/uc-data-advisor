"""Tests for agent tools."""

import pytest
from unittest.mock import MagicMock, patch
from src.agent.tools import check_dataset_exists, get_dataset_details
from src.agent.metadata_access import UCMetadataClient


@pytest.fixture
def mock_client():
    """Create a mock UC metadata client."""
    client = MagicMock(spec=UCMetadataClient)
    return client


def test_check_dataset_exists_exact_match(mock_client):
    """Test finding dataset by exact full name."""
    mock_client.get_table_details.return_value = {
        "name": "customers",
        "full_name": "main.sales.customers",
        "catalog_name": "main",
        "schema_name": "sales",
        "table_type": "MANAGED",
        "owner": "data_team@example.com",
        "comment": "Customer master data",
        "columns": [],
    }

    response = check_dataset_exists("main.sales.customers", mock_client)

    assert response.exists is True
    assert response.dataset is not None
    assert response.dataset.full_name == "main.sales.customers"
    assert "Found dataset" in response.message


def test_check_dataset_exists_partial_match(mock_client):
    """Test finding dataset by partial name search."""
    mock_client.get_table_details.return_value = None
    mock_client.list_catalogs.return_value = [
        {"name": "main", "owner": "admin", "comment": None}
    ]
    mock_client.list_schemas.return_value = [
        {"name": "sales", "catalog_name": "main", "owner": "admin", "comment": None}
    ]
    mock_client.list_tables.return_value = [
        {"name": "customers", "full_name": "main.sales.customers", "table_type": "MANAGED", "owner": "admin", "comment": "Customer data"},
        {"name": "customer_orders", "full_name": "main.sales.customer_orders", "table_type": "MANAGED", "owner": "admin", "comment": None},
    ]

    response = check_dataset_exists("customer", mock_client)

    assert response.exists is True
    assert len(response.alternatives) == 2
    assert "2 dataset(s)" in response.message


def test_check_dataset_exists_not_found(mock_client):
    """Test when no matching dataset is found."""
    mock_client.get_table_details.return_value = None
    mock_client.list_catalogs.return_value = [
        {"name": "main", "owner": "admin", "comment": None}
    ]
    mock_client.list_schemas.return_value = [
        {"name": "sales", "catalog_name": "main", "owner": "admin", "comment": None}
    ]
    mock_client.list_tables.return_value = [
        {"name": "orders", "full_name": "main.sales.orders", "table_type": "MANAGED", "owner": "admin", "comment": None},
    ]

    response = check_dataset_exists("nonexistent", mock_client)

    assert response.exists is False
    assert response.dataset is None
    assert "No datasets found" in response.message


def test_check_dataset_exists_with_catalog_scope(mock_client):
    """Test searching within a specific catalog."""
    mock_client.get_table_details.return_value = None
    mock_client.list_catalogs.return_value = [
        {"name": "main", "owner": "admin", "comment": None},
        {"name": "dev", "owner": "admin", "comment": None},
    ]
    mock_client.list_schemas.return_value = [
        {"name": "sales", "catalog_name": "main", "owner": "admin", "comment": None}
    ]
    mock_client.list_tables.return_value = [
        {"name": "customers", "full_name": "main.sales.customers", "table_type": "MANAGED", "owner": "admin", "comment": None},
    ]

    response = check_dataset_exists("customers", mock_client, catalog_scope="main")

    assert response.exists is True
    # Should only search in 'main' catalog, not 'dev'
    mock_client.list_schemas.assert_called_once_with("main")


def test_get_dataset_details_found(mock_client):
    """Test getting dataset details when found."""
    mock_client.get_table_details.return_value = {
        "name": "customers",
        "full_name": "main.sales.customers",
        "catalog_name": "main",
        "schema_name": "sales",
        "table_type": "MANAGED",
        "owner": "data_team@example.com",
        "comment": "Customer master data",
        "created_at": "2024-01-01",
        "updated_at": "2024-06-01",
        "columns": [
            {"name": "id", "type": "BIGINT", "comment": "Primary key", "nullable": False},
        ],
    }

    result = get_dataset_details("main.sales.customers", mock_client)

    assert result is not None
    assert result.name == "customers"
    assert result.full_name == "main.sales.customers"
    assert len(result.columns) == 1


def test_get_dataset_details_not_found(mock_client):
    """Test getting dataset details when not found."""
    mock_client.get_table_details.return_value = None

    result = get_dataset_details("main.sales.nonexistent", mock_client)

    assert result is None

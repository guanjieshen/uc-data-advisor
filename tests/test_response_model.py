"""Tests for response model validation."""

import pytest
from src.agent.response_model import (
    ColumnMetadata,
    DatasetMetadata,
    DatasetExistsResponse,
)


def test_column_metadata_minimal():
    """Test ColumnMetadata with required fields only."""
    col = ColumnMetadata(name="id", type="BIGINT")
    assert col.name == "id"
    assert col.type == "BIGINT"
    assert col.comment is None


def test_dataset_metadata_full():
    """Test DatasetMetadata with all fields."""
    dataset = DatasetMetadata(
        name="customers",
        full_name="main.sales.customers",
        catalog_name="main",
        schema_name="sales",
        table_type="MANAGED",
        owner="data_team@example.com",
        comment="Customer master data",
        columns=[
            ColumnMetadata(name="id", type="BIGINT", comment="Primary key"),
            ColumnMetadata(name="name", type="STRING"),
        ],
    )
    assert dataset.full_name == "main.sales.customers"
    assert len(dataset.columns) == 2
    assert dataset.columns[0].comment == "Primary key"


def test_dataset_exists_response_found():
    """Test DatasetExistsResponse when dataset is found."""
    dataset = DatasetMetadata(
        name="orders",
        full_name="main.sales.orders",
        catalog_name="main",
        schema_name="sales",
    )
    response = DatasetExistsResponse(
        query="orders",
        exists=True,
        dataset=dataset,
        message="Found dataset: main.sales.orders",
    )
    assert response.exists is True
    assert response.dataset is not None
    assert response.dataset.full_name == "main.sales.orders"


def test_dataset_exists_response_not_found():
    """Test DatasetExistsResponse when dataset is not found."""
    response = DatasetExistsResponse(
        query="nonexistent",
        exists=False,
        message="No datasets found matching 'nonexistent'.",
    )
    assert response.exists is False
    assert response.dataset is None
    assert len(response.alternatives) == 0

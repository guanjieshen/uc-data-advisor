"""Metadata response model for dataset discovery.

Defines the standard response contracts for UC metadata queries,
ensuring consistent structure across all agent interfaces.

Implements Story #4: Define metadata model for dataset discovery responses
"""

from pydantic import BaseModel, Field


class ColumnMetadata(BaseModel):
    """Column-level metadata."""

    name: str = Field(description="Column name")
    type: str = Field(description="Column data type")
    comment: str | None = Field(default=None, description="Column description")
    nullable: bool | None = Field(default=None, description="Whether column allows nulls")


class DatasetMetadata(BaseModel):
    """Standard metadata response for a single dataset."""

    name: str = Field(description="Table name")
    full_name: str = Field(description="Fully qualified name (catalog.schema.table)")
    catalog_name: str = Field(description="Catalog name")
    schema_name: str = Field(description="Schema name")
    table_type: str | None = Field(default=None, description="Table type (MANAGED, EXTERNAL, VIEW)")
    owner: str | None = Field(default=None, description="Dataset owner")
    comment: str | None = Field(default=None, description="Table description")
    created_at: str | None = Field(default=None, description="Creation timestamp")
    updated_at: str | None = Field(default=None, description="Last update timestamp")
    columns: list[ColumnMetadata] = Field(default_factory=list, description="Column metadata")

    # Post-MVP fields (extensible)
    domain: str | None = Field(default=None, description="Business domain (post-MVP)")
    certification_status: str | None = Field(
        default=None,
        description="Certification status: certified, deprecated, or None (post-MVP)"
    )
    tags: list[str] = Field(default_factory=list, description="Applied tags")


class DatasetSearchResult(BaseModel):
    """Response for dataset search queries."""

    query: str = Field(description="Original search query")
    total_matches: int = Field(description="Total number of matching datasets")
    datasets: list[DatasetMetadata] = Field(description="Matching datasets")
    suggestions: list[str] = Field(
        default_factory=list,
        description="Query refinement suggestions if no/few results"
    )


class DatasetExistsResponse(BaseModel):
    """Response for dataset existence checks."""

    query: str = Field(description="Original query")
    exists: bool = Field(description="Whether a matching dataset was found")
    dataset: DatasetMetadata | None = Field(
        default=None,
        description="Dataset metadata if found"
    )
    alternatives: list[DatasetMetadata] = Field(
        default_factory=list,
        description="Alternative matches if exact match not found"
    )
    message: str = Field(description="Human-readable response message")

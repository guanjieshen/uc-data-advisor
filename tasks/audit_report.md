# UC Metadata Audit Report

**Date**: 2026-03-12
**Workspace**: fevm-cjc-aws-workspace.cloud.databricks.com
**Story**: #22 - Audit current Unity Catalog metadata coverage

## Summary

| Object Type | Total | With Descriptions | Coverage |
|-------------|-------|-------------------|----------|
| Catalogs | 3 | 1 | 33.3% |
| Schemas | 10 | 1 | 10.0% |
| Tables | 93 | 36 | 38.7% |
| Columns | 1746 | 149 | 8.5% |

**Overall description coverage: 35.8%**

## Catalogs

| Catalog | Owner | Has Description |
|---------|-------|-----------------|
| cjc_aws_workspace_catalog | aec5be3d-de3c-404c-8e60-feed0f265fd3 | No |
| fevm_shared_catalog | gregory.wood@databricks.com | No |
| samples | System user | Yes |

## Key Findings

### Positive
- **100% ownership coverage** - All tables have owners assigned
- **All tables are MANAGED** - Consistent table type
- **system.information_schema accessible** - Can query metadata via SQL
- **SDK API works** - `WorkspaceClient` successfully queries UC metadata

### Gaps
- **Low description coverage** (35.8% overall)
- **Very low column description coverage** (8.5%)
- **Tags not available** - `system.information_schema.tag_assignments` not found
- **Schema descriptions sparse** (10%)

## Implications for Design (#21)

### Access Pattern
**Recommendation**: Use Databricks SDK for metadata access

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(profile="fevm-cjc")

# List tables
for table in w.tables.list(catalog_name="main", schema_name="default"):
    print(table.name, table.comment)
```

The SDK provides:
- Direct access to catalogs, schemas, tables, columns
- Built-in pagination
- Respects UC permissions
- No need for raw SQL for basic operations

For complex queries (lineage, audit), use `statement_execution` API with system tables.

### Search Strategy
Given low description coverage:
- **Exact name matching** will work well (100% have names)
- **Fuzzy name matching** - viable using table/column names
- **Description-based search** - limited effectiveness (only 35.8% have descriptions)
- **Owner-based search** - viable (100% coverage)

**Recommendation**: Prioritize name and owner-based search for MVP. Consider #20 (AI-generated metadata) to improve description coverage for post-MVP.

### Response Model
Based on reliable fields:
- `name` - Always available
- `full_name` - Always available (catalog.schema.table)
- `owner` - Always available (100%)
- `table_type` - Always available (MANAGED)
- `comment` - Available ~39% of tables
- `columns` - Always available, but comments only 8.5%
- `created_at`, `updated_at` - Available via system tables

### Permission Model
- SDK respects UC ACLs automatically
- User identity determines visible objects
- No additional filtering needed

## Recommendations

### For Story #21 (Design)
1. Use SDK API as primary metadata access method
2. Implement name-based exact and fuzzy matching
3. Include owner in search and response
4. Mark description fields as optional in response model
5. Skip tags for MVP (not available in this workspace)

### For Future Stories
- **#20 (AI-generated metadata)**: High priority given low coverage
- **#5 (Fuzzy search)**: Focus on table/column names, not descriptions
- **#19 (Domains/certification)**: Blocked until tags available

## Next Steps

1. [ ] Finalize design document (#21) based on these findings
2. [ ] Implement metadata access using SDK
3. [ ] Define response model with nullable description fields
4. [ ] Create search implementation prioritizing name matching

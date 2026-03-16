# UC Data Advisor - Implementation Plan

**Last Updated**: 2026-03-12

## Overview

Build a conversational agent for Unity Catalog dataset discovery. The agent answers natural language questions about available datasets, their metadata, ownership, and location.

## Project Structure

```
uc-data-advisor/
├── databricks.yml              # DAB config (dev/prod targets)
├── resources/
│   ├── jobs.yml               # Job definitions
│   └── schemas.yml            # UC schema definitions
├── src/
│   └── agent/
│       ├── __init__.py
│       ├── metadata_access.py # UC metadata client (#3)
│       ├── response_model.py  # Response contracts (#4)
│       └── tools.py           # Agent tools (#6, #7)
├── tests/
│   └── test_response_model.py # Unit tests (4 passing)
├── .github/workflows/
│   ├── ci.yml                 # Test + validate on PR
│   └── deploy.yml             # Deploy to Databricks
├── .envrc                     # direnv + uv config
├── pyproject.toml             # Python dependencies
└── tasks/
    ├── plan.md                # This file
    ├── todo.md                # Task tracking
    └── lessons.md             # Learnings
```

## Environment

- **Workspace**: https://fevm-cjc-aws-workspace.cloud.databricks.com
- **Profile**: fevm-cjc (authenticated)
- **Warehouse ID**: 751fe324525584e5
- **Python**: 3.12 via uv
- **DAB**: Validated, ready to deploy

## Current Phase: Foundation

Development environment is set up. Next steps:
1. **#22 - Metadata Audit**: Understand what metadata exists in the workspace
2. **#21 - Design**: Finalize architecture and contracts based on audit findings

## Execution Roadmap

### Phase 1: Foundation (Current)

| Story | Title | Status |
|-------|-------|--------|
| #22 | Audit UC metadata coverage | Ready to Start |
| #21 | Design metadata access layer | Waiting on #22 |

**Key Decisions Needed**:
- [ ] Access pattern: SDK API vs SQL system tables vs hybrid
- [ ] Which metadata fields are reliably populated
- [ ] Permission enforcement model
- [ ] Performance targets

### Phase 2: Core Data Layer

| Story | Title | Status |
|-------|-------|--------|
| #3 | Secure access to UC metadata | Scaffolded |
| #4 | Define metadata response model | Scaffolded |

Initial code in `src/agent/` - refine after design approval.

### Phase 3: Agent Experience

| Story | Title | Status |
|-------|-------|--------|
| #6 | Dataset existence question flow | Scaffolded |
| #7 | Return dataset details | Scaffolded |
| #5 | Keyword/fuzzy search | Not Started |

### Phase 4: Teams Integration

| Story | Title | Status |
|-------|-------|--------|
| #10 | Expose agent in Teams | Not Started |
| #11 | Map Teams identity | Not Started |

### Phase 5: Governance & Validation

| Story | Title | Status |
|-------|-------|--------|
| #14 | Enforce metadata-only scope | Not Started |
| #17 | Define pilot success criteria | Ready to Start |
| #18 | Execute UAT | Blocked |

## Architecture

### Metadata Access (Current Approach)

Using Databricks SDK (`databricks-sdk`) for UC metadata:

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(profile="fevm-cjc")

# List catalogs
for catalog in w.catalogs.list():
    print(catalog.name)

# Get table details
table = w.tables.get(full_name="catalog.schema.table")
```

### Response Model (Pydantic)

```python
class DatasetMetadata(BaseModel):
    name: str
    full_name: str  # catalog.schema.table
    catalog_name: str
    schema_name: str
    owner: str | None
    comment: str | None
    columns: list[ColumnMetadata]
```

### Agent Framework Options

- **Databricks Agent Bricks** (recommended) - KA + Genie + MAS
- Custom implementation with SDK

## Open Questions

1. ~~Environment Access~~: Resolved - using fevm-cjc workspace
2. **Teams App Registration**: What's the process for registering an MS Teams bot?
3. **Identity Mapping**: How will Teams AAD identity map to Databricks identity?
4. **Scope Boundaries**: What happens when user asks out-of-scope questions?

## Risk Register

| Risk | Impact | Mitigation |
|------|--------|------------|
| Sparse metadata | Agent can't answer questions | #20 - AI-generated metadata |
| Teams identity mapping complex | Delays integration | Start #11 research early |
| Performance issues | Poor UX | Define targets in #21 |

## Next Actions

1. [x] ~~Set up development environment~~
2. [ ] Run #22 - Metadata audit on fevm-cjc workspace
3. [ ] Complete #21 - Design based on audit findings
4. [ ] Add GitHub secrets for CI/CD
5. [ ] Research Teams bot registration process

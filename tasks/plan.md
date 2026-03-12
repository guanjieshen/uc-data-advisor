# UC Data Advisor - Implementation Plan

**Last Updated**: 2026-03-12

## Overview

Build a conversational agent for Unity Catalog dataset discovery. The agent answers natural language questions about available datasets, their metadata, ownership, and location.

## Current Phase: Pre-Implementation

Before any code is written, two foundational stories must be completed:
1. **#22 - Metadata Audit**: Understand what metadata exists
2. **#21 - Design**: Define architecture and contracts

## Execution Roadmap

### Phase 1: Foundation (Current)

**Goal**: Establish data layer design and understand metadata landscape

| Story | Title | Dependencies | Status |
|-------|-------|--------------|--------|
| #22 | Audit UC metadata coverage | Access to UC environment | Not Started |
| #21 | Design metadata access layer | Informed by #22 | Not Started |

**Key Decisions Needed**:
- [ ] Access pattern: System tables vs REST API vs SQL vs hybrid
- [ ] Which metadata fields are reliably populated
- [ ] Permission enforcement model
- [ ] Performance targets

### Phase 2: Core Data Layer

**Goal**: Implement secure metadata access and response model

| Story | Title | Dependencies | Status |
|-------|-------|--------------|--------|
| #3 | Secure access to UC metadata | Blocked by #21 | Not Started |
| #4 | Define metadata response model | Blocked by #21 | Not Started |

**Can run in parallel** after design is approved.

### Phase 3: Agent Experience

**Goal**: Build core conversational flows

| Story | Title | Dependencies | Status |
|-------|-------|--------------|--------|
| #6 | Dataset existence question flow | #3, #4 | Not Started |
| #7 | Return dataset details | #3, #4 | Not Started |

### Phase 4: Teams Integration

**Goal**: Expose agent through MS Teams

| Story | Title | Dependencies | Status |
|-------|-------|--------------|--------|
| #10 | Expose agent in Teams | #6, #7 | Not Started |
| #11 | Map Teams identity | #10 | Not Started |

### Phase 5: Governance & Validation

**Goal**: Security guardrails and pilot readiness

| Story | Title | Dependencies | Status |
|-------|-------|--------------|--------|
| #14 | Enforce metadata-only scope | #6, #7 | Not Started |
| #17 | Define pilot success criteria | None | Not Started |
| #18 | Execute UAT | All above | Not Started |

## Open Questions

1. **Environment Access**: Do we have access to representative UC environment for #22 audit?
2. **Teams App Registration**: What's the process for registering an MS Teams bot?
3. **Identity Mapping**: How will Teams AAD identity map to Databricks identity?
4. **Scope Boundaries**: What happens when user asks out-of-scope questions?

## Architecture Considerations

### Metadata Access Options

| Option | Pros | Cons |
|--------|------|------|
| System Tables | SQL-native, performant | May lack some fields |
| REST API | Complete metadata | More complex auth |
| Hybrid | Best of both | Implementation complexity |

### Agent Framework Options

- Databricks Agent Framework (recommended for UC integration)
- LangChain/LangGraph
- Custom implementation

## Risk Register

| Risk | Impact | Mitigation |
|------|--------|------------|
| Sparse metadata | Agent can't answer questions | #20 - AI-generated metadata |
| Teams identity mapping complex | Delays integration | Start #11 research early |
| Performance issues | Poor UX | Define targets in #21 |

## Next Actions

1. [ ] Confirm access to UC environment for audit
2. [ ] Start #22 - Metadata audit
3. [ ] Research Teams bot registration process
4. [ ] Set up development environment

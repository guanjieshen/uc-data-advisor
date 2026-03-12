# UC Data Advisor - Task Tracking

**Last Updated**: 2026-03-12

## Current Sprint: Project Setup & Phase 1 Foundation

### In Progress
- [ ] **#22**: Audit current Unity Catalog metadata coverage
  - Requires: Access to fevm-cjc workspace
  - Output: Metadata coverage report
  - Informs: Design decisions in #21

### Ready to Start
- [ ] **#21**: Design metadata access layer and agent tool contracts
  - Informed by: #22 (audit)
  - Gates: All implementation stories (#3, #4, #5)

- [ ] **#17**: Define pilot success criteria
  - No blockers - can start anytime

### Blocked (waiting on design)
- [ ] **#3**: Set up secure access to Unity Catalog metadata
  - Blocked by: #21 (design)
  - Initial scaffolding created in `src/agent/metadata_access.py`

- [ ] **#4**: Define metadata model for dataset discovery responses
  - Blocked by: #21 (design)
  - Initial scaffolding created in `src/agent/response_model.py`

- [ ] **#6**: Build dataset existence question flow
  - Blocked by: #3, #4
  - Initial scaffolding created in `src/agent/tools.py`

- [ ] **#7**: Return dataset details with existence response
  - Blocked by: #3, #4
  - Initial scaffolding created in `src/agent/tools.py`

- [ ] **#10**: Expose agent through Microsoft Teams
  - Blocked by: #6, #7

- [ ] **#11**: Map Teams identity to authorized data access
  - Blocked by: #10

- [ ] **#14**: Enforce metadata-only response scope
  - Blocked by: #6, #7

- [ ] **#18**: Execute UAT for Teams and custom UI
  - Blocked by: All above

---

## Completed

### Project Setup (2026-03-12)
- [x] Initialize repository
- [x] Create cjc-dev branch
- [x] Add basic README
- [x] Set up GitHub project access
- [x] Create CLAUDE.md with project instructions
- [x] Create task tracking structure (todo.md, plan.md, lessons.md)
- [x] Set up direnv + uv environment
- [x] Verify Databricks authentication (fevm-cjc profile)
- [x] Create DAB project structure
  - databricks.yml with dev/prod targets
  - resources/jobs.yml, resources/schemas.yml
  - src/agent/ package with initial modules
- [x] Set up GitHub Actions (ci.yml, deploy.yml)
- [x] Add initial test suite (4 tests passing)

---

## Issues & Blockers

| Issue | Description | Owner | Status |
|-------|-------------|-------|--------|
| GitHub Secrets | Need to add DATABRICKS_HOST and DATABRICKS_TOKEN to repo | TBD | Open |

---

## Environment Info

- **Workspace**: https://fevm-cjc-aws-workspace.cloud.databricks.com
- **Profile**: fevm-cjc
- **Warehouse ID**: 751fe324525584e5

---

## Notes

- Recommended execution: #22 → #21 → #3 + #4 (parallel) → #6 + #7 → #10 + #11 → #14 → #17 → #18
- #17 (pilot criteria) has no blockers and can be worked in parallel
- Design story #21 gates all implementation - prioritize completing it
- Initial code scaffolding created for #3, #4, #6, #7 to accelerate implementation once design is approved

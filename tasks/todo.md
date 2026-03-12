# UC Data Advisor - Task Tracking

**Last Updated**: 2026-03-12

## Current Sprint: Project Setup & Phase 1 Foundation

### In Progress
- [ ] Set up project structure and tracking files
- [ ] Review GitHub project and identify blockers

### Ready to Start
- [ ] **#22**: Audit current Unity Catalog metadata coverage
  - Requires: Access to representative UC environment
  - Output: Metadata coverage report
  - Informs: Design decisions in #21

### Blocked
- [ ] **#21**: Design metadata access layer and agent tool contracts
  - Blocked by: #22 (audit informs design)
  - Gates: All implementation stories (#3, #4, #5)

- [ ] **#3**: Set up secure access to Unity Catalog metadata
  - Blocked by: #21 (design)

- [ ] **#4**: Define metadata model for dataset discovery responses
  - Blocked by: #21 (design)

- [ ] **#6**: Build dataset existence question flow
  - Blocked by: #3, #4

- [ ] **#7**: Return dataset details with existence response
  - Blocked by: #3, #4

- [ ] **#10**: Expose agent through Microsoft Teams
  - Blocked by: #6, #7

- [ ] **#11**: Map Teams identity to authorized data access
  - Blocked by: #10

- [ ] **#14**: Enforce metadata-only response scope
  - Blocked by: #6, #7

- [ ] **#17**: Define pilot success criteria
  - No blockers - can start anytime

- [ ] **#18**: Execute UAT for Teams and custom UI
  - Blocked by: All above

---

## Completed

- [x] Initialize repository
- [x] Create cjc-dev branch
- [x] Add basic README
- [x] Set up GitHub project access
- [x] Create CLAUDE.md with project instructions
- [x] Create task tracking structure (todo.md, plan.md, lessons.md)

---

## Issues & Blockers

| Issue | Description | Owner | Status |
|-------|-------------|-------|--------|
| UC Environment Access | Need access to representative UC environment for #22 | TBD | Open |
| Teams Bot Registration | Need to understand registration process | TBD | Open |

---

## Notes

- Recommended execution: #22 → #21 → #3 + #4 (parallel) → #6 + #7 → #10 + #11 → #14 → #17 → #18
- #17 (pilot criteria) has no blockers and can be worked in parallel
- Design story #21 gates all implementation - prioritize completing it

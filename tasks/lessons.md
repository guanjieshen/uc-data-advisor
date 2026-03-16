# UC Data Advisor - Lessons Learned

**Purpose**: Capture patterns, mistakes, and rules to improve future work.

---

## Session: 2026-03-12

### Environment Setup

**Lesson**: Always use `jj` for version control, not `git`
- This project uses Jujutsu (jj) exclusively
- Commands differ: `jj describe` not `git commit`, `jj bookmark` not `git branch`

**Lesson**: Always use fish shell syntax
- No bash-isms: `for..do..done` → `for..end`
- No `export VAR=value` → `set -x VAR value`
- No `$()` in assignments → `set VAR (command)`

**Lesson**: Never use homebrew/brew on this machine
- Under any circumstances

### Version Control Discipline

**Lesson**: Commit and push early and often
- Don't accumulate changes - push frequently
- Verify with `jj bookmark list` that local and origin are in sync
- No `@origin (ahead/behind)` means synced

### GitHub Integration

**Lesson**: gh CLI requires scope refresh for project access
- `read:project` scope needed for `gh project` commands
- Use `gh auth refresh -h github.com -s read:project,project`

**Lesson**: workflow scope needed to push GitHub Actions files
- Error: "refusing to allow an OAuth App to create or update workflow"
- Use `gh auth refresh -h github.com -s workflow`

**Lesson**: gh auth and git credentials are separate
- After `gh auth login`, run `gh auth setup-git` to configure git credential helper

### Project Setup

**Lesson**: Check for existing Databricks profiles before creating new ones
- `cat ~/.databrickscfg` to see existing profiles
- Found `fevm-cjc` profile already configured for target workspace

**Lesson**: uv creates .venv in project dir by default
- Even with UV_PROJECT_ENVIRONMENT set in .envrc
- direnv must be sourced in shell before uv commands for env var to take effect
- Either approach works (.venv local or ~/.virtualenvs/project)

**Lesson**: DAB validation catches issues early
- Run `databricks bundle validate` before committing
- Use `-p profile` to specify Databricks profile

**Lesson**: Warehouse ID is useful to capture
- Found via `databricks warehouses list -p fevm-cjc --output json`
- Current warehouse: 751fe324525584e5 (Serverless Starter Warehouse)

**Lesson**: Clusters need `data_security_mode: "SINGLE_USER"` for Unity Catalog
- Without it, cluster uses spark_catalog (Hive metastore)
- Error: "spark_catalog requires a single-part namespace"
- Fix: Add `data_security_mode: "SINGLE_USER"` to cluster config

**Lesson**: Use correct catalog name for workspace
- `main` catalog doesn't exist in all workspaces
- Check available catalogs: `databricks catalogs list -p profile`
- This workspace uses `cjc_aws_workspace_catalog`

---

## Patterns to Follow

1. **Plan before implementing** - Write to plan.md before coding
2. **Track in todo.md** - Keep task status current
3. **Update lessons.md** - After any correction or discovery
4. **Verify before done** - Prove it works before marking complete

---

## Anti-Patterns to Avoid

1. Starting implementation before design is approved (#21 gates all)
2. Assuming metadata exists without auditing (#22 first)
3. Using git commands instead of jj
4. Using bash syntax instead of fish

---

## Rules

- [ ] Check plan.md at session start
- [ ] Review lessons.md for relevant project patterns
- [ ] Update todo.md after each task state change
- [ ] Commit with `jj describe -m "message"` then `jj git push`

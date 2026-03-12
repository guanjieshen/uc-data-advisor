## Shell Environment

**Fish Shell Only**: This machine runs fish shell. Always use fish shell syntax.

- No `for x in ...; do ... done` - use `for x in ...; ...; end`
- No `$()` for command substitution in assignment - use `set VAR (command)`
- No `export VAR=value` - use `set -x VAR value`
- No `&&` chaining in some contexts - use `; and` or separate commands

**No Homebrew**: NEVER use `brew` or `homebrew` commands on this machine under any circumstances.

## Version Control

**Jujutsu Only**: Use `jj` for all version control operations, not `git` commands.

- `jj status` instead of `git status`
- `jj describe -m "message"` instead of `git commit`
- `jj bookmark create <name>` instead of `git branch`
- `jj git push --bookmark <name>` to push
- `jj log` instead of `git log`
- `jj new` to create a new change
- `jj squash` to combine changes

## Project Tracking

**GitHub Project**: https://github.com/users/guanjieshen/projects/5/views/1

Use `gh` CLI to manage the UC Advisor Agent project:
- `gh project item-list 5 --owner guanjieshen` - list all items
- `gh issue view <number> -R guanjieshen/uc-data-advisor` - view issue details
- `gh issue edit <number> -R guanjieshen/uc-data-advisor` - update issues

### MVP Stories (P0) - Priority Order

| Issue | Title | Epic | Status |
|-------|-------|------|--------|
| #22 | Audit current Unity Catalog metadata coverage | 1 | Backlog |
| #21 | Design metadata access layer and agent tool contracts | 1 | Backlog |
| #3 | Set up secure access to Unity Catalog metadata | 1 | Backlog |
| #4 | Define metadata model for dataset discovery responses | 1 | Backlog |
| #6 | Build dataset existence question flow | 2 | Backlog |
| #7 | Return dataset details with existence response | 2 | Backlog |
| #10 | Expose agent through Microsoft Teams | 3 | Backlog |
| #11 | Map Teams identity to authorized data access | 3 | Backlog |
| #14 | Enforce metadata-only response scope | 5 | Backlog |
| #17 | Define pilot success criteria | 6 | Backlog |
| #18 | Execute UAT for Teams and custom UI | 6 | Backlog |

**Recommended execution order**: #22 (audit) → #21 (design) → #3 + #4 (parallel) → #6 + #7 → #10 + #11 → #14 → #17 → #18

---

## Workflow Orchestration

### 1. Plan Mode Default
- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan immediately - don't keep pushing
- Use plan mode for execution steps and building out
- Write detailed specs up front to reduce ambiguity

### 2. Subagent Strategy
- Use subagents liberally to keep main context window clean
- Offload research, exploration, and parallel analysis to subagents
- For complex problems, throw more compute at it via subagents
- One task per subagent for focused execution

### 3. Self-Improvement Loop
- After ANY correction from the user: update `tasks/lessons.md` with the pattern
- Write rules for yourself that prevent the same mistake
- Ruthlessly iterate on these lessons until mistake rate drops
- Review lessons at session start for relevant project

### 4. Verification Before Done
- Never mark a task complete without proving it works
- Diff behavior between main and your changes when relevant
- Ask yourself: "Would a staff engineer approve this?"
- Run tests, check logs, demonstrate correctness

### 5. Demand Elegance (Balanced)
- For non-trivial changes: pause and ask "is there a more elegant way?"
- If a fix feels hacky: "Knowing everything I know now, implement the elegant solution"
- Skip this for simple, obvious fixes - don't over-engineer
- Challenge your own work before presenting it

### 6. Autonomous Bug Fixing
- When given a bug report: just fix it. Don't ask for hand-holding
- Point at logs, errors, failing tests - then resolve them
- Zero context switching required from the user
- Go fix failing CI tests without being told how

## Task Management

1. **Plan First**: Write plan to `tasks/todo.md` with checkable items
2. **Verify Plan**: Check in before starting implementation
3. **Track Progress**: Mark items complete as you go
4. **Explain Changes**: High-level summary at each step
5. **Document Results**: Add review section to `tasks/todo.md`
6. **Capture Lessons**: Update `tasks/lessons.md` after corrections

## Core Principles

- **Simplicity First**: Make every change as simple as possible. Impact minimal code.
- **No Laziness**: Find root causes. No temporary fixes. Senior developer standards.
- **Minimal Impact**: Changes should only touch what's necessary. Avoid introducing bugs.

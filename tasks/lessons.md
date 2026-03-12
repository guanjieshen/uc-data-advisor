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

**Lesson**: gh auth and git credentials are separate
- After `gh auth login`, run `gh auth setup-git` to configure git credential helper

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

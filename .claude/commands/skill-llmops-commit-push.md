Commit all changes and push to the remote branch. Runs pre-commit hooks and unit tests before committing.

## Steps

### 1. Check the current branch

```bash
git branch --show-current
```

If the current branch is `main` or `master`, **stop immediately**:
> "Refusing to commit directly to `main`. Please switch to a feature branch first."

### 2. Check for pending changes

```bash
git status
```

If there are no staged or unstaged changes and no untracked files, stop:
> "Nothing to commit — working tree is clean."

### 3. Stage all changes and run pre-commit hooks

```bash
git add -A
```

Then run pre-commit hooks:
```bash
uv run pre-commit run --all-files
```

If hooks modify files (e.g. ruff auto-fixes, end-of-file-fixer), re-stage and re-run until clean:
```bash
git add -A
uv run pre-commit run --all-files
```

If hooks fail with errors that cannot be auto-fixed, fix them manually, re-stage, and re-run. Do not proceed until all hooks pass.

### 4. Run unit tests and check coverage

```bash
uv run --extra ci pytest
```

This command runs tests **and** enforces the coverage threshold (configured via `--cov-fail-under` in `pyproject.toml`). A non-zero exit code means either tests failed **or** coverage is below the threshold.

If the run fails for any reason — test failures, coverage below threshold, or import errors — **stop immediately** and report the full failure output. Do not proceed until all tests pass and coverage is above the threshold.

To identify which lines are uncovered and causing a coverage failure, run:
```bash
uv run --extra ci coverage report --include="*/artlake/*" -m
```

Common fix for Spark/Databricks entry-point functions that cannot be unit-tested: add `# pragma: no cover` to the function definition line.

### 5. Craft the commit message

Inspect what changed:
```bash
git diff --cached
git log --oneline -5
```

Write a commit message following Conventional Commits:

```
<type>(<scope>): <short imperative summary> (50 chars max)

<body — what changed and why. Wrap at 72 chars.
  Use bullet points for multiple logical changes.>

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
```

**Types:** `feat`, `fix`, `docs`, `chore`, `refactor`, `style`, `test`

### 6. Commit

```bash
git commit -m "$(cat <<'EOF'
<message>
EOF
)"
```

If the commit is rejected by a pre-commit hook, fix the issue, re-stage, and create a **new** commit (never amend).

### 7. Push

```bash
git push -u origin HEAD
```

Report the remote URL printed by git.

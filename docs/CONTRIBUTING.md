# Contributing

## Prerequisites

- Python 3.12 (matches Databricks Serverless Environment 4)
- [uv](https://docs.astral.sh/uv/getting-started/installation/) for dependency management
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) for bundle deployments

## Local Setup

```bash
# Clone the repo
git clone <repo-url>
cd llmops-databricks-course-victor-kuznetsov

# Install all dependencies (including dev tools)
uv sync --extra dev

# Install pre-commit hooks
uv run pre-commit install
```

## Running Code Quality Checks

```bash
# Run all pre-commit hooks (ruff lint, ruff format, mypy, etc.)
uv run pre-commit run --all-files

# Run individually
uv run ruff check .
uv run ruff format --check .
uv run mypy src/
```

## Running Tests

```bash
# Run unit tests
uv run pytest

# Run with coverage report
uv run pytest --cov=llmops_databricks_course_victor_kuznetsov --cov-report=term-missing

# Run integration tests (requires Databricks workspace)
uv run pytest -m integration
```

## Opening a PR

1. Create a branch from `main` following the [naming conventions](CONVENTIONS.md#branch-naming).
2. Make your changes and ensure all checks pass locally.
3. Push and open a PR against `main`.
4. CI runs lint + unit tests automatically.

## Dependency Management

- **Regular dependencies**: pin to exact version (`"pydantic==2.11.7"`).
- **Dev/CI dependencies**: use range (`"pytest>=8.3.4,<9"`).
- Use `/fix-deps` to look up latest PyPI versions.
- After changes: `uv sync --extra dev` to validate.

See [CONVENTIONS.md](CONVENTIONS.md) for naming rules and [CLAUDE.md](../CLAUDE.md) for AI-assisted development guidelines.

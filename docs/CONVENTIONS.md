# Naming Conventions

## Files and Folders

| Item | Convention | Example |
|---|---|---|
| Python modules | `snake_case` | `data_loader.py` |
| Python packages (directories) | `snake_case` | `event_ingestion/` |
| Databricks notebooks | `snake_case` | `notebooks/hello_world.py` |
| Resource/config files (YAML, JSON) | `snake_case` | `hello_world_job.yml` |
| Documentation files | `UPPER_CASE.md` | `docs/CONTRIBUTING.md`, `docs/artlake/DECISIONS.md` |
| Directories | `snake_case` for code, `kebab-case` for docs subfolders | `src/`, `docs/artlake/` |

## Python

| Item | Convention | Example |
|---|---|---|
| Classes | `PascalCase` | `EventScraper` |
| Functions / methods | `snake_case` | `fetch_events()` |
| Variables | `snake_case` | `event_count` |
| Constants | `UPPER_SNAKE_CASE` | `MAX_RETRIES` |
| Private members | `_leading_underscore` | `_parse_html()` |
| Type aliases | `PascalCase` | `EventRecord` |
| Docstrings | Google style | |
| Type annotations | Required on all public functions and classes | |

## Databricks Notebooks

- First line: `# Databricks notebook source`
- Cell separator: `# COMMAND ----------`
- File names: `snake_case.py` in `notebooks/`
- No `#!/usr/bin/env python` shebangs

## Entry Points (`.whl` / `python_wheel_task`)

| Item | Convention | Example |
|---|---|---|
| Entry point name | `artlake-<verb>-<noun>` (kebab-case) | `artlake-search`, `artlake-clean-events` |
| `pyproject.toml` declaration | `[project.scripts]` section | `artlake-search = "llmops_...search.web:main"` |
| Module function | `main()` in the domain module | `search/web.py` has `def main(): ...` |
| Naming principle | Named for what the code **does**, not medallion layers | `clean/events.py` not `silver.py` |

## Delta Lake / Unity Catalog

| Item | Convention | Example |
|---|---|---|
| Catalog | `snake_case` | `artlake` |
| Schema | `snake_case` (by purpose) | `staging`, `bronze`, `gold` |
| Table | `snake_case` | `raw_events`, `seen_urls`, `events` |
| Column | `snake_case` | `event_title`, `created_at` |
| Full path | `catalog.schema.table` | `artlake.gold.events` |

### Schema purposes

| Schema | Purpose |
|---|---|
| `staging` | Inter-task data (search results, scraped pages, seen URLs) — transient between pipeline runs |
| `bronze` | Structured clean events (first persisted layer) |
| `gold` | Categorised events, embeddings — ready for BI/serving |

## Git

### Branch Naming

Pattern: `<type>/<issue-number>-<kebab-case-title>`

| Type | When |
|---|---|
| `feature` | New functionality |
| `bug` | Bug fix, defect |

Examples:
- `feature/5-add-delta-ingestion-pipeline`
- `bug/12-fix-geo-filter-radius`

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<optional scope>): <description>

[optional body]

[optional footer]
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`, `ci`, `build`.

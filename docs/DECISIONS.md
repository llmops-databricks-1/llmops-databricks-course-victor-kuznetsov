# Platform-Wide Technology Decisions

> Platform-level choices that apply across all projects in this repository.
> For project-specific decisions, see `docs/<project>/DECISIONS.md`.

---

## Python & Dependency Management

| Choice | Detail |
|---|---|
| Python version | 3.12 (matches Databricks Serverless Environment 4) |
| Dependency manager | `uv` + `pyproject.toml` |
| Dependency pinning | Exact for runtime (`==`), range for dev/CI (`>=X,<NEXT_MAJOR`) |
| Package layout | `src/` layout with `setuptools` build backend |

---

## Code Quality

| Tool | Purpose |
|---|---|
| `ruff` | Linter (replaces flake8 + isort + pyupgrade) |
| `ruff format` | Formatter |
| `mypy` | Static type checking (strict on new modules) |
| `pre-commit` | Git hook runner — enforces ruff, ruff format, mypy |
| Docstrings | Google style |
| Logging | `loguru` (no `print()` in library code) |

---

## Testing

| Choice | Detail |
|---|---|
| Framework | `pytest` |
| Coverage | `pytest-cov`, minimum 80% on `src/` |
| Unit tests | Pure Python, no Databricks dependency, run locally |
| Integration tests | `@pytest.mark.integration`, require live workspace |

---

## Databricks-Native Tools

| Tool | Purpose |
|---|---|
| Databricks Workflows | Orchestration (scheduled + triggered) |
| Delta Lake | Storage (Bronze / Silver / Gold medallion) |
| Unity Catalog | Governance, lineage, access control |
| Vector Search | Semantic retrieval over embeddings |
| Foundation Model API | Hosted LLM and embedding inference |
| Agent Framework | RAG agents (MLflow + LangGraph) |
| AI/BI Genie | Conversational natural-language interface |
| AI/BI Dashboards | Visual analytics |
| Databricks Secrets | Secret management (`dbutils.secrets`) |

---

## Data Collection & Processing

| Tool | Purpose |
|---|---|
| `duckduckgo-search` | Web search (free, no API key) |
| `playwright` | JS-rendered page scraping |
| `beautifulsoup4` | Static HTML scraping |
| `geopy` + Nominatim | Geocoding (free, no API key) |
| `lingua-language-detector` | Language detection (EN/NL/DE/FR) |
| `pydantic` | Data validation and serialization |

---

## Versioning & CI

| Choice | Detail |
|---|---|
| Version source | `version.txt` (single source of truth) |
| CI | GitHub Actions — lint + unit tests on every PR |
| Deployment | Databricks Asset Bundles (`databricks.yml`) |

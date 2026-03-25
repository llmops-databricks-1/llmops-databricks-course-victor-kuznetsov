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

## Execution Model & Deployment

| Choice | Detail |
|---|---|
| Execution model | `.whl` entry points via `python_wheel_task` — no notebooks for data processing |
| Orchestration | Databricks Workflows is the **sole orchestrator** — no Python-level pipeline orchestration |
| Deployment | Databricks Asset Bundles (DAB-native, no custom job parameters) |
| Entry points | Each domain module has `main()` → declared in `[project.scripts]` → one Databricks Workflow task |

---

## Databricks-Native Tools

| Tool | Purpose |
|---|---|
| Databricks Workflows | Orchestration (sole orchestrator, scheduled + triggered) |
| Delta Lake | Storage (staging / bronze / gold tables) |
| Unity Catalog | Governance, lineage, access control |
| Unity Catalog Volumes | Unstructured artifact storage (PDFs, images) |
| Vector Search | Semantic retrieval over embeddings (Delta Sync mode) |
| Foundation Model API | Hosted LLM, embedding, and vision model inference (via OpenAI SDK) |
| Agent Framework | RAG agents (MLflow + LangGraph) |
| AI/BI Genie | Conversational natural-language interface |
| AI/BI Dashboards | Visual analytics (including interactive radius filtering) |
| Databricks Secrets | Secret management (`dbutils.secrets`) |

---

## Data Collection & Processing

| Tool | Purpose |
|---|---|
| `duckduckgo-search` | Web search (free, no API key; SerpAPI upgrade path) |
| `requests` + `beautifulsoup4` | Web page scraping |
| `geopy` + Nominatim | Geocoding + country identification (free, no API key) |
| `pydantic` | Data validation and serialization (v2, strict) |
| `ai_parse_document` | PDF text extraction + image OCR (Databricks-native SQL function) |
| Foundation Model API | Content translation, LLM categorisation, artifact summaries |

---

## Configuration Files

| Choice | Detail |
|---|---|
| Format | YAML for all configs, mappings, and admin-editable system data |
| JSON | Reserved for machine-to-machine data only (API payloads, serialized outputs) |

---

## Versioning & CI

| Choice | Detail |
|---|---|
| Version source | `version.txt` (single source of truth) |
| CI | GitHub Actions — lint + unit tests on every PR |
| Deployment | Databricks Asset Bundles (`databricks.yml`) |

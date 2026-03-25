# ArtLake — Technology Decisions

> This document records key technology choices, their rationale, and trade-offs.
> Guiding principles: **Databricks-native where possible**, **free/open-source preferred**, paid tools only when quality or maintenance effort clearly justifies the cost.

---

## ADR-001 · Search & Web Scraping

**Decision**: Use `duckduckgo-search` (Python library) as the primary search engine wrapper, with `requests` + `beautifulsoup4` for page scraping.

**Rationale**:
- `duckduckgo-search` is free, requires no API key, and returns results in all four target languages (EN/NL/DE/FR). Sufficient for training-course data volumes.
- `beautifulsoup4` + `requests` handles static HTML pages and is straightforward to deploy on Databricks clusters.
- All packages are open source and run inside `.whl` entry points on Databricks.

**Upgrade path**: Switch to **SerpAPI** (~$50/month, 5000 queries) if DuckDuckGo result quality, rate limits, or JS-rendered page content prove insufficient.

**Rejected**:
- Google Custom Search API — free tier limited to 100 queries/day, too restrictive.
- Direct Google scraping — ToS violation.
- `playwright` (headless Chromium) — deployment complexity on Databricks clusters (requires custom Docker images or init scripts for Chromium binaries). SerpAPI is a cleaner upgrade path for JS-rendered content.

---

## ADR-002 · Social Network Data Collection

**Decision**: Start compliant — access social network content indirectly via search queries targeting public social domains (e.g. `site:facebook.com/events "open call"`) rather than direct platform APIs or direct scraping.

**Rationale**:
- Facebook Events API was deprecated in 2018 for third-party apps.
- Instagram Graph API requires app review and is scoped to own content.
- LinkedIn API is extremely limited and actively restricts scraping.
- Querying public posts via DuckDuckGo avoids ToS issues, API approval overhead, and is simpler to maintain.

**Escalation path**: If compliant search-based collection proves insufficient in coverage or quality, we will run a structured comparison:
1. Compliant (DuckDuckGo `site:` queries) — baseline
2. Official APIs where available (Instagram Graph API with app approval)
3. Headless browser scraping of public pages (ToS grey area — evaluate risk vs. coverage gain)

Proceed to next option only if previous step's output quality is demonstrably worse.

**Limitation**: Misses non-public or members-only events. Acceptable for MVP.

---

## ADR-003 · Geocoding

**Decision**: Use **Nominatim** (OpenStreetMap) via the `geopy` Python library.

**Rationale**:
- Free, no API key required.
- Good coverage of NL/BE/DE/FR postal codes.
- 1 request/second rate limit is acceptable for our batch-scraping volumes (we geocode event locations, not every page request).

**Upgrade path**: **OpenCage Geocoder** (free tier: 2500 requests/day) if Nominatim reliability becomes an issue.

**Rejected**: Google Geocoding API — paid beyond small free tier, overkill for this use case.

---

## ADR-004 · Language Targeting

**Decision**: Handle language at the **search query level**, not as a separate post-filter. Search queries are generated per target language using translated keywords. Results are tagged with the query language. No `lingua-language-detector` pipeline step.

**Rationale**:
- If we search with German keywords, we expect German results — a separate language detection step is redundant.
- Reduces pipeline complexity (one fewer workflow task).
- Multilingual keyword generation (base English → translated per language) gives us control over which languages we target.

**Retained for edge cases**: `lingua-language-detector` remains a dependency for optional quality checks within the scrape step (e.g., validating that a page returned by a German query is actually in German), but is not a standalone pipeline task.

**Rejected approach**: Separate `artlake-detect-language` pipeline step — adds a workflow task with minimal value when search queries already control language.

---

## ADR-005 · Embeddings

**Decision**: Use **Databricks Foundation Model API** (`databricks-gte-large-en`) hosted on the workspace, applied to **pre-translated** (English) content.

**Rationale**:
- Included in Databricks workspace — free, no additional cost.
- Fastest embedding model available on Databricks (recommended by course reference material).
- Databricks-native: integrates directly with Vector Search and Agent Framework.
- 1024 dimensions, 512 max tokens — suitable for event descriptions.
- Content is pre-translated to the configured language (default: English) via ADR-018, so monolingual embeddings work well.

**Alternative**: `databricks-bge-large-en` — higher quality but slower. Switch if GTE retrieval quality proves insufficient.

**Upgrade path**: OpenAI `text-embedding-3-small` if Databricks-native models prove insufficient (cost: ~$0.02/1M tokens).

**Rejected**: Using a multilingual embedding model (e.g. `bge-m3`, `multilingual-e5-large`) — adds complexity. Translating content first and using a strong monolingual model is simpler and produces more consistent embeddings.

---

## ADR-006 · Vector Search

**Decision**: **Databricks Vector Search** (native).

**Rationale**:
- Databricks-native, integrates directly with Delta Lake tables and the Agent Framework.
- No additional infrastructure to manage.
- Supports both direct-access and Delta Sync index modes.

---

## ADR-007 · RAG & Agent Framework

**Decision**: **Databricks Agent Framework** (MLflow + LangGraph) with **Databricks Foundation Model API** (Llama 3 or DBRX) as the reasoning LLM.

**Rationale**:
- Fully Databricks-native: agent traces logged to MLflow, models served via Model Serving endpoints.
- Foundation Model API free tokens included in workspace — no extra cost for training.
- LangGraph provides robust multi-step agent reasoning.

---

## ADR-008 · Orchestration

**Decision**: **Databricks Workflows** (Jobs + Tasks).

**Rationale**:
- Databricks-native, no additional tooling.
- Supports scheduled + triggered execution.
- Integrates with Unity Catalog, secrets, and cluster policies out of the box.

**Rejected**: Apache Airflow — additional infrastructure, not needed given Databricks Workflows covers our use cases.

---

## ADR-009 · Storage

**Decision**: **Delta Lake** (Bronze / Silver / Gold medallion) under **Unity Catalog**, with raw HTML/JSON stored in **Unity Catalog Volumes**.

**Rationale**:
- Databricks-native, ACID guarantees, time-travel for debugging pipelines.
- Unity Catalog provides governance, lineage, and access control in one place.
- Volumes for unstructured raw content (scraped HTML, API responses); Delta tables for structured event data.

---

## ADR-010 · Secret Management

**Decision**: **Databricks Secrets** (`dbutils.secrets`) backed by a Databricks secret scope.

**Rationale**:
- Databricks-native, no additional vault infrastructure needed for training.
- API keys (SerpAPI, Nominatim, etc.) stored as secrets; accessed in notebooks/jobs without hardcoding.

---

## ADR-011 · Presentation

**Decision**: **Databricks AI/BI Genie** (conversational Q&A) + **Databricks AI/BI Dashboards** (visual overview).

**Rationale**:
- Both are Databricks-native and connect directly to Gold Delta tables.
- Genie enables the artist to ask natural-language questions without writing SQL.
- Dashboards provide a persistent, visual view (event map, calendar, category breakdown).

---

## ADR-012 · Execution Model

**Decision**: All pipeline steps are `.whl` entry points executed via Databricks `python_wheel_task`. No notebooks for data processing.

**Rationale**:
- `.whl` packages are testable, versionable, and deployable as a unit.
- `python_wheel_task` is the native Databricks mechanism for running wheel entry points in jobs.
- Each module has its own `main()` function, declared in `pyproject.toml` `[project.scripts]`.
- Notebooks are retained only for one-time setup tasks and interactive experimentation.

**Rejected**: Databricks notebook tasks for pipeline steps — harder to test, version, and maintain as the codebase grows.

---

## ADR-013 · Country Filter over Radius Filter

**Decision**: Ingestion pipeline filters by a configurable **list of target countries**, not by geographical radius. Radius-based distance filtering is deferred to Phase 3 (BI/presentation layer, user-interactive).

**Rationale**:
- Country filtering is cheaper — applied both at search query level (country name in query) and after geocoding.
- Events already collected should not be discarded by the pipeline. Letting the user interactively set a radius in dashboards/Genie is more flexible.
- Avoids wasting compute on re-filtering events that were expensive to scrape.
- Lat/lng coordinates are still stored on every event for future BI use.

---

## ADR-014 · Seen-URL Tracking

**Decision**: Maintain a persistent `artlake.staging.seen_urls` Delta table that stores **all** evaluated URLs — both accepted and filtered out — across pipeline runs.

**Rationale**:
- Prevents re-scraping URLs that were already evaluated and rejected (e.g., wrong country).
- Fingerprint is the **full normalised URL** (not domain), supporting art aggregators where the domain is the same but event paths differ.
- Title is a secondary signal for cross-site near-duplicate detection.
- Status tracking (pending → accepted / filtered_country / duplicate) provides pipeline observability.

---

## ADR-015 · Artifact Handling (PDFs & Images)

**Decision**: Detect, download, and process PDFs and images (posters, open call rules, flyers) found on event pages.

**Rationale**:
- Many art events publish detailed rules, deadlines, and requirements as PDF documents or poster images, not as web page text.
- Missing this content would mean incomplete event information.
- Raw artifacts stored in **Unity Catalog Volumes** (per ADR-009).
- Processing pipeline: **`ai_parse_document`** (Databricks-native SQL function) for both PDF text extraction and image OCR → LLM-generated structured summary (deadline, requirements, location, fees) via Foundation Model API.

**Rejected**: `pdfplumber` / `PyMuPDF` (Python libraries) — `ai_parse_document` is Databricks-native, handles both PDFs and images in a single function call, and avoids managing additional Python dependencies on the cluster.

---

## ADR-016 · Event Categorisation Strategy

**Decision**: Start with **rule-based keyword matching** (MVP), then upgrade to **LLM-based classification** as a drop-in replacement.

**Rationale**:
- Rule-based approach is simpler to test, debug, and understand — good for initial development.
- LLM-based classification provides better accuracy on edge cases and mixed-language content.
- Both implementations share the same input/output contract, making the switch seamless.
- Teaching both approaches aligns with the LLMOps course goal.

---

## ADR-017 · No Python Orchestration

**Decision**: **Databricks Workflows is the sole orchestrator.** No Python-level pipeline orchestration (no `pipelines/` module, no `entrypoints/` wrapper).

**Rationale**:
- Databricks Workflows is purpose-built for task chaining, scheduling, retries, and monitoring.
- Adding Python orchestration on top creates unnecessary sub-orchestration that duplicates Workflow capabilities.
- Each domain module has its own `main()` that does one thing — Workflows chains them.
- "Use the tools for what they are designed to be used for."

**Rejected**: Python pipeline orchestrator module — misuses Databricks Workflows by reducing it to a single-task launcher.

---

## ADR-018 · Content Translation

**Decision**: Translate all scraped content (title, description, location text) to a **configured target language** (default: English) via Databricks Foundation Model API before downstream processing.

**Rationale**:
- Eliminates the need for multilingual embeddings — BGE-large-en works well on English text.
- Simplifies rule-based categorisation — keyword dictionaries needed only in one language instead of four.
- LLM-based categorisation prompts are more reliable in a single language.
- The user reads events in their preferred language anyway — translating early serves both pipeline and UX needs.
- Events already in the target language are skipped (detected via query language tag from ADR-004).

**Rejected**: Multilingual embeddings (e.g. `bge-m3`) without translation — produces inconsistent embedding quality across languages and complicates categorisation.

---

## ADR-019 · Processing Status Tracking

**Decision**: Staging tables use a **`processing_status`** column (`new` → `processing` → `done` | `failed`) for row-level progress tracking. Downstream tasks filter on upstream `processing_status = 'done'`.

**Rationale**:
- Provides run-level isolation without partitioning by `run_id` — simpler schema.
- If a workflow step fails and is retried, only unprocessed (`new`) or failed rows are picked up.
- Failed rows are visible for debugging without blocking the pipeline.
- Pattern is proven in chatbot message processing pipelines.

**Rejected**: `run_id`-based partitioning — requires passing run context through all tasks and complicates cross-run queries (e.g. dedup needs to see all historical URLs, not just current run).

---

## ADR-020 · Configuration & Mapping File Format

**Decision**: Use **YAML** as the standard format for all configuration files, lookup tables, generated translation files, and admin-editable system data.

**Rationale**:
- Human-readable and easy to hand-edit — critical for admin-adjustable files (e.g. translated keyword sets).
- Supports comments, useful for documenting manual overrides or annotations.
- Consistent with existing project conventions (`databricks.yml`, resource definitions).
- JSON reserved for machine-to-machine data only (API payloads, serialized outputs).

**Rejected**: JSON for config files — no comment support, harder to review diffs on nested structures.

---

## ADR-021 · LLM Integration Pattern

**Decision**: All LLM calls use the **OpenAI Python SDK** pointed at **Databricks Foundation Model serving endpoints** (pay-per-token). Default model: `databricks-llama-4-maverick`.

**Rationale**:
- Zero secrets management — authentication via `WorkspaceClient` workspace token.
- Databricks-native: no external network dependency, data stays within the workspace.
- OpenAI SDK is the standard interface — trivial to swap models later (including external models via Databricks gateway).
- Pay-per-token pricing is negligible for batch/one-off tasks (keyword translation, content summarisation).
- Same pattern as the course reference repository (`course-code-hub`).

**Integration pattern**:
```python
from databricks.sdk import WorkspaceClient
from openai import OpenAI

w = WorkspaceClient()
client = OpenAI(
    api_key=w.tokens.create(lifetime_seconds=1200).token_value,
    base_url=f"{w.config.host.rstrip('/')}/serving-endpoints",
)
```

**Upgrade path**: Create an "External Model" serving endpoint to proxy OpenAI GPT-4o or Anthropic Claude if Foundation Model quality proves insufficient for a specific task.

---

## Summary

| Layer | Tool | Cost |
|---|---|---|
| Search | `duckduckgo-search` (SerpAPI upgrade path) | Free |
| Scraping | `requests` + `beautifulsoup4` | Free |
| Geocoding | Nominatim / `geopy` | Free |
| Language targeting | Multilingual keyword generation (search-level) | Free |
| Content translation | Databricks Foundation Model API | Databricks-native |
| Artifact processing | `ai_parse_document` (Databricks-native) | Databricks-native |
| Execution model | `.whl` entry points via `python_wheel_task` | — |
| Orchestration | Databricks Workflows (sole orchestrator) | Databricks-native |
| Deployment | Databricks Asset Bundles (DAB-native) | Databricks-native |
| Raw storage | Unity Catalog Volumes (artifacts) | Databricks-native |
| Structured storage | Delta Lake + Unity Catalog | Databricks-native |
| Secret management | Databricks Secrets | Databricks-native |
| Embeddings | Databricks Foundation Model API (GTE-large-en) | Free (workspace) |
| Vector search | Databricks Vector Search (Delta Sync) | Databricks-native |
| RAG / Agents | Databricks Agent Framework + MLflow | Databricks-native |
| LLM | Databricks Foundation Model API (Llama 3) | Free (workspace) |
| LLM integration | OpenAI SDK → Databricks serving endpoints | Databricks-native |
| Config format | YAML for all configs, mappings, system data | — |
| NL interface | Databricks AI/BI Genie | Databricks-native |
| Dashboards | Databricks AI/BI Dashboards | Databricks-native |

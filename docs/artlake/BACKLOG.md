# ArtLake — Phase 1 & 2 Backlog Plan

## Context

ArtLake has architecture docs, ADRs, repo standards, CI, and tooling in place — but no application code yet. We need to create GitHub issues for the **Ingestion** and **Data Processing** phases. Phase 3 (Serving) deferred.

### Key decisions
- **No notebooks for processing** — all steps are `.whl` entry points via `python_wheel_task`
- **No Python orchestration** — Databricks Workflows is the sole orchestrator. Each module has `main()` → one workflow task.
- **Function-based module naming** — modules named for what the code does.
- **Country filter, not radius** — ingestion filters by configurable country list. Radius filtering deferred to Phase 3 (BI, user-interactive).
- **No separate language detection** — search queries are generated per target language; results tagged with query language. No lingua-based post-filter.
- **Seen-URLs tracking** — all evaluated URLs (accepted + filtered) stored in Delta table. Fingerprint on full normalised URL (supports art aggregators).
- **Artifacts** — detect, download, and process PDFs/images. `ai_parse_document` (Databricks-native SQL function) for PDF text extraction and image OCR. Raw files in UC Volumes.
- **Categorisation** — rule-based MVP first, then LLM upgrade.
- **Social media** — separate story from general search.
- **Content translation** — all scraped content is translated to the configured language (default: English) via Foundation Model API before processing. No need for multilingual embeddings or classification.
- **Processing status tracking** — staging tables use a `processing_status` column for row-level progress tracking (e.g. `new` → `processing` → `done` / `failed`). Downstream tasks filter on status instead of relying on run-level isolation.
- **No Playwright** — `beautifulsoup4` + `requests` for page scraping. SerpAPI (paid) as upgrade path when richer content extraction is needed.
- **Config via DAB** — pipeline configuration via DAB variables or a bundled YAML file deployed with DAB artifacts (decision pending).
- **DAB-native deployment** — no custom parameters; rely on Databricks Asset Bundle context (`${bundle.target}`, etc.). Infrastructure (UC schemas, volumes) managed via DABs.
- **Databricks-native tools** — use Databricks-hosted models for vision/OCR, embeddings, LLM classification. Use `ai_parse_document` for PDF/image extraction.

---

### Package structure

Each module has a `main()` → `[project.scripts]` entry point → `python_wheel_task`.

```
src/artlake/
├── models/                  # Data contracts (Pydantic v2)
│   ├── event.py             #   RawEvent, CleanEvent, GoldEvent, EventArtifact, SeenUrl
│   └── config.py            #   ArtLakeConfig
│
├── search/                  # Finding events online
│   ├── web.py               #   DuckDuckGo general search         → artlake-search
│   ├── social.py            #   Site-scoped social media queries   → artlake-search-social
│   └── keywords.py          #   Multilingual keyword generation (base EN → translated per language)
│
├── scrape/                  # Fetching content from web pages
│   ├── pages.py             #   BS4 + requests, artifact link detection → artlake-scrape-pages
│   └── download.py          #   Download PDFs/images to UC Volumes      → artlake-download-artifacts
│
├── filter/                  # Data quality gates
│   ├── dedup.py             #   URL dedup + seen-URL tracking      → artlake-dedup
│   └── country.py           #   Geocoding + country filter         → artlake-geocode
│
├── clean/                   # Raw → structured field extraction
│   └── events.py            #   Parse dates, normalise fields      → artlake-clean-events
│
├── categorise/              # Event type classification
│   ├── rules.py             #   Keyword-based (multilingual)       → artlake-categorise-rules
│   └── llm.py               #   LLM-based (Foundation Model API)   → artlake-categorise-llm
│
├── translate/               # Content translation
│   └── content.py           #   Translate to configured language (Foundation Model API) → artlake-translate
│
├── process_artifacts/       # Document & image intelligence
│   └── extract.py           #   ai_parse_document (Databricks native) + LLM summary → artlake-process-artifacts
│
├── embed/                   # Semantic search preparation
│   └── generate.py          #   Vector embedding generation (GTE)  → artlake-embed
│
└── storage/                 # Shared persistence utilities (not entry points)
    ├── delta.py             #   Delta table read/write helpers
    └── volumes.py           #   UC Volume read/write helpers
```

### Data flow between tasks (via Delta tables)

```
artlake.staging.search_results   ← search writes here (tagged with query language, processing_status)
artlake.staging.seen_urls        ← dedup reads/writes (persists across runs)
artlake.staging.scraped_pages    ← scrape writes here (processing_status)
artlake.staging.artifacts        ← download/process writes here (processing_status)
artlake.bronze.raw_events        ← clean-events writes here (original language)
artlake.bronze.translated_events ← translate enriches with configured language text
artlake.gold.events              ← categorise writes here
artlake.gold.embeddings          ← embed writes here (from translated content)
```

**`processing_status` values:** `new` → `processing` → `done` | `failed`
Downstream tasks read rows where `processing_status = 'done'` from the upstream table.

### Databricks Workflow structure

**Ingestion workflow:**
```
artlake-search → artlake-search-social → artlake-dedup → artlake-scrape-pages
→ artlake-download-artifacts → artlake-geocode → artlake-clean-events
```

**Processing workflow:**
```
artlake-translate → artlake-process-artifacts → artlake-categorise-rules → artlake-embed
```

---

## Pre-requisite

### Configure databricks.yml — [#6](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/6)

---

## Phase 1: Ingestion (Stories 1.1–1.10)

### 1.1 — Core data models (Pydantic schemas) — [#7](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/7)

**Modules:** `models/event.py`, `models/config.py`

**Models:**
- `RawEvent` — url, title, snippet, source, raw_html, scraped_at, language (from query), artifact_urls
- `CleanEvent` — title, description, date_start, date_end, location_text, lat, lng, country, language, source, url, artifact_paths
- `GoldEvent` — extends CleanEvent with category, artifact_summaries
- `EventArtifact` — url, artifact_type (pdf/image), file_path, extracted_text, llm_summary
- `SeenUrl` — url (full normalised URL as primary key), fingerprint, first_seen_at, status (accepted/filtered_country/duplicate)
- `ArtLakeConfig` — target_countries, languages, target_language (default: "en"), categories, scrape_schedule

**Acceptance criteria:**
- Pydantic v2 with strict validation
- Unit tests (valid + invalid inputs)
- Package-importable

---

### 1.2 — Multilingual keyword generation — [#8](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/8)

**Module:** `search/keywords.py` (library module, not an entry point)

**Behaviour:**
- Define base English keywords per category (e.g. "open call painting", "art market", "art exhibition")
- Translate to target languages using lookup dictionaries or LLM
- Generate search query strings per language × category × country

**Acceptance criteria:**
- Keyword sets for EN, NL, DE, FR
- Unit tests verifying query generation for all combinations
- Extensible: easy to add new categories or languages

---

### 1.3 — DuckDuckGo search (general queries) — [#9](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/9)

**Module:** `search/web.py` → `artlake-search`

**Behaviour:**
- Use keywords from 1.2 to generate queries per language × category × target country
- Execute via `duckduckgo-search`
- Tag each result with query language
- Write to `artlake.staging.search_results`

**Acceptance criteria:**
- Country names included in queries
- Unit tests with mocked responses
- Rate limit / empty result handling (loguru)

---

### 1.4 — Social media site-scoped search — [#10](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/10)

**Module:** `search/social.py` → `artlake-search-social`

**Behaviour:**
- `site:facebook.com/events`, `site:instagram.com` queries using keywords from 1.2
- Write to `artlake.staging.search_results` with source per platform

**Acceptance criteria:**
- Query templates per platform
- Unit tests with mocked responses

---

### 1.5 — Deduplication + seen-URL tracking — [#11](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/11)

**Module:** `filter/dedup.py` → `artlake-dedup`

**Behaviour:**
- Read new URLs from `artlake.staging.search_results`
- Check against `artlake.staging.seen_urls`
- **Full normalised URL** as primary dedup key
- Title as secondary signal for cross-site near-duplicates
- Write new URLs to `seen_urls` with status="pending"
- Pass only unseen URLs forward

**Acceptance criteria:**
- URL normalisation (strip tracking params, trailing slashes)
- Persists across runs
- Unit tests: duplicates, near-duplicates, same-domain-different-path (aggregator case)

---

### 1.6 — Web page content scraper — [#12](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/12)

**Module:** `scrape/pages.py` → `artlake-scrape-pages`

**Behaviour:**
- Read URLs where `processing_status = 'done'` from `staging.search_results` (after dedup)
- Fetch pages with `requests` + `beautifulsoup4`
- Extract: title, main content, dates, location mentions
- Detect PDF/image links → `artifact_urls`
- Write to `artlake.staging.scraped_pages` with `processing_status = 'new'`

**Acceptance criteria:**
- PDF/image link detection
- Timeout and error handling (connection errors, HTTP 4xx/5xx)
- Unit tests with saved HTML fixtures

**Upgrade path:** SerpAPI (paid) when richer content extraction or JS-rendered pages are needed.

---

### 1.7 — Artifact downloader — [#13](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/13)

**Module:** `scrape/download.py` → `artlake-download-artifacts`

**Behaviour:**
- Download PDFs/images to UC Volume: `artlake/volumes/raw_artifacts/{event_fingerprint}/{filename}`
- Create `EventArtifact` records
- Skip files > configurable max size

**Acceptance criteria:**
- Handles PDF, JPG, PNG, WEBP
- Content-hash dedup
- Unit tests with mocked downloads
- Integration test for UC Volume write

---

### 1.8 — Geocoding + country filter — [#14](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/14)

**Module:** `filter/country.py` → `artlake-geocode`

**Behaviour:**
- Geocode event location text → (lat, lng, country) via `geopy` + Nominatim
- Keep events in `target_countries`; filter out others
- Update `seen_urls` status to "filtered_country" for rejected
- Store lat/lng/country on accepted events

**Acceptance criteria:**
- Caching to respect Nominatim 1 req/s
- Unresolvable locations: keep event, country="unknown"
- Unit tests with known locations

**Note:** Nominatim cold-start — 1 req/s with hundreds of uncached events on first run may take 10+ minutes. Accept this for MVP; consider OpenCage Geocoder (free tier: 2500 req/day) if latency becomes a problem.

---

### 1.9 — Clean events (raw → structured) — [#15](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/15)

**Module:** `clean/events.py` → `artlake-clean-events`

**Behaviour:**
- Read from scraped pages (passed all filters)
- Parse dates (ISO, natural language, European dd/mm/yyyy)
- Normalise titles, descriptions, location text
- Write structured `CleanEvent` records to `artlake.bronze.raw_events`
- Update `seen_urls` status to "accepted"

**Acceptance criteria:**
- Date parsing handles multiple formats
- Unit tests for each parsing function
- Integration test for Delta write

---

### 1.10 — Ingestion workflow definition — [#21](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/21)

**Workflow:** `resources/ingest_events_job.yml`

**Behaviour:**
- Multi-task Databricks Workflow chaining: search → search-social → dedup → scrape-pages → download-artifacts → geocode → clean-events
- Each task is `python_wheel_task`
- Scheduled daily via DAB

**Acceptance criteria:**
- All tasks reference the correct `.whl` entry points
- Workflow deployable via `databricks bundle deploy`
- Successful dry-run on dev target

---

## Phase 2: Data Processing (Stories 2.1–2.8)

### 2.1 — Content translation — [#22](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/22)

**Module:** `translate/content.py` → `artlake-translate`

**Behaviour:**
- Read `CleanEvent` records from `artlake.bronze.raw_events`
- Translate title, description, and location text to configured language (default: English) via **Foundation Model API**
- Write to `artlake.bronze.translated_events`
- Skip events already in the target language (detected via query language tag)

**Rationale:** Translating all content to one language eliminates the need for multilingual embeddings (GTE-large-en works well on English) and simplifies categorisation.

**Acceptance criteria:**
- Supports EN, NL, DE, FR → configured target language
- Batch processing with rate limit handling
- Fallback: if translation fails, keep original text, mark `processing_status = 'failed'`
- Unit tests with mocked LLM

---

### 2.2 — Artifact processing (`ai_parse_document` + LLM summary) — [#16](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/16)

**Module:** `process_artifacts/extract.py` → `artlake-process-artifacts`

**Behaviour:**
- Read artifact file paths from UC Volumes
- PDF + image extraction via **`ai_parse_document`** (Databricks-native SQL function): `SELECT ai_parse_document(content) FROM read_files(..., format => 'binaryFile')`
- LLM: structured summary (deadline, requirements, location, fees) via Foundation Model API
- Update `EventArtifact` records

**Acceptance criteria:**
- Multi-page PDFs
- Common image formats (JPG, PNG)
- Consistent structured LLM output
- Fallback: if `ai_parse_document` or LLM fails, mark `processing_status = 'failed'`
- Unit tests with sample PDFs and images

---

### 2.3 — Rule-based event categorisation — [#17](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/17)

**Module:** `categorise/rules.py` → `artlake-categorise-rules`

**Behaviour:**
- Read from translated events
- Classify: open_call / market / exhibition / workshop / other via keyword matching (operates on translated content in configured language)
- Enrich with `artifact_summaries`
- Write `GoldEvent` records to `artlake.gold.events`

**Acceptance criteria:**
- Keyword dictionary for configured language (default: English)
- Fallback to "other"
- Unit tests per category
- Integration test

---

### 2.4 — LLM-based event categorisation — [#18](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/18)

**Module:** `categorise/llm.py` → `artlake-categorise-llm`

**Behaviour:**
- Foundation Model API classification with structured prompt + few-shot examples
- Same input/output contract as rule-based (drop-in replacement)

**Acceptance criteria:**
- Few-shot examples per category
- Comparison: rules vs LLM accuracy
- Unit tests with mocked LLM
- Config flag to switch approaches

---

### 2.5 — Vector embedding generation — [#19](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/19)

**Module:** `embed/generate.py` → `artlake-embed`

**Behaviour:**
- Read translated event descriptions
- Generate embeddings via Foundation Model API (GTE-large-en — works because content is pre-translated)
- Store in `artlake.gold.embeddings`

**Acceptance criteria:**
- Batch processing with rate limit handling
- Unit tests with mocked API

---

### 2.6 — Processing workflow definition — [#23](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/23)

**Workflow:** `resources/process_events_job.yml`

**Behaviour:**
- Multi-task Databricks Workflow chaining: translate → process-artifacts → categorise-rules → embed
- All `python_wheel_task`, triggered after ingestion or scheduled via DAB

**Acceptance criteria:**
- All tasks reference the correct `.whl` entry points
- Workflow deployable via `databricks bundle deploy`
- Successful dry-run on dev target

---

### 2.7 — Vector Search index setup — [#20](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/20)

**Module:** `embed/vector_search.py` (one-time setup)

**Behaviour:**
- Create Vector Search endpoint
- Create Delta Sync index over embeddings
- Auto-updates with new data

**Acceptance criteria:**
- Idempotent
- Verified with test query

---

### 2.8 — End-to-end integration test — [#24](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/24)

**Behaviour:**
- Run full ingestion workflow (search → clean-events) with test fixtures (mocked search results, saved HTML pages)
- Run full processing workflow (translate → embed) on ingested test data
- Verify Gold table output: correct categories, non-null embeddings, translated content
- Verify `seen_urls` tracking: re-running ingestion does not re-process same URLs

**Acceptance criteria:**
- Reproducible test data fixtures (no live web calls)
- Validates Delta table schemas match Pydantic models
- Validates `processing_status` transitions across all staging tables
- Runs in CI (mocked Databricks APIs) and as `@pytest.mark.integration` on live workspace

---

## Dependency Graph

### Code dependencies (what must be implemented before what)

```
1.1 (models) ─── 1.2 (keywords) ──┬── 1.3 (search web)
                                    └── 1.4 (search social)
                 1.1 (models) ──┬── 1.5 (dedup)
                                ├── 1.6 (scrape pages)
                                ├── 1.7 (artifact dl)
                                ├── 1.8 (geocode)
                                └── 1.9 (clean events)

                 1.9 ── 2.1 (translate) ──┬── 2.3 (categorise rules) ── 2.5 (embed)
                                           └── 2.4 (categorise LLM)
                 1.7 ── 2.2 (process artifacts)
                 2.5 ── 2.7 (vector search)
```

**Parallelisable after 1.1 + 1.2:** stories 1.3, 1.4, 1.5, 1.6, 1.7, 1.8 can be developed concurrently (they share only the data models).

### Workflow dependencies (task execution order at runtime)

```
Ingestion:  search → search-social → dedup → scrape → download → geocode → clean
Processing: translate → process-artifacts → categorise → embed
```

### Workflow definitions (depend on all tasks in the workflow being implemented)

```
1.10 (ingestion workflow)   blocked by: 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9
2.6  (processing workflow)  blocked by: 2.1, 2.2, 2.3, 2.5
2.8  (E2E test)             blocked by: 1.10, 2.6
```

## Execution plan

1. **Create GitHub issues** for new stories (1.10, 2.1, 2.6, 2.8) and update existing ones (#12, #15, #16, #17, #19)
2. Add labels: `phase:ingestion`, `phase:processing`, `feature`, `test`
3. Create milestones: "Phase 1: Ingestion", "Phase 2: Data Processing"
4. Issues reference code and workflow dependencies separately
5. Each issue → feature branch → PR → merge to main

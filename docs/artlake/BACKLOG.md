# ArtLake — Phase 1 & 2 Backlog Plan

## Context

ArtLake has architecture docs, ADRs, repo standards, CI, and tooling in place — but no application code yet. We need to create GitHub issues for the **Ingestion** and **Data Processing** phases. Phase 3 (Serving) deferred.

### Key decisions
- **No notebooks for processing** — all steps are `.whl` entry points via `python_wheel_task`
- **No Python orchestration** — Databricks Workflows is the sole orchestrator. Each module has `main()` → one workflow task.
- **Function-based module naming** — modules named for what the code does.
- **Country filter, not radius** — ingestion filters by configurable country list. Radius filtering deferred to Phase 3 (BI, user-interactive).
- **No separate language detection** — search queries are generated per target language; results tagged with query language. No lingua-based post-filter.
- **Seen-URLs tracking** — unseen URLs written to Delta table as a persistent set. Fingerprint is `sha2(url, 256)` — no normalization needed; DuckDuckGo returns canonical URLs and exact hash match covers all real duplicates.
- **Artifacts** — detect, download, and process PDFs/images. `ai_parse_document` (Databricks-native SQL function) for PDF text extraction and image OCR. Raw files in UC Volumes.
- **Categorisation** — rule-based MVP first, then LLM upgrade.
- **Social media** — separate story from general search.
- **Content translation** — all scraped content is translated to the configured language (default: English) via Foundation Model API before processing. No need for multilingual embeddings or classification.
- **Processing status tracking** — `processing_status` column (`new` → `processing` → `done` / `failed`) used only on tables for expensive operations (`scraped_pages`, `artifacts`). Not used on `search_results` or `seen_urls`.
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
│   └── generate.py          #   Multilingual query generation (base EN → translated per language) → artlake-generate-queries
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
│   ├── events.py            #   Parse dates, normalise fields      → artlake-clean-events
│   └── patterns.py          #   Generate multilingual field-label patterns via LLM → artlake-generate-language-patterns
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
artlake.staging.search_results   ← search writes here (tagged with query language and source)
artlake.staging.seen_urls        ← dedup reads/writes (persists across runs)
artlake.staging.scraped_pages    ← scrape writes here (processing_status)
artlake.staging.artifacts        ← download/process writes here (processing_status)
artlake.bronze.raw_events        ← clean-events writes here (original language)
artlake.bronze.translated_events ← translate enriches with configured language text
artlake.gold.events              ← categorise writes here
artlake.gold.embeddings          ← embed writes here (from translated content)
```

**`processing_status` values:** `new` → `processing` → `done` | `failed`
Used on `scraped_pages` and `artifacts`. Downstream tasks read rows where upstream `processing_status = 'done'`.

### Databricks Workflow structure

**Ingestion workflow:**
```
artlake-generate-queries → artlake-search → artlake-search-social → artlake-dedup → artlake-scrape-pages
→ artlake-generate-language-patterns → artlake-clean-events (for_each) → artlake-geocode → artlake-download-artifacts
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
- `RawEvent` — fingerprint (sha2), url, title, snippet, source, raw_html, scraped_at, language (from query), artifact_urls
- `CleanEvent` — fingerprint (sha2), title, description, date_start, date_end, location_text, lat, lng, country, language, source, url, artifact_urls, artifact_paths
- `GoldEvent` — extends CleanEvent with category, artifact_summaries
- `EventArtifact` — url, artifact_type (pdf/image), file_path, extracted_text, llm_summary
- `SeenUrl` — url, title, source, fingerprint (sha2(url, 256)), ingested_at
- `ArtLakeConfig` — target_countries, languages, target_language (default: "en"), categories, scrape_schedule

**Acceptance criteria:**
- Pydantic v2 with strict validation
- Unit tests (valid + invalid inputs)
- Package-importable

---

### 1.2 — Multilingual keyword generation — [#8](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/8)

**Module:** `search/generate.py` → entry point `artlake-generate-queries`

**Behaviour:**
- Read base English keywords and country/language config from `config/input/keywords.yml`
- Translate keywords to target languages via LLM (Databricks Foundation Model API)
- Generate search query strings per language × category × country (country name included in each query)
- Write output to `config/output/queries.yml` (bundled with DAB, resolved at runtime to the workspace files path)

**Acceptance criteria:**
- Keyword sets for EN, NL, DE, FR
- Unit tests verifying query generation for all combinations
- Extensible: easy to add new categories or languages

---

### 1.3 — DuckDuckGo search (general queries) — [#9](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/9)

**Module:** `search/web.py` → `artlake-search`

**Behaviour:**
- Read pre-generated queries from `config/output/queries.yml` (written by `artlake-generate-queries`)
- Execute each query via `duckduckgo-search`
- Tag each result with the query language
- Write to `artlake.staging.search_results`

**Acceptance criteria:**
- Country names included in queries (guaranteed by query generation step)
- Each result tagged with query language
- Unit tests with mocked `duckduckgo-search` responses
- Rate limit / empty result handling (loguru)

---

### 1.4 — Social media site-scoped search — [#10](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/10)

**Module:** `search/social.py` → `artlake-search-social`

**Behaviour:**
- Load target platforms from `config/input/social_platforms.yml` (mapping of `source_name → site: operator`)
- Combine each query from `queries.yml` with the site: operator per platform
- Write to `artlake.staging.search_results` with source set to the platform name

**Platform config** (`config/input/social_platforms.yml`):
```yaml
facebook: "site:facebook.com/events"
instagram: "site:instagram.com"
linkedin: "site:linkedin.com"
```

Platforms are hand-authored config (not generated), so this file lives in `config/input/` alongside `keywords.yml`. Adding or removing a platform requires no code change.

**Key design decisions:**
- Platforms passed as `dict[str, str]` to `search_social()` — no dataclass needed
- `load_platforms()` mirrors `load_queries()` pattern
- `write_results` reused from `search/web.py`

**Acceptance criteria:**
- `config/input/social_platforms.yml` drives platform list
- Source field correctly identifies the originating platform
- Unit tests with mocked responses
- Entry point `artlake-search-social` declared in `pyproject.toml`

---

### 1.5 — Deduplication + seen-URL tracking — [#11](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/11)

**Module:** `filter/dedup.py` → `artlake-dedup`

**Behaviour:**
- Read URLs from `artlake.staging.search_results`
- Compute `sha2(url, 256)` fingerprint per row
- Anti-join against `artlake.staging.seen_urls` on fingerprint
- Write only unseen URLs to `seen_urls` (url, title, source, fingerprint, ingested_at)
- `seen_urls` is a pure set — presence means seen; no status column

**Acceptance criteria:**
- Persists across runs — re-running writes 0 rows when nothing new
- Unit tests: exact duplicates, same-domain-different-path (aggregator case), within-batch duplicates
- Entry point `artlake-dedup` declared in `pyproject.toml`

---

### 1.6 — Web page content scraper — [#12](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/12)

**Module:** `scrape/pages.py` → `artlake-scrape-pages`

**Behaviour:**

Two-mode entry point, designed for Databricks `for_each_task` parallelism:

- `--mode list` — anti-join `artlake.staging.seen_urls` LEFT ANTI JOIN `artlake.staging.scraped_pages` on `fingerprint` (PK); emit the unseen URL list as a Databricks task value (`dbutils.jobs.taskValues.set`). No `processing_status` used — coordination is purely by presence in `scraped_pages`.
- `--mode scrape --url <url>` — fetch a single URL (one `for_each_task` iteration):
  1. Check `robots.txt` via stdlib `urllib.robotparser` — skip if disallowed
  2. Try `/<domain>/llms.txt` — use structured markdown content if available
  3. Fall back to `requests` + `beautifulsoup4` raw HTML
  4. Extract **raw content only**: title, body text, raw hrefs
  5. Detect PDF/image artifact links (`.pdf` hrefs, poster/flyer `<img>` heuristics) → `artifact_urls`
  6. Write one row to `artlake.staging.scraped_pages`: fingerprint (sha2, PK), url, title, raw_text, artifact_urls, `processing_status = 'new'`

No date/location extraction at this step — deferred to downstream `artlake-clean-events`.

**Acceptance criteria:**
- [ ] Anti-join on `fingerprint`, not `url` string
- [ ] `robots.txt` respected before fetching
- [ ] `llms.txt` tried before raw HTML fallback
- [ ] Raw content stored only (no structured extraction)
- [ ] `fingerprint` written as PK on `scraped_pages`
- [ ] PDF/image artifact link detection
- [ ] Timeout and error handling (connection errors, HTTP 4xx/5xx) — write `processing_status = 'failed'` on error
- [ ] Unit tests with saved HTML fixtures (no live HTTP)
- [ ] Entry point `artlake-scrape-pages` declared in `pyproject.toml`
- [ ] `resources/scrape_pages_job.yml` with `for_each_task` pattern

**Upgrade path:** SerpAPI (paid) when JS-rendered pages or richer content extraction is needed.

---

### 1.7 — Artifact downloader — [#13](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/13)

**Module:** `scrape/download.py` → `artlake-download-artifacts`

**Behaviour:**
- Read `artifact_urls` from `artlake.bronze.raw_events` where `processing_status = 'done'` (geocoded, non-outdated events only)
- Download PDFs/images to UC Volume: `artlake/volumes/raw_artifacts/{event_fingerprint}/{filename}`
- Create `EventArtifact` records
- Skip files > configurable max size

**Acceptance criteria:**
- [ ] Handles PDF, JPG, PNG, WEBP
- [ ] Content-hash deduplication (skip identical files already downloaded)
- [ ] Configurable max file size
- [ ] Unit tests with mocked downloads
- [ ] Integration test for UC Volume write (`@pytest.mark.integration`)
- [ ] Entry point `artlake-download-artifacts` declared in `pyproject.toml`

---

### 1.8 — Geocoding + country filter — [#14](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/14)

**Module:** `filter/country.py` → `artlake-geocode`

**Behaviour:**
- Read `CleanEvent` records from `artlake.bronze.raw_events` where `processing_status = 'new'` (excludes `outdated` events)
- Geocode `location_text` → (lat, lng, country) via `geopy` + Nominatim (ADR-003)
- Keep events in `target_countries`; filter out others
- Set `processing_status = 'done'` on accepted events; `processing_status = 'failed'` on unresolvable (country kept as `'unknown'`)
- Lat/lng stored for future Phase 3 BI radius filtering (ADR-013)

**Acceptance criteria:**
- [ ] Reads from `raw_events` where `processing_status = 'new'`
- [ ] Geocoding result caching to respect Nominatim 1 req/s rate limit
- [ ] Unresolvable locations: keep event, set country=`"unknown"`, `processing_status = 'failed'`
- [ ] Unit tests with known locations and expected countries
- [ ] Country detection from geocoded coordinates
- [ ] Entry point `artlake-geocode` declared in `pyproject.toml`

**Note:** Nominatim cold-start — 1 req/s with hundreds of uncached events on first run may take 10+ minutes. Accept this for MVP; consider OpenCage Geocoder (free tier: 2500 req/day) if latency becomes a problem.

---

### 1.9 — Clean events (raw → structured) — [#15](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/15)

**Modules:**
- `clean/patterns.py` → `artlake-generate-language-patterns` — reads `keywords.yml`, calls LLM once to generate multilingual field-label patterns, writes `config/output/language_patterns.yml`.
- `clean/events.py` → `artlake-clean-events` — two-mode entry point with `for_each_task` parallelism.

**Behaviour:**
- `artlake-generate-language-patterns`: derive languages/target_countries from `keywords.yml`, call LLM for title/location field labels per language, write `language_patterns.yml` (mirrors `artlake-generate-queries` pattern)
- `artlake-clean-events --mode list`: anti-join `scraped_pages` (`processing_status = 'new'`) and emit URL list as Databricks task value for `for_each_task`
- `artlake-clean-events --mode clean --url <url>`: process one page per for_each iteration:
  - Parse dates (ISO, natural language, European dd/mm/yyyy)
  - Normalise title, description, location via rule-based extraction (using `language_patterns.yml` field labels per language)
  - LLM fallback when fields are incomplete
  - Copy `artifact_urls` and `fingerprint` from `scraped_pages` to `CleanEvent`
  - Write to `artlake.bronze.raw_events`:
    - `processing_status = 'outdated'` if `date_end < today`
    - `processing_status = 'requires_manual_validation'` if extraction fails after LLM
    - `processing_status = 'new'` otherwise

**Acceptance criteria:**
- [x] Date parsing handles ISO, natural language, European dd/mm/yyyy formats
- [x] Events with past dates written with `processing_status = 'outdated'`
- [x] Events with no detected date written with `processing_status = 'new'`
- [x] Null handling for missing fields (description, dates, coordinates)
- [x] Unit tests for each parsing function
- [ ] Integration test for Delta write (`@pytest.mark.integration`)
- [x] Entry point `artlake-clean-events` declared in `pyproject.toml`
- [x] Entry point `artlake-generate-language-patterns` declared in `pyproject.toml`
- [x] `for_each_task` pattern in `resources/clean_events_job.yml`
- [x] `fingerprint` propagated from `scraped_pages` to `raw_events`

---

### 1.10 — Ingestion workflow definition — [#21](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/21)

**Workflow:** `resources/ingest_events_job.yml`

**Behaviour:**
- Multi-task Databricks Workflow chaining: search → search-social → dedup → scrape-pages → clean-events → geocode → download-artifacts
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
1.1 (models) ─── 1.2 (generate-queries) ──┬── 1.3 (search web)
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

**Parallelisable after 1.1 + 1.2:** stories 1.3, 1.4, 1.5, 1.6, 1.7, 1.8 can be developed concurrently (they share only the data models). At runtime, 1.3 and 1.4 depend on 1.2 having written `queries.yml`.

### Workflow dependencies (task execution order at runtime)

```
Ingestion:  search → search-social → dedup → scrape → clean → geocode → download
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

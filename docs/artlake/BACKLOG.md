# ArtLake — Phase 1 & 2 Backlog Plan

## Status overview

### Done ✅
| Story | Issue | Description |
|---|---|---|
| Pre-req | [#6](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/6) | Configure databricks.yml |
| 1.1 | [#7](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/7) | Core data models (Pydantic schemas) |
| 1.2 | [#8](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/8) | Multilingual keyword + query generation |
| 1.3 | [#9](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/9) | DuckDuckGo search (general queries) |
| 1.4 | [#10](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/10) | Social media site-scoped search |
| 1.5 | [#11](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/11) | Deduplication + seen-URL tracking |
| 1.6 | [#12](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/12) | Web page content scraper (for_each) |
| 1.7 | [#13](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/13) | Artifact downloader (PDFs & images → UC Volumes) |
| 1.8 | [#14](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/14) | Geocoding + country filter |
| 1.9 | [#15](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/15) | Clean events + language pattern generation (for_each) |

### Up next — ingestion filter + workflow 🔜
| Story | Issue | Description | Blocked by |
|---|---|---|---|
| 2.3 | [#17](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/17) | Rule-based event categorisation (ingestion filter, gates geocode) | #15 ✅ |
| 2.4 | [#18](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/18) | LLM-based event categorisation (ingestion filter, gates download) | #17 |
| 1.10 | [#21](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/21) | Ingestion workflow definition | #17, #18 |

### Phase 2: Processing 🔜
| Story | Issue | Description | Blocked by |
|---|---|---|---|
| 2.2 | [#16](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/16) | Artifact processing (ai_parse_document + LLM summary) | #13 ✅ |
| 2.1 | [#22](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/22) | Content translation (events + artifact summaries) | #16, #18 |
| 2.5 | [#19](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/19) | Vector embedding generation | #22 |
| 2.6 | [#23](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/23) | Processing workflow definition | #16, #22, #19 |
| 2.7 | [#20](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/20) | Vector Search index setup | #19 |
| 2.8 | [#24](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/24) | End-to-end integration test | #21, #23 |

### Recommended sequence
```
Now:   #17 (categorise-rules) → #18 (categorise-llm) → #21 (ingestion workflow)

       #16 (artifact proc) → #22 (translate: events + artifact summaries) → #19 (embed) → #23 (processing workflow)
                                                                                                │
                                                                                           #20 (vector search)
                                                                                                │
                                                                                           #24 (E2E test)
```

---

## Context

ArtLake has architecture docs, ADRs, repo standards, CI, and tooling in place. Phase 1 ingestion is largely complete (#6–#12, #15). Phase 3 (Serving) deferred.

### Key decisions
- **No notebooks for processing** — all steps are `.whl` entry points via `python_wheel_task`
- **No Python orchestration** — Databricks Workflows is the sole orchestrator. Each module has `main()` → one workflow task.
- **Function-based module naming** — modules named for what the code does.
- **Country filter, not radius** — ingestion filters by configurable country list. Radius filtering deferred to Phase 3 (BI, user-interactive).
- **No separate language detection** — search queries are generated per target language; results tagged with query language. No lingua-based post-filter.
- **Seen-URLs tracking** — unseen URLs written to Delta table as a persistent set. Fingerprint is `sha2(url, 256)` — no normalization needed; DuckDuckGo returns canonical URLs and exact hash match covers all real duplicates.
- **Artifacts** — detect, download, and process PDFs/images. `ai_parse_document` (Databricks-native SQL function) for PDF text extraction and image OCR. Raw files in UC Volumes.
- **Parallel search** — `artlake-search` and `artlake-search-social` run in parallel; both write to `search_results`; `artlake-dedup` waits on both.
- **Categorisation gates geocoding** — rule-based + LLM categorisation runs in the ingestion workflow right after `clean_events`, before `geocode` and artifact download. `non_art` events are filtered here — they never reach geocoding, artifact download, translation, or embedding, cutting all downstream compute costs.
- **Social media** — separate story from general search.
- **Content translation includes artifact summaries** — `artlake-translate` runs after artifact processing (#16) and joins `raw_events` with `artifacts` to translate both event fields (title, description, location) and artifact extracted text / LLM summaries. This gives embeddings full document content from PDFs and images. Translate depends on artifact processing; embed depends on translate.
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
artlake.bronze.raw_events        ← clean-events writes here (original language); categorise-rules + categorise-llm update category column
artlake.bronze.translated_events ← translate writes here (art-only, geocoded events; event fields + artifact summaries, all in configured language)
artlake.gold.events              ← written after translation (final categorised + translated events)
artlake.gold.embeddings          ← embed writes here (from translated content)
```

**`processing_status` values:** `new` → `processing` → `done` | `failed`
Used on `scraped_pages` and `artifacts`. Downstream tasks read rows where upstream `processing_status = 'done'`.

### Databricks Workflow structure

**Ingestion workflow:**
```
artlake-generate-queries ──┬── artlake-search ──────────┐
                            └── artlake-search-social ───┴── artlake-dedup
                                                                    │
artlake-generate-language-patterns ──┬── artlake-scrape-pages (for_each, list+scrape)
                                      └──────────────────────────────────────────────┐
                                                               artlake-clean-events (for_each, list+clean)
                                                                    │
                                                       artlake-categorise-rules → artlake-categorise-llm
                                                                    │ (non_art filtered out)
                                                              artlake-geocode
                                                                    │
                                                       artlake-download-artifacts (for_each, list+download)
```

**Processing workflow:**
```
artlake-process-artifacts → artlake-translate (events + artifact summaries) → artlake-embed
```

---

## Pre-requisite

### ✅ Configure databricks.yml — [#6](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/6)

---

## Phase 1: Ingestion (Stories 1.1–1.10)

### ✅ 1.1 — Core data models (Pydantic schemas) — [#7](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/7)

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

### ✅ 1.2 — Multilingual keyword generation — [#8](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/8)

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

### ✅ 1.3 — DuckDuckGo search (general queries) — [#9](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/9)

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

### ✅ 1.4 — Social media site-scoped search — [#10](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/10)

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

### ✅ 1.5 — Deduplication + seen-URL tracking — [#11](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/11)

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

### ✅ 1.6 — Web page content scraper — [#12](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/12)

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

### ✅ 1.7 — Artifact downloader — [#13](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/13)

**Module:** `scrape/download.py` → `artlake-download-artifacts`

**Depends on:** #17 (categorise-rules), #18 (categorise-llm)

**Behaviour:**

Two-mode entry point (`for_each_task` pattern, mirrors scrape and clean steps):

- `--mode list` — read `artifact_urls` from `artlake.bronze.raw_events` where `category NOT IN ('non_art') AND processing_status = 'done'` (art-only, geocoded events); emit artifact list as Databricks task value for `for_each_task`
- `--mode download --fingerprint <fp> --url <artifact_url>` — download one artifact per iteration:
  - Download PDFs/images to UC Volume: `artlake/volumes/raw_artifacts/{event_fingerprint}/{filename}`
  - Write `EventArtifact` record to `artlake.staging.artifacts` with `processing_status = 'new'`
  - Skip files > configurable max size

**Acceptance criteria:**
- [ ] Two-mode entry point (`--mode list` / `--mode download`) following the for_each pattern
- [ ] Reads only art-categorised, geocoded events (`category NOT IN ('non_art') AND processing_status = 'done'`)
- [ ] Handles PDF, JPG, PNG, WEBP
- [ ] Content-hash deduplication (skip identical files already downloaded)
- [ ] Configurable max file size
- [ ] Unit tests with mocked downloads
- [ ] Integration test for UC Volume write (`@pytest.mark.integration`)
- [ ] Entry point `artlake-download-artifacts` declared in `pyproject.toml`

---

### ✅ 1.8 — Geocoding + country filter — [#14](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/14)

**Module:** `filter/country.py` → `artlake-geocode`

**Behaviour:**
- Read `CleanEvent` records from `artlake.bronze.raw_events` where `category NOT IN ('non_art') AND processing_status = 'new'` (art-relevant events only — non_art filtered by categorise steps upstream)
- Geocode `location_text` → (lat, lng, country) via `geopy` + Nominatim (ADR-003)
- Keep events in `target_countries`; filter out others
- Set `processing_status = 'done'` on accepted events; `processing_status = 'failed'` on unresolvable (country kept as `'unknown'`)
- Lat/lng stored for future Phase 3 BI radius filtering (ADR-013)

**Acceptance criteria:**
- [x] Reads from `raw_events` where `processing_status = 'new'`
- [ ] **Follow-up:** add `category NOT IN ('non_art')` filter to skip non-art events (categorise now runs before geocode in the ingestion workflow)
- [x] Geocoding result caching to respect Nominatim 1 req/s rate limit
- [x] Unresolvable locations: keep event, set country=`"unknown"`, `processing_status = 'failed'`
- [x] Unit tests with known locations and expected countries
- [x] Country detection from geocoded coordinates
- [x] Entry point `artlake-geocode` declared in `pyproject.toml`

**Note:** Nominatim cold-start — 1 req/s with hundreds of uncached events on first run may take 10+ minutes. Accept this for MVP; consider OpenCage Geocoder (free tier: 2500 req/day) if latency becomes a problem.

---

### ✅ 1.9 — Clean events (raw → structured) — [#15](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/15)

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

### 🔜 1.10 — Ingestion workflow definition — [#21](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/21)

**Workflow:** `resources/ingest_events_job.yml`

**Behaviour:**
- Multi-task Databricks Workflow: [`generate-queries` ∥ `generate-category-examples`] → [`search` ∥ `search-social`] → `dedup` → [`scrape-pages` (for_each) ∥ `generate-language-patterns`] → `clean-events` (for_each) → `categorise-rules` → `categorise-llm` → `geocode` → `download-artifacts` (for_each)
- Each task is `python_wheel_task`
- `search` and `search-social` run as parallel tasks (both depend on `generate-queries`, both feed `dedup`)
- `generate-language-patterns` runs in parallel with `scrape-pages` (no shared dependency; both feed `clean-events`)
- Scheduled daily via DAB

**Acceptance criteria:**
- [ ] All tasks reference the correct `.whl` entry points
- [ ] `search` and `search-social` configured as parallel tasks
- [ ] `generate-language-patterns` runs in parallel with `scrape-pages` for_each group
- [ ] Workflow deployable via `databricks bundle deploy`
- [ ] Successful dry-run on dev target

---

## Phase 2: Data Processing (Stories 2.1–2.8)

### 🔜 2.1 — Content translation — [#22](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/22)

**Module:** `translate/content.py` → `artlake-translate`

**Depends on:** #16 (artifact processing), #18 (categorise-llm — confirms art category)

**Behaviour:**
- Read art-categorised, geocoded `CleanEvent` records from `artlake.bronze.raw_events` (`category NOT IN ('non_art') AND processing_status = 'done'`)
- Join with `artlake.staging.artifacts` to include `extracted_text` and `llm_summary` from processed artifacts
- Translate event fields (title, description, location_text) **and** artifact text to the configured language (default: English) via **Foundation Model API**
- Write combined translated record to `artlake.bronze.translated_events` (event fields + translated artifact summaries)
- Skip events already in the target language (detected via query language tag)

**Rationale:** Artifact PDFs and images often contain the most structured content (deadlines, fees, requirements for open calls). Translating artifact text alongside event fields ensures embeddings cover the full document content. Artifact processing must complete first so its output is available for translation.

**Acceptance criteria:**
- [ ] Reads only art-categorised, geocoded events
- [ ] Joins with `artifacts` table to include `extracted_text` and `llm_summary` per event
- [ ] Translates event fields and artifact text in one pass
- [ ] Supports EN, NL, DE, FR → configured target language
- [ ] Batch processing with rate limit handling
- [ ] Fallback: if translation fails, keep original text, mark `processing_status = 'failed'`
- [ ] Unit tests with mocked LLM (event-only and event+artifact cases)

---

### 🔜 2.2 — Artifact processing (`ai_parse_document` + LLM summary) — [#16](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/16)

**Module:** `process_artifacts/extract.py` → `artlake-process-artifacts`

**Depends on:** #13 (artifact downloader)

**Behaviour:**
- Read artifact records from `artlake.staging.artifacts` where `processing_status = 'new'`
- PDF + image extraction via **`ai_parse_document`** (Databricks-native SQL function): `SELECT ai_parse_document(content) FROM read_files(..., format => 'binaryFile')`
- LLM: structured summary (deadline, requirements, location, fees) via Foundation Model API
- Update `EventArtifact` records in `artlake.staging.artifacts` with `extracted_text`, `llm_summary`, and `processing_status = 'done'`
- Output feeds directly into `artlake-translate` — translate reads artifact summaries to include in translated content

**Acceptance criteria:**
- [ ] Reads from `artifacts` where `processing_status = 'new'`
- [ ] Multi-page PDFs
- [ ] Common image formats (JPG, PNG)
- [ ] Consistent structured LLM output (deadline, requirements, location, fees fields)
- [ ] Fallback: if `ai_parse_document` or LLM fails, mark `processing_status = 'failed'`
- [ ] Unit tests with sample PDFs and images

---

### 🔜 2.3 — Rule-based event categorisation — [#17](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/17)

**Module:** `categorise/rules.py` → `artlake-categorise-rules`

**Depends on:** #15 ✅

**Behaviour:**
- Read `CleanEvent` records from `artlake.bronze.raw_events` (runs **before** translation to filter non-art events early and save LLM costs)
- Classify: `open_call` / `market` / `exhibition` / `workshop` / `non_art` / `uncertain` via multilingual keyword matching (keywords already available in `language_patterns.yml` and `keywords.yml` per language)
- `non_art` — clearly irrelevant events; skipped by all downstream steps
- `uncertain` — passed to LLM-based categorisation (#18) for a second opinion
- Write category back to `artlake.bronze.raw_events`; do **not** write to `gold.events` yet (gold write happens after translation)

**Acceptance criteria:**
- [ ] Multilingual keyword dictionaries (EN, NL, DE, FR) loaded from `keywords.yml`
- [ ] Outputs `non_art` for clearly irrelevant events and `uncertain` for borderline cases
- [ ] Fallback to `uncertain` when no rule matches (not `other` — LLM decides)
- [ ] Unit tests per category with representative multilingual event texts
- [ ] Integration test for `raw_events` read/write
- [ ] Entry point `artlake-categorise-rules` declared in `pyproject.toml`

---

### 🔜 2.4 — LLM-based event categorisation — [#18](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/18)

**Module:** `categorise/llm.py` → `artlake-categorise-llm`

**Depends on:** #17

**Behaviour:**
- Read `CleanEvent` records from `artlake.bronze.raw_events` where `category = 'uncertain'` (borderline cases not resolved by rule-based step)
- Foundation Model API classification with structured prompt + few-shot examples — prompt handles raw multilingual content (no translation needed at this stage)
- Outputs same categories as rule-based: `open_call` / `market` / `exhibition` / `workshop` / `non_art` (no `uncertain` — LLM must decide)
- Writes final category back to `artlake.bronze.raw_events`

**Acceptance criteria:**
- [ ] Reads only `uncertain` events from `raw_events`
- [ ] Few-shot examples per category in multiple languages
- [ ] No `uncertain` output — LLM always resolves to a definitive category
- [ ] Unit tests with mocked LLM responses
- [ ] Integration test for `raw_events` category update
- [ ] Entry point `artlake-categorise-llm` declared in `pyproject.toml`

---

### 🔜 2.5 — Vector embedding generation — [#19](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/19)

**Module:** `embed/generate.py` → `artlake-embed`

**Behaviour:**
- Read records from `artlake.bronze.translated_events` — each record contains translated event fields + translated artifact summaries
- Concatenate title, description, and artifact summaries into a single text chunk per event
- Generate embeddings via Foundation Model API (GTE-large-en — works because all content is pre-translated)
- Store in `artlake.gold.embeddings`

**Acceptance criteria:**
- Batch processing with rate limit handling
- Unit tests with mocked API

---

### 🔜 2.6 — Processing workflow definition — [#23](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/23)

**Workflow:** `resources/process_events_job.yml`

**Behaviour:**
- Multi-task Databricks Workflow: `process-artifacts` → `translate` (events + artifact summaries) → `embed`
- All `python_wheel_task`, triggered after ingestion workflow completes or scheduled via DAB

**Acceptance criteria:**
- All tasks reference the correct `.whl` entry points
- Workflow deployable via `databricks bundle deploy`
- Successful dry-run on dev target

---

### 🔜 2.7 — Vector Search index setup — [#20](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/20)

**Module:** `embed/vector_search.py` (one-time setup)

**Behaviour:**
- Create Vector Search endpoint
- Create Delta Sync index over embeddings
- Auto-updates with new data

**Acceptance criteria:**
- Idempotent
- Verified with test query

---

### 🔜 2.8 — End-to-end integration test — [#24](https://github.com/llmops-databricks-1/llmops-databricks-course-victor-kuznetsov/issues/24)

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
1.1 (models) ─── 1.2 (generate-queries) ──┬── 1.3 (search web)   ─┐
                                            └── 1.4 (search social) ─┴── 1.5 (dedup)
                                                                              │
                                                              1.6 (scrape pages, for_each)
                                                                              │
                                            1.9 (generate-language-patterns) ┤
                                                                              │
                                                               1.9 (clean events, for_each)
                                                                              │
                                                 2.3 (categorise rules) ── 2.4 (categorise LLM)
                                                                              │
                                                                      1.8 (geocode)
                                                                              │
                                                              1.7 (artifact dl, for_each)
                                                                              │
                                                              2.2 (process artifacts)
                                                                              │
                                                              2.1 (translate: events + artifacts)
                                                                              │
                                                                       2.5 (embed)
                                                                              │
                                                                    2.7 (vector search)
```

**Parallelisable at runtime:** `search` ∥ `search-social`; `scrape-pages` for_each ∥ `generate-language-patterns`. Development of 1.3, 1.4, 1.5, 1.6, 1.7 can proceed concurrently (share only data models).

### Workflow dependencies (task execution order at runtime)

```
Ingestion:  [search ∥ search-social] → dedup → [scrape-pages (for_each) ∥ generate-language-patterns]
                → clean-events (for_each) → categorise-rules → categorise-llm → geocode → download-artifacts (for_each)

Processing: process-artifacts → translate (events + artifact summaries) → embed
```

### Workflow definitions (depend on all tasks in the workflow being implemented)

```
1.10 (ingestion workflow)   blocked by: 1.3, 1.4, 1.5, 1.6, 1.7 (now for_each), 1.8, 1.9, 2.3, 2.4
2.6  (processing workflow)  blocked by: 2.2, 2.1, 2.5
2.8  (E2E test)             blocked by: 1.10, 2.6
```

## Execution plan

1. **Create GitHub issues** for new stories (1.10, 2.1, 2.6, 2.8) and update existing ones (#12, #15, #16, #17, #19)
2. Add labels: `phase:ingestion`, `phase:processing`, `feature`, `test`
3. Create milestones: "Phase 1: Ingestion", "Phase 2: Data Processing"
4. Issues reference code and workflow dependencies separately
5. Each issue → feature branch → PR → merge to main

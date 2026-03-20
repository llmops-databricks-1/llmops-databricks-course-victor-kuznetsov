# ArtLake — Technology Decisions

> This document records key technology choices, their rationale, and trade-offs.
> Guiding principles: **Databricks-native where possible**, **free/open-source preferred**, paid tools only when quality or maintenance effort clearly justifies the cost.

---

## ADR-001 · Search & Web Scraping

**Decision**: Use `duckduckgo-search` (Python library) as the primary search engine wrapper, with `playwright` + `beautifulsoup4` for page scraping.

**Rationale**:
- `duckduckgo-search` is free, requires no API key, and returns results in all four target languages (EN/NL/DE/FR). Sufficient for training-course data volumes.
- `playwright` handles JavaScript-rendered pages; `beautifulsoup4` handles static HTML — together they cover most source types.
- All three are open source and run natively in Databricks notebooks as Python packages.

**Upgrade path**: Switch to **SerpAPI** (~$50/month, 5000 queries) if DuckDuckGo result quality or rate limits prove insufficient at production volumes.

**Rejected**:
- Google Custom Search API — free tier limited to 100 queries/day, too restrictive.
- Direct Google scraping — ToS violation.

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

## ADR-004 · Language Detection

**Decision**: Use `lingua-language-detector` (open source).

**Rationale**:
- Best accuracy among Python language detection libraries for short texts (event titles, snippets).
- Handles NL, DE, FR, EN reliably — the four target languages.
- Free, no external service dependency.

**Rejected**: `langdetect` — lower accuracy on short or mixed-language strings.

---

## ADR-005 · Embeddings

**Decision**: Use **Databricks Foundation Model API** (BGE-large-en-v1.5 or `databricks-bge-large-en`) hosted on the workspace.

**Rationale**:
- Included in Databricks workspace — no additional cost.
- Databricks-native: integrates directly with Vector Search and Agent Framework.
- BGE-large produces high-quality multilingual embeddings suitable for EN/NL/DE/FR event descriptions.

**Upgrade path**: OpenAI `text-embedding-3-small` if multilingual quality proves insufficient (cost: ~$0.02/1M tokens).

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

## Summary

| Layer | Tool | Cost |
|---|---|---|
| Search | `duckduckgo-search` | Free |
| Scraping | `playwright` + `beautifulsoup4` | Free |
| Geocoding | Nominatim / `geopy` | Free |
| Language detection | `lingua-language-detector` | Free |
| Orchestration | Databricks Workflows | Databricks-native |
| Raw storage | Unity Catalog Volumes | Databricks-native |
| Structured storage | Delta Lake + Unity Catalog | Databricks-native |
| Secret management | Databricks Secrets | Databricks-native |
| Embeddings | Databricks Foundation Model API (BGE) | Free (workspace) |
| Vector search | Databricks Vector Search | Databricks-native |
| RAG / Agents | Databricks Agent Framework + MLflow | Databricks-native |
| LLM | Databricks Foundation Model API (Llama 3) | Free (workspace) |
| NL interface | Databricks AI/BI Genie | Databricks-native |
| Dashboards | Databricks AI/BI Dashboards | Databricks-native |

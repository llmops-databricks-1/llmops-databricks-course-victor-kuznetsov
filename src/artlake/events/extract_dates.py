"""Extract structured event dates and fields from scraped pages.

Entry points:
  artlake-extract-dates            — bulk Spark: process all NEW scraped pages
  artlake-generate-language-patterns — generate language_patterns.yml from keywords

Extraction funnel per page:
  1. Parse dates rule-based (dateparser — multi-language, multi-format).
  2. Determine event_status: future / finished / undefined.
  3. Extract title / description / location rule-based (regex heuristics).
  4. LLM fallback for fields still missing after step 3.

Writes EventDate records to bronze.event_dates.
"""

from __future__ import annotations

import html
import json
import re
import unicodedata
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

import backoff
import yaml
from loguru import logger
from openai import OpenAI
from pydantic import BaseModel, ConfigDict

from artlake.models.event import EventDate, EventStatus, ProcessingStatus, ScrapedPage
from artlake.search.models import KeywordConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DATE_SEARCH_CHARS = 3000
_TEXT_TRUNCATE = 3000
_DATE_HORIZON_DAYS = 730
_DATE_FUTURE_DAYS = 365 * 100

_EXTRACTION_SYSTEM = (
    "You are a data extraction assistant. Extract structured event information "
    "from web page text. Respond with ONLY a JSON object, no other text."
)

_EXTRACTION_PROMPT = """\
Extract event information from the following text.

Return ONLY a JSON object with these exact keys:
{{
  "title": "event title or null",
  "description": "brief event description (max 500 chars) or null",
  "date_start": "YYYY-MM-DD or null",
  "date_end": "YYYY-MM-DD or null",
  "location_text": "venue/address or null"
}}

Text:
{text}"""


# ---------------------------------------------------------------------------
# Language patterns (was clean/patterns.py)
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_PATTERNS = (
    "You are a multilingual data extraction assistant for art event web pages."
)

_USER_PROMPT_PATTERNS = """\
Given these language codes: {languages}

Generate field labels that appear on art event web pages immediately before a colon,
for two field types:

1. title: labels identifying the event name
   (e.g. "Title: Open Call" or "Titel: Kunstmarkt")
2. location: labels identifying the venue or address
   (e.g. "Location: Amsterdam" or "Adresse: Paris")

Return ONLY a JSON object with this structure:
{{
  "title_keywords": {{
    "en": ["Title", "Event", "Name"],
    "nl": ["Titel", "Evenement", "Naam"],
    ...
  }},
  "location_keywords": {{
    "en": ["Location", "Venue", "Address", "Place"],
    "nl": ["Locatie", "Adres"],
    ...
  }}
}}

Include English ("en") regardless of the input list. Keep each list concise (3-6 labels).
Languages: {languages}"""


class LanguagePatterns(BaseModel):
    """Schema for config/output/language_patterns.yml."""

    model_config = ConfigDict(strict=True)

    generated_at: str
    model: str
    languages: list[str]
    target_countries: list[str]
    title_keywords: dict[str, list[str]]
    location_keywords: dict[str, list[str]]


def build_field_re(keywords: dict[str, list[str]]) -> re.Pattern[str]:
    """Build a compiled colon-based extraction regex from per-language keyword lists."""
    all_keywords: list[str] = []
    for kws in keywords.values():
        all_keywords.extend(kws)
    all_keywords.sort(key=len, reverse=True)
    escaped = [re.escape(kw) for kw in all_keywords]
    pattern = r"(?:^|\b)(?:" + "|".join(escaped) + r")\s*:\s*(.+)"
    return re.compile(pattern, re.IGNORECASE | re.MULTILINE)


def load_patterns(path: Path) -> LanguagePatterns:
    """Load LanguagePatterns from a YAML file."""
    raw = yaml.safe_load(path.read_text())
    return LanguagePatterns(**raw)


def _create_default_client() -> OpenAI:  # pragma: no cover
    """Create an OpenAI client using Databricks workspace auth."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    host = w.config.host or ""
    token = w.tokens.create(lifetime_seconds=1200).token_value
    return OpenAI(
        api_key=token,
        base_url=f"{host.rstrip('/')}/serving-endpoints",
    )


@backoff.on_exception(backoff.expo, Exception, max_tries=3)
def _call_patterns_llm(
    client: OpenAI,
    model: str,
    languages: list[str],
) -> dict[str, dict[str, list[str]]]:
    languages_str = ", ".join(languages)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT_PATTERNS},
            {
                "role": "user",
                "content": _USER_PROMPT_PATTERNS.format(languages=languages_str),
            },
        ],
        temperature=0.0,
        max_tokens=512,
    )
    content = response.choices[0].message.content or ""
    cleaned = re.sub(r"```(?:json)?\s*", "", content).strip()
    result: dict[str, dict[str, list[str]]] = json.loads(cleaned)
    return result


def generate_patterns(
    keywords_path: Path,
    output_path: Path,
    *,
    model: str = "databricks-meta-llama-3-3-70b-instruct",
    client: OpenAI | None = None,
) -> LanguagePatterns:
    """Read keywords.yml, call LLM to generate extraction patterns, write YAML."""
    raw = yaml.safe_load(keywords_path.read_text())
    config = KeywordConfig(**raw)

    target_countries = [country.code for country in config.countries]

    lang_set: set[str] = {"en"}
    for country in config.countries:
        lang_set.update(country.languages)
    languages = sorted(lang_set)

    if client is None:
        client = _create_default_client()  # pragma: no cover

    logger.info("Generating extraction patterns for languages: {}", languages)
    result = _call_patterns_llm(client, model, languages)

    output = LanguagePatterns(
        generated_at=datetime.now(tz=UTC).isoformat(),
        model=model,
        languages=languages,
        target_countries=target_countries,
        title_keywords=result.get("title_keywords", {}),
        location_keywords=result.get("location_keywords", {}),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.dump(
            output.model_dump(),
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )
    )
    logger.info("Wrote language patterns to {}", output_path)
    return output


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------


def parse_dates(
    text: str, languages: list[str] | None = None
) -> tuple[datetime | None, datetime | None]:
    """Extract start and end dates from raw text using dateparser.

    Args:
        text: Raw page text to search for dates.
        languages: BCP-47 language codes (e.g. ["NL", "EN"]). Codes are
            lowercased before passing to dateparser. Defaults to ["en"].
    """
    from dateparser.search import search_dates  # type: ignore[import-untyped]

    snippet = text[:_DATE_SEARCH_CHARS]
    results = search_dates(
        snippet,
        languages=[lang.lower() for lang in languages] if languages else ["en"],
        settings={"PREFER_DAY_OF_MONTH": "first", "RETURN_AS_TIMEZONE_AWARE": True},
    )
    if not results:
        return None, None

    now = datetime.now(UTC)
    cutoff = now - timedelta(days=_DATE_HORIZON_DAYS)
    max_future = now + timedelta(days=_DATE_FUTURE_DAYS)
    seen: set[tuple[int, int, int]] = set()
    dates: list[datetime] = []
    for _, dt in results:
        key = (dt.year, dt.month, dt.day)
        if key not in seen and cutoff <= dt <= max_future:
            seen.add(key)
            dates.append(dt)

    dates.sort()

    if not dates:
        return None, None
    if len(dates) == 1:
        return dates[0], None
    return dates[0], dates[-1]


# ---------------------------------------------------------------------------
# Event status
# ---------------------------------------------------------------------------


def get_event_status(
    date_start: datetime | None, date_end: datetime | None
) -> EventStatus:
    """Return the event status based on dates."""
    if date_start is None and date_end is None:
        return EventStatus.UNDEFINED
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    reference = date_end if date_end is not None else date_start
    assert reference is not None
    if reference < today:
        return EventStatus.FINISHED
    return EventStatus.FUTURE


# ---------------------------------------------------------------------------
# Text normalisation
# ---------------------------------------------------------------------------

_TITLE_SUFFIX_RE = re.compile(r"\s*[|\-–—]\s*.+$")


def _normalize_text(text: str) -> str:
    text = html.unescape(text)
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _clean_title(title: str) -> str:
    title = _normalize_text(title)
    title = _TITLE_SUFFIX_RE.sub("", title).strip()
    return title


# ---------------------------------------------------------------------------
# Rule-based field extraction
# ---------------------------------------------------------------------------


def _extract_field(text: str, field_re: re.Pattern[str]) -> str | None:
    match = field_re.search(text)
    if match:
        value = match.group(1).strip()
        value = re.split(r"\.\s+|\n|\r|;\s+", value)[0].strip()
        return value if value else None
    return None


def _looks_like_html(text: str) -> bool:
    stripped = text.lstrip()
    return stripped.startswith("<!DOCTYPE") or stripped.startswith("<html")


def extract_fields_rule_based(
    page: ScrapedPage,
    location_re: re.Pattern[str],
    title_re: re.Pattern[str],
) -> dict[str, str | None]:
    """Extract title, description, and location_text using heuristics."""
    title: str | None = _clean_title(page.title) if page.title else None

    if page.raw_text and _looks_like_html(page.raw_text):
        return {"title": title, "description": None, "location_text": None}

    if not title and page.raw_text:
        raw_title = _extract_field(page.raw_text, title_re)
        if raw_title:
            title = _clean_title(raw_title[:200])
        else:
            for line in page.raw_text.splitlines():
                stripped = _normalize_text(line)
                if len(stripped) > 10 and not stripped.startswith("<"):
                    title = _clean_title(stripped[:200])
                    break

    description: str | None = None
    if page.raw_text:
        raw = _normalize_text(page.raw_text[:500])
        description = raw or None

    location_text: str | None = None
    if page.raw_text:
        raw_loc = _extract_field(page.raw_text, location_re)
        location_text = _normalize_text(raw_loc) if raw_loc else None

    return {"title": title, "description": description, "location_text": location_text}


# ---------------------------------------------------------------------------
# LLM fallback extraction
# ---------------------------------------------------------------------------


def _parse_json_response(content: str) -> dict[str, str | None]:
    cleaned = re.sub(r"```(?:json)?\s*", "", content).strip()
    result: dict[str, str | None] = json.loads(cleaned)
    return result


@backoff.on_exception(backoff.expo, Exception, max_tries=3)
def _call_extraction_llm(client: OpenAI, model: str, text: str) -> dict[str, str | None]:
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": _EXTRACTION_SYSTEM},
            {"role": "user", "content": _EXTRACTION_PROMPT.format(text=text)},
        ],
        temperature=0.0,
        max_tokens=512,
    )
    content = response.choices[0].message.content or ""
    return _parse_json_response(content)


def extract_fields_llm(
    page: ScrapedPage, client: OpenAI, model: str
) -> dict[str, str | None] | None:
    """Call the LLM to extract structured fields. Returns None on failure."""
    text = page.raw_text[:_TEXT_TRUNCATE] if page.raw_text else ""
    if not text:
        return None
    try:
        return _call_extraction_llm(client, model, text)
    except Exception:
        logger.warning("LLM extraction failed for {}", page.url)
        return None


# ---------------------------------------------------------------------------
# Field completeness + merging
# ---------------------------------------------------------------------------


def _fields_complete(fields: dict[str, str | None]) -> bool:
    return bool(
        fields.get("title") and fields.get("description") and fields.get("location_text")
    )


def _merge_fields(
    base: dict[str, str | None], override: dict[str, str | None]
) -> dict[str, str | None]:
    return {k: base.get(k) or override.get(k) for k in base}


def _parse_llm_date(date_str: str | None) -> datetime | None:
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str).replace(tzinfo=UTC)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# EventDate assembly
# ---------------------------------------------------------------------------


def _source_from_url(url: str) -> str:
    return urlparse(url).netloc or str(url)


def _country_from_url(url: str, target_countries: list[str]) -> str | None:
    netloc = urlparse(url).netloc.lower()
    for code in target_countries:
        if netloc.endswith(f".{code.lower()}"):
            return code
    return None


def _make_event_date(
    page: ScrapedPage,
    language: str,
    date_start: datetime | None,
    date_end: datetime | None,
    fields: dict[str, str | None],
    event_status: EventStatus,
    target_countries: list[str],
    query_country: str | None = None,
) -> EventDate:
    return EventDate(
        fingerprint=page.fingerprint,
        title=fields.get("title") or page.title or str(page.url),
        description=fields.get("description") or "",
        date_start=date_start,
        date_end=date_end,
        location_text=fields.get("location_text") or "",
        language=language,
        source=_source_from_url(str(page.url)),
        url=page.url,
        artifact_urls=page.artifact_urls,
        query_country=query_country,
        domain_country=_country_from_url(str(page.url), target_countries),
        event_status=event_status,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_dates(
    page: ScrapedPage,
    language: str,
    client: OpenAI,
    model: str,
    patterns: LanguagePatterns,
    query_country: str | None = None,
) -> EventDate:
    """Apply the extraction funnel to a single ScrapedPage and return an EventDate.

    Funnel:
      1. Parse dates rule-based.
      2. Extract fields rule-based.
      3. LLM fallback for incomplete fields; fill missing dates from LLM output.
      4. Determine event_status (future / finished / undefined).
    """
    location_re = build_field_re(patterns.location_keywords)
    title_re = build_field_re(patterns.title_keywords)

    date_start, date_end = parse_dates(page.raw_text or "", patterns.languages)
    fields = extract_fields_rule_based(page, location_re, title_re)

    if not _fields_complete(fields):
        logger.info("Falling back to LLM extraction for {}", page.url)
        llm_result = extract_fields_llm(page, client, model)
        if llm_result:
            fields = _merge_fields(fields, llm_result)
            if date_start is None:
                date_start = _parse_llm_date(llm_result.get("date_start"))
            if date_end is None:
                date_end = _parse_llm_date(llm_result.get("date_end"))

    event_status = get_event_status(date_start, date_end)
    logger.info("Event {} status: {}", page.url, event_status)

    return _make_event_date(
        page,
        language,
        date_start,
        date_end,
        fields,
        event_status,
        patterns.target_countries,
        query_country=query_country,
    )


# ---------------------------------------------------------------------------
# Spark integration (pragma: no cover — tested via integration marker)
# ---------------------------------------------------------------------------


def _event_date_schema() -> StructType:  # pragma: no cover
    from pyspark.sql.types import (
        ArrayType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("fingerprint", StringType(), False),
            StructField("title", StringType(), False),
            StructField("description", StringType(), False),
            StructField("date_start", TimestampType(), True),
            StructField("date_end", TimestampType(), True),
            StructField("location_text", StringType(), False),
            StructField("query_country", StringType(), True),
            StructField("domain_country", StringType(), True),
            StructField("language", StringType(), False),
            StructField("source", StringType(), False),
            StructField("url", StringType(), False),
            StructField("artifact_urls", ArrayType(StringType()), False),
            StructField("event_status", StringType(), False),
            StructField("ingested_at", StringType(), False),
        ]
    )


def _write_event_dates(  # pragma: no cover
    spark: SparkSession,
    event_rows: list[dict[str, object]],
    event_dates_table: str,
) -> None:
    schema = _event_date_schema()
    df = spark.createDataFrame(event_rows, schema=schema)

    parts = event_dates_table.split(".")
    if len(parts) == 3:
        catalog, db, _ = parts
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db}")

    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(event_dates_table)
    )


def run_extract_dates(  # pragma: no cover
    scraped_pages_table: str,
    search_results_table: str,
    event_dates_table: str,
    patterns_path: Path,
    *,
    model: str,
    client: OpenAI | None = None,
    env: str = "dev",
) -> int:
    """Read all NEW ScrapedPage rows, extract event dates, write EventDate rows.

    Args:
        scraped_pages_table: Fully-qualified staging.scraped_pages Delta table.
        search_results_table: Fully-qualified staging.search_results Delta table
            (used to look up language and query_country per URL).
        event_dates_table: Fully-qualified bronze.event_dates Delta table.
        patterns_path: Path to the language_patterns YAML file.
        model: Databricks Foundation Model ID for LLM fallback extraction.
        client: Pre-built OpenAI client (created from workspace token if None).
        env: Deployment environment tag (dev/tst/acc/prd).

    Returns the number of EventDate rows written.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

    patterns = load_patterns(patterns_path)

    pages_df = spark.table(scraped_pages_table).filter(
        F.col("processing_status") == ProcessingStatus.NEW
    )

    if spark.catalog.tableExists(search_results_table):
        search_df = spark.table(search_results_table)
        join_cols = ["url", "language"]
        if "query_country" in search_df.columns:
            join_cols.append("query_country")
        lang_df = search_df.select(*join_cols)
        pages_df = pages_df.join(lang_df, on="url", how="left")
        if "query_country" not in pages_df.columns:
            pages_df = pages_df.withColumn("query_country", F.lit(None).cast("string"))
    else:
        pages_df = pages_df.withColumn("language", F.lit("UNKNOWN"))
        pages_df = pages_df.withColumn("query_country", F.lit(None).cast("string"))

    pages_df = pages_df.fillna({"language": "UNKNOWN"})

    rows = pages_df.collect()
    logger.info("Extracting dates from {} new scraped pages", len(rows))

    if not rows:
        logger.info("No new pages to process")
        return 0

    if client is None:
        client = _create_default_client()

    event_rows = []
    for row in rows:
        page = ScrapedPage(
            fingerprint=row["fingerprint"],
            url=row["url"],
            title=row["title"] or "",
            raw_text=row["raw_text"] or "",
            artifact_urls=list(row["artifact_urls"] or []),
            processing_status=ProcessingStatus(row["processing_status"]),
            robots_allowed=row["robots_allowed"],
            error=row["error"],
        )
        language = (row["language"] or "unknown").upper()
        query_country = row["query_country"] or None
        event = extract_dates(page, language, client, model, patterns, query_country)
        r = event.model_dump(mode="python")
        r["url"] = str(r["url"])
        r["ingested_at"] = str(r["ingested_at"])
        event_rows.append(r)

    _write_event_dates(spark, event_rows, event_dates_table)
    logger.info("Wrote {} EventDate rows to {}", len(event_rows), event_dates_table)
    return len(event_rows)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def main() -> None:  # pragma: no cover
    """Entry point for artlake-extract-dates wheel task.

    Reads all NEW scraped pages from staging.scraped_pages, extracts event
    dates and fields (rule-based + LLM fallback), and writes EventDate rows
    to bronze.event_dates.
    """
    import argparse

    parser = argparse.ArgumentParser(description="ArtLake event date extractor")
    parser.add_argument(
        "--scraped-pages-table",
        default="artlake.staging.scraped_pages",
        help="Fully-qualified scraped_pages Delta table",
    )
    parser.add_argument(
        "--search-results-table",
        default="artlake.staging.search_results",
        help="Fully-qualified search_results Delta table (for language lookup)",
    )
    parser.add_argument(
        "--event-dates-table",
        default="artlake.bronze.event_dates",
        help="Fully-qualified event_dates Delta table",
    )
    parser.add_argument(
        "--model",
        default="databricks-meta-llama-3-3-70b-instruct",
        help="Databricks Foundation Model for LLM fallback extraction",
    )
    parser.add_argument(
        "--env",
        default="dev",
        help="Deployment environment (dev/tst/acc/prd)",
    )
    parser.add_argument(
        "--language-patterns",
        type=Path,
        default=Path("config/output/language_patterns.yml"),
        help="Path to the language_patterns YAML file",
    )
    args = parser.parse_args()
    run_extract_dates(
        scraped_pages_table=args.scraped_pages_table,
        search_results_table=args.search_results_table,
        event_dates_table=args.event_dates_table,
        patterns_path=args.language_patterns,
        model=args.model,
        env=args.env,
    )


def main_generate_patterns() -> None:  # pragma: no cover
    """Entry point for artlake-generate-language-patterns wheel task."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate multilingual extraction patterns via LLM."
    )
    parser.add_argument(
        "--keywords",
        type=Path,
        default=Path("config/input/keywords.yml"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("config/output/language_patterns.yml"),
    )
    parser.add_argument(
        "--model",
        default="databricks-meta-llama-3-3-70b-instruct",
    )
    args = parser.parse_args()
    generate_patterns(args.keywords, args.output, model=args.model)

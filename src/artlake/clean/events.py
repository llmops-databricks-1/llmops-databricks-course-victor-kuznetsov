"""Clean scraped pages into structured CleanEvent records.

Entry point: artlake-clean-events

Extraction funnel per page:
  1. Parse dates rule-based (dateparser — multi-language, multi-format).
  2. Flag outdated events early → write with processing_status='outdated'.
  3. Extract title / description / location rule-based (regex heuristics).
  4. LLM fallback for fields still missing after step 3.
  5. Still incomplete after LLM → processing_status='requires_manual_validation'.
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
from loguru import logger
from openai import OpenAI

from artlake.clean.patterns import LanguagePatterns, build_field_re, load_patterns
from artlake.models.event import CleanEvent, ProcessingStatus, ScrapedPage

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DATE_SEARCH_CHARS = 3000  # chars fed to dateparser.search
_TEXT_TRUNCATE = 3000  # chars sent to LLM
_DATE_HORIZON_DAYS = 730  # filter out dates > 2 years old (publication/copyright noise)
_DATE_FUTURE_DAYS = 365 * 100  # filter out dates > 100 years ahead (parser artefacts)

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
# Date parsing
# ---------------------------------------------------------------------------


def parse_dates(
    text: str, languages: list[str] | None = None
) -> tuple[datetime | None, datetime | None]:
    """Extract start and end dates from raw text using dateparser.

    Returns (date_start, date_end). Both may be None if no dates are found.
    Dates more than two years in the past are discarded (copyright / publication noise).

    Args:
        text: Raw page text to search for dates.
        languages: BCP-47 language codes to pass to dateparser (e.g. ["en", "nl"]).
            Defaults to ["en"] when None. Passing None to dateparser enables
            auto-detection across all languages, which produces too many false
            positives (e.g. common words parsed as today's date).
    """
    from dateparser.search import search_dates  # type: ignore[import-untyped]

    snippet = text[:_DATE_SEARCH_CHARS]
    results = search_dates(
        snippet,
        languages=languages or ["en"],
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
# Outdated check
# ---------------------------------------------------------------------------


def is_outdated(date_start: datetime | None, date_end: datetime | None) -> bool:
    """Return True if the event has already ended (or started, when no end date).

    Events with no detected date are assumed ongoing → not outdated.
    """
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    if date_end is not None:
        return date_end < today
    if date_start is not None:
        return date_start < today
    return False


# ---------------------------------------------------------------------------
# Text normalisation
# ---------------------------------------------------------------------------

# Site-name suffix patterns: "Title | Gallery", "Title - Site", "Title – Brand"
_TITLE_SUFFIX_RE = re.compile(r"\s*[|\-–—]\s*.+$")


def _normalize_text(text: str) -> str:
    """Decode HTML entities, normalize unicode (NFKC), collapse whitespace."""
    text = html.unescape(text)
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _clean_title(title: str) -> str:
    """Normalize title and strip common site-name suffixes (| … / - … / – …)."""
    title = _normalize_text(title)
    title = _TITLE_SUFFIX_RE.sub("", title).strip()
    return title


# ---------------------------------------------------------------------------
# Rule-based field extraction
# ---------------------------------------------------------------------------


def _extract_field(text: str, field_re: re.Pattern[str]) -> str | None:
    """Extract the value following a labeled field (e.g. 'Location: Amsterdam')."""
    match = field_re.search(text)
    if match:
        value = match.group(1).strip()
        # Trim to first sentence or line
        value = re.split(r"\.\s+|\n|\r|;\s+", value)[0].strip()
        return value if value else None
    return None


def extract_fields_rule_based(
    page: ScrapedPage,
    location_re: re.Pattern[str],
    title_re: re.Pattern[str],
) -> dict[str, str | None]:
    """Extract title, description, and location_text using heuristics.

    Returns a dict with string-or-None values for each field.
    All extracted strings are normalised (HTML entities decoded, unicode
    normalised, whitespace collapsed).

    Args:
        page: Scraped page to extract from.
        location_re: Compiled regex built from patterns.location_keywords.
        title_re: Compiled regex built from patterns.title_keywords.
    """
    title: str | None = _clean_title(page.title) if page.title else None

    # If raw_text is unparsed HTML the scraper failed to extract properly —
    # return all None to force the LLM fallback on the full funnel step.
    if page.raw_text and _looks_like_html(page.raw_text):
        return {"title": title, "description": None, "location_text": None}

    # Fallback title: try labeled field first ("Titel: Open Call"), then first line
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
    """Parse JSON from LLM response, stripping markdown fences if present."""
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
    """Fill missing (None) fields in *base* from *override*."""
    return {k: base.get(k) or override.get(k) for k in base}


def _parse_llm_date(date_str: str | None) -> datetime | None:
    """Parse a YYYY-MM-DD string from LLM output."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str).replace(tzinfo=UTC)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# CleanEvent assembly
# ---------------------------------------------------------------------------


def _source_from_url(url: str) -> str:
    return urlparse(url).netloc or str(url)


def _country_from_url(url: str, target_countries: list[str]) -> str | None:
    """Infer ISO-3166-1 alpha-2 country code from URL TLD.

    Checks whether the URL's hostname ends with a ccTLD that matches one of the
    configured target_countries codes (e.g. "NL" → ".nl").
    """
    netloc = urlparse(url).netloc.lower()
    for code in target_countries:
        if netloc.endswith(f".{code.lower()}"):
            return code
    return None


def _looks_like_html(text: str) -> bool:
    """Return True when raw_text is unparsed HTML (scraper fallback artefact)."""
    stripped = text.lstrip()
    return stripped.startswith("<!DOCTYPE") or stripped.startswith("<html")


def _make_clean_event(
    page: ScrapedPage,
    language: str,
    date_start: datetime | None,
    date_end: datetime | None,
    fields: dict[str, str | None],
    status: ProcessingStatus,
    target_countries: list[str],
) -> CleanEvent:
    return CleanEvent(
        title=fields.get("title") or page.title or str(page.url),
        description=fields.get("description") or "",
        date_start=date_start,
        date_end=date_end,
        location_text=fields.get("location_text") or "",
        language=language,
        source=_source_from_url(str(page.url)),
        url=page.url,
        artifact_urls=page.artifact_urls,
        country=_country_from_url(str(page.url), target_countries),
        processing_status=status,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def clean_page(
    page: ScrapedPage,
    language: str,
    client: OpenAI,
    model: str,
    patterns: LanguagePatterns,
) -> CleanEvent:
    """Apply the extraction funnel to a single ScrapedPage and return a CleanEvent.

    Funnel:
      1. Parse dates rule-based.
      2. Return early with status='outdated' if event has passed.
      3. Extract fields rule-based.
      4. LLM fallback for incomplete fields; re-check outdated after LLM date fill.
      5. status='requires_manual_validation' if fields are still incomplete.

    Args:
        page: Scraped page to process.
        language: BCP-47 language code for this page (from search_results join).
        client: OpenAI-compatible client pointed at Databricks Foundation Models.
        model: Model ID for LLM fallback extraction.
        patterns: LanguagePatterns providing languages, target_countries, and
            location_keywords for rule-based extraction.
    """
    location_re = build_field_re(patterns.location_keywords)
    title_re = build_field_re(patterns.title_keywords)

    # Step 1 — rule-based date parsing
    date_start, date_end = parse_dates(page.raw_text or "", patterns.languages)

    # Step 2 — filter outdated early (skip expensive steps below)
    if is_outdated(date_start, date_end):
        logger.info("Outdated event (rule-based dates): {}", page.url)
        fields = extract_fields_rule_based(page, location_re, title_re)
        return _make_clean_event(
            page,
            language,
            date_start,
            date_end,
            fields,
            ProcessingStatus.OUTDATED,
            patterns.target_countries,
        )

    # Step 3 — rule-based field extraction
    fields = extract_fields_rule_based(page, location_re, title_re)

    # Step 4 — LLM fallback for missing fields
    if not _fields_complete(fields):
        logger.info("Falling back to LLM extraction for {}", page.url)
        llm_result = extract_fields_llm(page, client, model)
        if llm_result:
            fields = _merge_fields(fields, llm_result)
            # Fill dates from LLM if rule-based found none
            if date_start is None:
                date_start = _parse_llm_date(llm_result.get("date_start"))
            if date_end is None:
                date_end = _parse_llm_date(llm_result.get("date_end"))
            # Re-check outdated now that LLM may have provided dates
            if is_outdated(date_start, date_end):
                logger.info("Outdated event (LLM dates): {}", page.url)
                return _make_clean_event(
                    page,
                    language,
                    date_start,
                    date_end,
                    fields,
                    ProcessingStatus.OUTDATED,
                    patterns.target_countries,
                )

    # Step 5 — determine final status
    status = (
        ProcessingStatus.NEW
        if _fields_complete(fields)
        else ProcessingStatus.REQUIRES_MANUAL_VALIDATION
    )
    if status == ProcessingStatus.REQUIRES_MANUAL_VALIDATION:
        logger.warning("Incomplete extraction, manual validation needed: {}", page.url)

    return _make_clean_event(
        page, language, date_start, date_end, fields, status, patterns.target_countries
    )


# ---------------------------------------------------------------------------
# Spark integration (pragma: no cover — tested via integration marker)
# ---------------------------------------------------------------------------


def _build_openai_client() -> OpenAI:  # pragma: no cover
    """Create an OpenAI-compatible client backed by Databricks Foundation Models."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    token = w.tokens.create(lifetime_seconds=1200).token_value
    host = w.config.host or ""
    return OpenAI(
        api_key=token,
        base_url=f"{host.rstrip('/')}/serving-endpoints",
    )


def _clean_event_schema() -> StructType:  # pragma: no cover
    """Return the Spark StructType schema for CleanEvent rows."""
    from pyspark.sql.types import (
        ArrayType,
        FloatType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("title", StringType(), False),
            StructField("description", StringType(), False),
            StructField("date_start", TimestampType(), True),
            StructField("date_end", TimestampType(), True),
            StructField("location_text", StringType(), False),
            StructField("lat", FloatType(), True),
            StructField("lng", FloatType(), True),
            StructField("country", StringType(), True),
            StructField("language", StringType(), False),
            StructField("source", StringType(), False),
            StructField("url", StringType(), False),
            StructField("artifact_urls", ArrayType(StringType()), False),
            StructField("artifact_paths", ArrayType(StringType()), False),
            StructField("processing_status", StringType(), False),
            StructField("ingested_at", StringType(), False),
        ]
    )


def _write_clean_events(  # pragma: no cover
    spark: SparkSession,
    clean_rows: list[dict[str, object]],
    raw_events_table: str,
) -> None:
    """Write a list of CleanEvent dicts to the raw_events Delta table."""
    schema = _clean_event_schema()
    df = spark.createDataFrame(clean_rows, schema=schema)

    parts = raw_events_table.split(".")
    if len(parts) == 3:
        catalog, db, _ = parts
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db}")

    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(raw_events_table)
    )


def run_list(  # pragma: no cover
    scraped_pages_table: str,
    patterns_path: Path,
    limit: int = 0,
) -> list[str]:
    """Read new scraped pages and emit their URLs as a Databricks task value.

    Returns the list of URLs. Also sets the task value ``urls`` so that a
    downstream ``for_each_task`` can iterate over them in parallel.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

    _ = load_patterns(patterns_path)  # validate patterns file is readable

    pages_df = spark.table(scraped_pages_table).filter(
        F.col("processing_status") == ProcessingStatus.NEW
    )

    if limit > 0:
        pages_df = pages_df.limit(limit)

    urls: list[str] = [row["url"] for row in pages_df.select("url").collect()]
    logger.info("New scraped pages to clean: {}", len(urls))

    try:
        from databricks.sdk.runtime import dbutils

        dbutils.jobs.taskValues.set(key="urls", value=urls)
        logger.info("Task value 'urls' set with {} entries", len(urls))
    except ImportError:
        logger.warning("dbutils not available — skipping task value set")

    return urls


def run_clean_one(  # pragma: no cover
    url: str,
    scraped_pages_table: str,
    search_results_table: str,
    raw_events_table: str,
    patterns_path: Path,
    *,
    model: str,
    client: OpenAI | None = None,
    env: str = "dev",
) -> None:
    """Clean a single scraped page identified by *url* and append to raw_events.

    Designed for use as the inner task of a Databricks ``for_each_task``.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

    patterns = load_patterns(patterns_path)

    pages_df = spark.table(scraped_pages_table).filter(F.col("url") == url)

    # Join for language
    if spark.catalog.tableExists(search_results_table):
        lang_df = spark.table(search_results_table).select("url", "language")
        pages_df = pages_df.join(lang_df, on="url", how="left")
    else:
        pages_df = pages_df.withColumn("language", F.lit("unknown"))

    pages_df = pages_df.fillna({"language": "unknown"})

    rows = pages_df.collect()
    if not rows:
        logger.warning("No scraped page found for URL: {}", url)
        return

    if client is None:
        client = _build_openai_client()

    clean_rows = []
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
        language = row["language"] or "unknown"
        event = clean_page(page, language, client, model, patterns)
        r = event.model_dump(mode="python")
        r["url"] = str(r["url"])
        r["ingested_at"] = str(r["ingested_at"])
        clean_rows.append(r)

    _write_clean_events(spark, clean_rows, raw_events_table)
    logger.info("Wrote CleanEvent for {} to {}", url, raw_events_table)


def run_clean(  # pragma: no cover
    scraped_pages_table: str,
    search_results_table: str,
    raw_events_table: str,
    patterns_path: Path,
    *,
    model: str,
    client: OpenAI | None = None,
    env: str = "dev",
) -> int:
    """Read new ScrapedPage rows, clean them, and write CleanEvent rows to Delta.

    Args:
        scraped_pages_table: Fully-qualified Delta table for scraped pages.
        search_results_table: Fully-qualified Delta table for search results
            (used to look up language per URL).
        raw_events_table: Fully-qualified Delta table to write CleanEvent rows to.
        patterns_path: Path to the language_patterns YAML file.
        model: Databricks Foundation Model ID for LLM fallback extraction.
        client: Pre-built OpenAI-compatible client (created from workspace token if None).
        env: Deployment environment tag (dev/tst/acc/prd).

    Returns the number of events written.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

    patterns = load_patterns(patterns_path)

    pages_df = spark.table(scraped_pages_table).filter(
        F.col("processing_status") == ProcessingStatus.NEW
    )

    # Join with search_results to get language; default to 'unknown' if missing
    if spark.catalog.tableExists(search_results_table):
        lang_df = spark.table(search_results_table).select("url", "language")
        pages_df = pages_df.join(lang_df, on="url", how="left")
    else:
        pages_df = pages_df.withColumn("language", F.lit("unknown"))

    pages_df = pages_df.fillna({"language": "unknown"})

    rows = pages_df.collect()
    logger.info("Cleaning {} new scraped pages", len(rows))

    if client is None:
        client = _build_openai_client()

    clean_rows = []
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
        language = row["language"] or "unknown"
        event = clean_page(page, language, client, model, patterns)
        # mode="python" preserves datetime objects — required for TimestampType columns
        r = event.model_dump(mode="python")
        r["url"] = str(r["url"])
        r["ingested_at"] = str(r["ingested_at"])
        clean_rows.append(r)

    if not clean_rows:
        logger.info("No new pages to clean")
        return 0

    _write_clean_events(spark, clean_rows, raw_events_table)
    logger.info("Wrote {} CleanEvent rows to {}", len(clean_rows), raw_events_table)
    return len(clean_rows)


def main() -> None:  # pragma: no cover
    """Entry point for artlake-clean-events wheel task.

    Two modes:
      list  — Read new scraped pages and emit their URLs as a Databricks task value
              so a downstream for_each_task can iterate over them in parallel.
      clean — Process a single URL (used as the for_each inner task).
    """
    import argparse

    parser = argparse.ArgumentParser(description="ArtLake event cleaner")
    parser.add_argument(
        "--mode",
        choices=["list", "clean"],
        required=True,
        help="'list' emits new page URLs as a task value; 'clean' processes one URL",
    )
    parser.add_argument("--url", help="URL to clean (required for --mode clean)")
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
        "--raw-events-table",
        default="artlake.bronze.raw_events",
        help="Fully-qualified raw_events Delta table (required for --mode clean)",
    )
    parser.add_argument(
        "--model",
        default="databricks-meta-llama-3-3-70b-instruct",
        help="Databricks Foundation Model for LLM fallback extraction",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max URLs to emit in list mode (0 = no limit)",
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

    if args.mode == "list":
        run_list(
            scraped_pages_table=args.scraped_pages_table,
            patterns_path=args.language_patterns,
            limit=args.limit,
        )
    else:
        if not args.url:
            parser.error("--url is required for --mode clean")
        run_clean_one(
            url=args.url,
            scraped_pages_table=args.scraped_pages_table,
            search_results_table=args.search_results_table,
            raw_events_table=args.raw_events_table,
            patterns_path=args.language_patterns,
            model=args.model,
            env=args.env,
        )

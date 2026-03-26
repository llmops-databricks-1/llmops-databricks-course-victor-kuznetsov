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
from urllib.parse import urlparse

import backoff
from loguru import logger
from openai import OpenAI

from artlake.models.event import CleanEvent, ProcessingStatus, ScrapedPage

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DATE_SEARCH_CHARS = 3000  # chars fed to dateparser.search
_TEXT_TRUNCATE = 3000  # chars sent to LLM
_DATE_HORIZON_DAYS = 730  # filter out dates > 2 years old (publication/copyright dates)
_LANGUAGES = ["en", "nl", "de", "fr"]

_LOCATION_RE = re.compile(
    r"(?:^|\b)(?:Location|Venue|Address|Place|Locatie|Adresse|Adres|Ort|Lieu|Endroit)"
    r"\s*:\s*(.+)",
    re.IGNORECASE | re.MULTILINE,
)

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


def parse_dates(text: str) -> tuple[datetime | None, datetime | None]:
    """Extract start and end dates from raw text using dateparser.

    Returns (date_start, date_end). Both may be None if no dates are found.
    Dates more than two years in the past are discarded (copyright / publication noise).
    """
    from dateparser.search import search_dates  # type: ignore[import-untyped]

    snippet = text[:_DATE_SEARCH_CHARS]
    results = search_dates(
        snippet,
        languages=_LANGUAGES,
        settings={"PREFER_DAY_OF_MONTH": "first", "RETURN_AS_TIMEZONE_AWARE": True},
    )
    if not results:
        return None, None

    cutoff = datetime.now(UTC) - timedelta(days=_DATE_HORIZON_DAYS)
    seen: set[tuple[int, int, int]] = set()
    dates: list[datetime] = []
    for _, dt in results:
        key = (dt.year, dt.month, dt.day)
        if key not in seen and dt >= cutoff:
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


def _extract_location(text: str) -> str | None:
    match = _LOCATION_RE.search(text)
    if match:
        value = match.group(1).strip()
        # Trim to first sentence or line
        value = re.split(r"\.\s+|\n|\r|;\s+", value)[0].strip()
        return value if value else None
    return None


def extract_fields_rule_based(page: ScrapedPage) -> dict[str, str | None]:
    """Extract title, description, and location_text using heuristics.

    Returns a dict with string-or-None values for each field.
    All extracted strings are normalised (HTML entities decoded, unicode
    normalised, whitespace collapsed).
    """
    title: str | None = _clean_title(page.title) if page.title else None

    # Fallback: first meaningful line of raw_text
    if not title and page.raw_text:
        for line in page.raw_text.splitlines():
            stripped = _normalize_text(line)
            if len(stripped) > 10:
                title = _clean_title(stripped[:200])
                break

    description: str | None = None
    if page.raw_text:
        raw = _normalize_text(page.raw_text[:500])
        description = raw or None

    location_text: str | None = None
    if page.raw_text:
        raw_loc = _extract_location(page.raw_text)
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


def _make_clean_event(
    page: ScrapedPage,
    language: str,
    date_start: datetime | None,
    date_end: datetime | None,
    fields: dict[str, str | None],
    status: ProcessingStatus,
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
) -> CleanEvent:
    """Apply the extraction funnel to a single ScrapedPage and return a CleanEvent.

    Funnel:
      1. Parse dates rule-based.
      2. Return early with status='outdated' if event has passed.
      3. Extract fields rule-based.
      4. LLM fallback for incomplete fields; re-check outdated after LLM date fill.
      5. status='requires_manual_validation' if fields are still incomplete.
    """
    # Step 1 — rule-based date parsing
    date_start, date_end = parse_dates(page.raw_text or "")

    # Step 2 — filter outdated early (skip expensive steps below)
    if is_outdated(date_start, date_end):
        logger.info("Outdated event (rule-based dates): {}", page.url)
        fields = extract_fields_rule_based(page)
        return _make_clean_event(
            page, language, date_start, date_end, fields, ProcessingStatus.OUTDATED
        )

    # Step 3 — rule-based field extraction
    fields = extract_fields_rule_based(page)

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
                )

    # Step 5 — determine final status
    status = (
        ProcessingStatus.NEW
        if _fields_complete(fields)
        else ProcessingStatus.REQUIRES_MANUAL_VALIDATION
    )
    if status == ProcessingStatus.REQUIRES_MANUAL_VALIDATION:
        logger.warning("Incomplete extraction, manual validation needed: {}", page.url)

    return _make_clean_event(page, language, date_start, date_end, fields, status)


# ---------------------------------------------------------------------------
# Spark integration (pragma: no cover — tested via integration marker)
# ---------------------------------------------------------------------------


def run_clean(  # pragma: no cover
    scraped_pages_table: str,
    search_results_table: str,
    raw_events_table: str,
    *,
    model: str,
    client: OpenAI | None = None,
    env: str = "dev",
) -> int:
    """Read new ScrapedPage rows, clean them, and write CleanEvent rows to Delta.

    Returns the number of events written.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

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
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        token = w.tokens.create(lifetime_seconds=1200).token_value
        host = w.config.host or ""
        client = OpenAI(
            api_key=token,
            base_url=f"{host.rstrip('/')}/serving-endpoints",
        )

    from pyspark.sql.types import (
        ArrayType,
        FloatType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
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
        event = clean_page(page, language, client, model)
        # mode="python" preserves datetime objects — required for TimestampType columns
        r = event.model_dump(mode="python")
        r["url"] = str(r["url"])
        r["ingested_at"] = str(r["ingested_at"])
        clean_rows.append(r)

    if not clean_rows:
        logger.info("No new pages to clean")
        return 0

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
    logger.info("Wrote {} CleanEvent rows to {}", len(clean_rows), raw_events_table)
    return len(clean_rows)


def main() -> None:  # pragma: no cover
    """Entry point for artlake-clean-events wheel task."""
    import argparse

    parser = argparse.ArgumentParser(description="ArtLake event cleaner")
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
        help="Fully-qualified raw_events Delta table",
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
    args = parser.parse_args()

    run_clean(
        scraped_pages_table=args.scraped_pages_table,
        search_results_table=args.search_results_table,
        raw_events_table=args.raw_events_table,
        model=args.model,
        env=args.env,
    )

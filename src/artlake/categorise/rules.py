"""Rule-based event categorisation (artlake-categorise-rules entry point).

Reads CleanEvent records from raw_events where category IS NULL, classifies
each event via multilingual keyword matching, and updates the category column
via MERGE INTO.

Output categories:
  open_call   — clearly an open call / call for submissions
  exhibition  — gallery show, vernissage, or display
  workshop    — class, residency, masterclass, atelier
  market      — art fair, craft market, artisan market
  non_art     — clearly irrelevant (no art content detected)
  uncertain   — no rule matched; passed to artlake-categorise-llm

Art categories are matched first (higher precision). Non-art is a last-resort
filter for clearly irrelevant content. No match → "uncertain".
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ART_CATEGORIES = ("open_call", "exhibition", "workshop", "market")
_NON_ART = "non_art"
_UNCERTAIN = "uncertain"

# ---------------------------------------------------------------------------
# Pure functions (fully testable without Spark)
# ---------------------------------------------------------------------------


def load_category_keywords(path: Path) -> dict[str, dict[str, list[str]]]:
    """Load per-category keyword dictionaries from a YAML file.

    Expected top-level key: ``category_keywords``.
    Returns an empty dict if the key is absent.
    """
    with path.open() as fh:
        data: dict[str, Any] = yaml.safe_load(fh)
    result: dict[str, dict[str, list[str]]] = data.get("category_keywords", {})
    return result


def classify_text(
    title: str | None,
    description: str | None,
    category_keywords: dict[str, dict[str, list[str]]],
) -> str:
    """Return the best-matching category for an event.

    Matching strategy:
    1. Check specific art categories (open_call, exhibition, workshop, market)
       in priority order — first match wins.
    2. If none match, check non_art keywords as a last-resort filter.
    3. Default to "uncertain" (passed to LLM categoriser).

    Args:
        title: Event title (may be None).
        description: Event description (may be None).
        category_keywords: Mapping of category → language → keyword list,
            as returned by :func:`load_category_keywords`.

    Returns:
        One of: ``open_call``, ``exhibition``, ``workshop``, ``market``,
        ``non_art``, or ``uncertain``.
    """
    text = " ".join(filter(None, [title or "", description or ""])).lower()

    if not text.strip():
        return _UNCERTAIN

    # Art categories — checked first so art trumps non-art signals
    for category in _ART_CATEGORIES:
        for lang_kws in category_keywords.get(category, {}).values():
            if any(kw.lower() in text for kw in lang_kws):
                return category

    # Non-art last-resort filter
    for lang_kws in category_keywords.get(_NON_ART, {}).values():
        if any(kw.lower() in text for kw in lang_kws):
            return _NON_ART

    return _UNCERTAIN


# ---------------------------------------------------------------------------
# Spark integration (pragma: no cover — tested via integration marker)
# ---------------------------------------------------------------------------


def run_categorise(  # pragma: no cover
    raw_events_table: str,
    category_keywords_path: Path,
) -> int:
    """Classify uncategorised events in raw_events and update via MERGE INTO.

    Reads rows where ``category IS NULL``, classifies each via keyword
    matching, and merges the result back on ``fingerprint``.

    Args:
        raw_events_table: Fully-qualified Delta table (e.g. artlake.bronze.raw_events).
        category_keywords_path: Path to ``category_keywords.yml``.

    Returns:
        Number of events classified.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    spark = SparkSession.builder.getOrCreate()
    category_keywords = load_category_keywords(category_keywords_path)

    # Ensure category column exists (may be absent on tables written before
    # this module was introduced)
    existing_cols = set(spark.table(raw_events_table).columns)
    if "category" not in existing_cols:
        logger.info("category column missing — adding to {}", raw_events_table)
        spark.sql(f"ALTER TABLE {raw_events_table} ADD COLUMN category STRING")

    events_df = spark.table(raw_events_table).filter(F.col("category").isNull())
    rows = events_df.select("fingerprint", "title", "description").collect()

    if not rows:
        logger.info("No uncategorised events in {}", raw_events_table)
        return 0

    logger.info("Classifying {} events from {}", len(rows), raw_events_table)

    # Deduplicate by fingerprint — raw_events may contain duplicate URLs
    # (same page scraped from different search queries)
    seen: set[str] = set()
    updates: list[tuple[str, str]] = []
    for row in rows:
        fp = row["fingerprint"]
        if fp not in seen:
            seen.add(fp)
            updates.append(
                (fp, classify_text(row["title"], row["description"], category_keywords))
            )

    dist = Counter(cat for _, cat in updates)
    for cat, n in sorted(dist.items()):
        logger.info("  {} → {}", cat, n)

    update_schema = StructType(
        [
            StructField("fingerprint", StringType(), False),
            StructField("category", StringType(), False),
        ]
    )
    update_df = spark.createDataFrame(updates, schema=update_schema)
    update_df.createOrReplaceTempView("_categorise_rules_updates")

    spark.sql(f"""
        MERGE INTO {raw_events_table} AS target
        USING _categorise_rules_updates AS src
        ON target.fingerprint = src.fingerprint
        WHEN MATCHED THEN UPDATE SET target.category = src.category
    """)

    logger.info("Categorised {} events", len(updates))
    return len(updates)


def main() -> None:  # pragma: no cover
    """Entry point for artlake-categorise-rules wheel task."""
    import argparse

    parser = argparse.ArgumentParser(
        description="ArtLake rule-based event categorisation"
    )
    parser.add_argument(
        "--raw-events-table",
        default="artlake.bronze.raw_events",
        help="Fully-qualified raw_events Delta table",
    )
    parser.add_argument(
        "--category-keywords",
        required=True,
        type=Path,
        help="Path to category_keywords.yml",
    )
    args = parser.parse_args()
    run_categorise(
        raw_events_table=args.raw_events_table,
        category_keywords_path=args.category_keywords,
    )

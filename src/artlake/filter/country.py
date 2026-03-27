"""Geocoding + country filter (artlake-geocode entry point).

Reads CleanEvent records from raw_events where processing_status='new',
geocodes location_text via Nominatim (geopy), filters by target_countries,
and updates processing_status:
  - 'done'   → geocoded country is in target_countries
  - 'failed' → unresolvable location (country set to 'unknown') OR resolved
               country not in target_countries

lat/lng are stored on all resolved events for future Phase 3 BI radius filtering.

Three country signals available downstream:
  query_country  — country from the original search query (from search_results)
  domain_country — country inferred from URL TLD (set by clean-events step)
  country        — geocoded country from Nominatim (set by this step)
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from loguru import logger

from artlake.models.event import CleanEvent, ProcessingStatus

if TYPE_CHECKING:
    pass

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

GeocodeFn = Callable[[str], Any]
GeoResult = tuple[float | None, float | None, str | None]

# ---------------------------------------------------------------------------
# Pure geocoding helpers (fully testable without Spark or network)
# ---------------------------------------------------------------------------


def geocode_location(
    location_text: str,
    geocode_fn: GeocodeFn,
    cache: dict[str, GeoResult],
) -> GeoResult:
    """Resolve location_text to (lat, lng, country_code).

    Checks the cache first, then calls geocode_fn. Caches every result
    (including failures) so identical strings are resolved only once.

    Args:
        location_text: Free-text location from CleanEvent.
        geocode_fn: Callable that takes a query string and returns a geopy
            Location object or None. Should be a RateLimiter-wrapped
            Nominatim.geocode for production use.
        cache: In-memory dict keyed by location_text; mutated in place.

    Returns:
        (lat, lng, ISO-3166-1 alpha-2 country code uppercased) or
        (None, None, None) when the location cannot be resolved.
    """
    if not location_text:
        return (None, None, None)

    if location_text in cache:
        return cache[location_text]

    try:
        location = geocode_fn(location_text)
    except Exception:
        logger.warning("Geocoding error for: '{}'", location_text)
        cache[location_text] = (None, None, None)
        return (None, None, None)

    if location is None:
        cache[location_text] = (None, None, None)
        return (None, None, None)

    raw_cc: str = location.raw.get("address", {}).get("country_code", "")
    country_code = raw_cc.upper() if raw_cc else None
    result: GeoResult = (
        float(location.latitude),
        float(location.longitude),
        country_code,
    )
    cache[location_text] = result
    return result


def apply_geocoding(
    events: list[CleanEvent],
    target_countries: list[str],
    geocode_fn: GeocodeFn,
) -> list[CleanEvent]:
    """Geocode events and apply country filter.

    All events are returned with updated lat, lng, country, and
    processing_status. An in-memory cache ensures identical location_text
    strings are geocoded only once per call (respects Nominatim 1 req/s rate
    limit when combined with RateLimiter).

    Status rules:
      - country in target_countries → processing_status='done'
      - unresolvable (country=None) → country='unknown', status='failed'
      - resolved but country not in target_countries → status='failed'

    Args:
        events: CleanEvent records to geocode (all should have status='new').
        target_countries: ISO-3166-1 alpha-2 codes to accept (e.g. ["NL", "BE"]).
        geocode_fn: Rate-limited geocoding callable.

    Returns:
        Updated events list (same length as input, order preserved).
    """
    cache: dict[str, GeoResult] = {}
    target_set = {c.upper() for c in target_countries}
    updated: list[CleanEvent] = []

    for event in events:
        lat, lng, country_code = geocode_location(event.location_text, geocode_fn, cache)

        if country_code is None:
            updated.append(
                event.model_copy(
                    update={
                        "lat": lat,
                        "lng": lng,
                        "country": "unknown",
                        "processing_status": ProcessingStatus.FAILED,
                    }
                )
            )
            logger.warning(
                "Unresolvable location for {} ('{}')",
                event.fingerprint,
                event.location_text,
            )
        elif country_code in target_set:
            updated.append(
                event.model_copy(
                    update={
                        "lat": lat,
                        "lng": lng,
                        "country": country_code,
                        "processing_status": ProcessingStatus.DONE,
                    }
                )
            )
            logger.info("Accepted event {} (country={})", event.fingerprint, country_code)
        else:
            updated.append(
                event.model_copy(
                    update={
                        "lat": lat,
                        "lng": lng,
                        "country": country_code,
                        "processing_status": ProcessingStatus.FAILED,
                    }
                )
            )
            logger.info(
                "Filtered event {} (country={} not in {})",
                event.fingerprint,
                country_code,
                target_countries,
            )

    return updated


# ---------------------------------------------------------------------------
# Spark integration (pragma: no cover — tested via integration marker)
# ---------------------------------------------------------------------------


def _build_geocode_fn(env: str) -> GeocodeFn:  # pragma: no cover
    """Build a rate-limited Nominatim geocode callable."""
    from geopy.extra.rate_limiter import RateLimiter
    from geopy.geocoders import Nominatim

    geolocator = Nominatim(user_agent=f"artlake-{env}")
    return RateLimiter(geolocator.geocode, min_delay_seconds=1)  # type: ignore[no-any-return]


def run_geocode(  # pragma: no cover
    raw_events_table: str,
    target_countries: list[str],
    env: str = "dev",
) -> int:
    """Geocode new CleanEvent rows in raw_events and update via MERGE INTO.

    Reads rows where processing_status='new', geocodes location_text via
    Nominatim (1 req/s rate-limited), applies country filter, and updates
    lat/lng/country/processing_status in the Delta table on fingerprint.

    Args:
        raw_events_table: Fully-qualified raw_events Delta table.
        target_countries: ISO-3166-1 alpha-2 codes to accept (e.g. ["NL", "BE"]).
        env: Deployment environment tag used as Nominatim user_agent suffix.

    Returns:
        Number of events accepted (processing_status set to 'done').
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import FloatType, StringType, StructField, StructType

    spark = SparkSession.builder.getOrCreate()

    # Ensure fingerprint exists in the physical table (pre-dates issue #9 tables)
    existing_cols = set(spark.table(raw_events_table).columns)
    if "fingerprint" not in existing_cols:
        logger.info("fingerprint column missing — adding and backfilling from url")
        spark.sql(f"ALTER TABLE {raw_events_table} ADD COLUMN fingerprint STRING")
        spark.sql(
            f"UPDATE {raw_events_table} SET fingerprint = sha2(url, 256)"
            f" WHERE fingerprint IS NULL"
        )
        existing_cols.add("fingerprint")

    # Ensure new country signal columns exist (added in issue #14)
    for col_name in ("query_country", "domain_country"):
        if col_name not in existing_cols:
            spark.sql(f"ALTER TABLE {raw_events_table} ADD COLUMN {col_name} STRING")
            existing_cols.add(col_name)

    table_df = spark.table(raw_events_table)

    events_df = table_df.filter(F.col("processing_status") == ProcessingStatus.NEW)
    rows = events_df.collect()

    if not rows:
        logger.info("No new events to geocode")
        return 0

    logger.info("Geocoding {} new events", len(rows))

    geocode_fn = _build_geocode_fn(env)

    events = [
        CleanEvent(
            fingerprint=row["fingerprint"],
            title=row["title"],
            description=row["description"],
            date_start=row["date_start"],
            date_end=row["date_end"],
            location_text=row["location_text"] or "",
            lat=row["lat"],
            lng=row["lng"],
            query_country=row["query_country"],
            domain_country=row["domain_country"],
            country=row["country"],
            language=row["language"],
            source=row["source"],
            url=row["url"],
            artifact_urls=list(row["artifact_urls"] or []),
            artifact_paths=list(row["artifact_paths"] or []),
            processing_status=ProcessingStatus(row["processing_status"]),
        )
        for row in rows
    ]

    updated = apply_geocoding(events, target_countries, geocode_fn)

    update_schema = StructType(
        [
            StructField("fingerprint", StringType(), False),
            StructField("lat", FloatType(), True),
            StructField("lng", FloatType(), True),
            StructField("country", StringType(), True),
            StructField("processing_status", StringType(), False),
        ]
    )
    # Deduplicate by fingerprint — raw_events may have duplicate URLs
    seen: set[str] = set()
    update_rows = []
    for e in updated:
        if e.fingerprint not in seen:
            seen.add(e.fingerprint)
            update_rows.append(
                (e.fingerprint, e.lat, e.lng, e.country, str(e.processing_status))
            )
    update_df = spark.createDataFrame(update_rows, schema=update_schema)
    update_df.createOrReplaceTempView("_geocode_updates")

    spark.sql(f"""
        MERGE INTO {raw_events_table} AS target
        USING _geocode_updates AS src
        ON target.fingerprint = src.fingerprint
        WHEN MATCHED THEN UPDATE SET
            target.lat = src.lat,
            target.lng = src.lng,
            target.country = src.country,
            target.processing_status = src.processing_status
    """)

    accepted = sum(1 for e in updated if e.processing_status == ProcessingStatus.DONE)
    logger.info(
        "Geocoding complete: {} accepted, {} failed/filtered",
        accepted,
        len(updated) - accepted,
    )
    return accepted


def main() -> None:  # pragma: no cover
    """Entry point for artlake-geocode wheel task."""
    import argparse

    parser = argparse.ArgumentParser(description="ArtLake geocoder + country filter")
    parser.add_argument(
        "--raw-events-table",
        default="artlake.bronze.raw_events",
        help="Fully-qualified raw_events Delta table",
    )
    parser.add_argument(
        "--target-countries",
        nargs="+",
        default=["NL", "BE", "DE", "FR"],
        help="ISO-3166-1 alpha-2 country codes to accept (e.g. NL BE DE FR)",
    )
    parser.add_argument(
        "--env",
        default="dev",
        help="Deployment environment (dev/tst/acc/prd)",
    )
    args = parser.parse_args()
    run_geocode(
        raw_events_table=args.raw_events_table,
        target_countries=args.target_countries,
        env=args.env,
    )

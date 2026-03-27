"""Geocoding + country filter (artlake-geocode entry point).

Reads CleanEvent records from raw_events where processing_status='new',
geocodes location_text via Nominatim (geopy) with an LLM fallback for
strings that Nominatim cannot resolve, filters by target_countries,
and updates processing_status:
  - 'done'   → resolved country is in target_countries
  - 'failed' → unresolvable location (country set to 'unknown') OR resolved
               country not in target_countries

lat/lng are stored only when Nominatim resolves; LLM fallback provides the
country code without coordinates (lat/lng remain None).

Three country signals available downstream:
  query_country  — country from the original search query (from search_results)
  domain_country — country inferred from URL TLD (set by clean-events step)
  country        — geocoded/LLM-resolved country (set by this step)
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from loguru import logger
from openai import OpenAI

from artlake.models.event import CleanEvent, ProcessingStatus

if TYPE_CHECKING:
    pass

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

GeocodeFn = Callable[[str], Any]
GeoResult = tuple[float | None, float | None, str | None]
LLMAddressFn = Callable[[str], str | None]
LLMCountryFn = Callable[[str], str | None]

# ---------------------------------------------------------------------------
# LLM prompts
# ---------------------------------------------------------------------------

_ADDRESS_SYSTEM = (
    "You are a location normalizer. "
    "Given a messy location string, extract the most concise geocodable address. "
    "Return ONLY the normalized address suitable for a geocoder "
    "(e.g. 'Feldafing, Bavaria, Germany' or 'Ixelles, Brussels, Belgium'). "
    "If no specific location can be determined, return NONE. "
    "No explanation — just the address or NONE."
)

_ADDRESS_PROMPT = "Location: {text}"

_COUNTRY_SYSTEM = (
    "You are a location-to-country resolver. "
    "Given a location string, return ONLY the ISO 3166-1 alpha-2 country code "
    "(e.g. DE, BE, NL, FR). "
    "If the country cannot be determined, return UNKNOWN. "
    "No explanation, no punctuation — just the 2-letter code or UNKNOWN."
)

_COUNTRY_PROMPT = "Location: {text}"

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


def llm_extract_address(
    location_text: str,
    llm_fn: LLMAddressFn,
    cache: dict[str, str | None],
) -> str | None:
    """Use an LLM to normalize messy location text into a geocodable address.

    Called when Nominatim fails on the raw location_text. The returned
    string is intended to be passed back to Nominatim for a second attempt.
    Results are cached so identical strings trigger only one LLM call.

    Args:
        location_text: Free-text location that Nominatim failed to resolve.
        llm_fn: Callable that sends location_text to an LLM and returns the
            raw text response (or None on failure).
        cache: In-memory dict keyed by location_text; mutated in place.

    Returns:
        A clean geocodable address string, or None when no specific location
        can be extracted (LLM returns "NONE" or unrecognizable output).
    """
    if not location_text:
        return None

    if location_text in cache:
        return cache[location_text]

    try:
        raw = llm_fn(location_text)
    except Exception:
        logger.warning("LLM address extraction error for: '{}'", location_text[:80])
        cache[location_text] = None
        return None

    if not raw:
        cache[location_text] = None
        return None

    cleaned = raw.strip()
    if cleaned.upper() == "NONE" or not cleaned:
        cache[location_text] = None
        return None

    cache[location_text] = cleaned
    return cleaned


def llm_resolve_country(
    location_text: str,
    llm_fn: LLMCountryFn,
    cache: dict[str, str | None],
) -> str | None:
    """Use an LLM to extract the ISO-3166-1 alpha-2 country code from location_text.

    Called as a fallback when Nominatim cannot geocode the location. Does not
    provide lat/lng — only the country code. Results are cached so identical
    strings trigger only one LLM call.

    Args:
        location_text: Free-text location that Nominatim failed to resolve.
        llm_fn: Callable that sends location_text to an LLM and returns the
            raw text response (or None on failure).
        cache: In-memory dict keyed by location_text; mutated in place.

    Returns:
        Uppercased 2-letter country code, or None when the LLM cannot
        determine the country (returns "UNKNOWN" or invalid output).
    """
    if not location_text:
        return None

    if location_text in cache:
        return cache[location_text]

    try:
        raw = llm_fn(location_text)
    except Exception:
        logger.warning("LLM country resolution error for: '{}'", location_text[:80])
        cache[location_text] = None
        return None

    if not raw:
        cache[location_text] = None
        return None

    cleaned = raw.strip().upper()
    if cleaned == "UNKNOWN" or len(cleaned) < 2 or not cleaned[:2].isalpha():
        cache[location_text] = None
        return None

    code = cleaned[:2]
    cache[location_text] = code
    return code


def apply_geocoding(
    events: list[CleanEvent],
    target_countries: list[str],
    geocode_fn: GeocodeFn,
    llm_address_fn: LLMAddressFn | None = None,
    llm_country_fn: LLMCountryFn | None = None,
) -> list[CleanEvent]:
    """Geocode events and apply country filter.

    Resolution happens in three stages, each used only when the previous fails:

    1. Nominatim on raw location_text → lat/lng + country
    2. LLM extracts a clean address → Nominatim on that address → lat/lng + country
    3. LLM returns ISO-3166-1 alpha-2 country code directly (no coordinates)

    All events are returned with updated lat, lng, country, and
    processing_status. In-memory caches ensure identical location strings
    are processed only once per call.

    Status rules:
      - country in target_countries → processing_status='done'
      - unresolvable (country=None) → country='unknown', status='failed'
      - resolved but country not in target_countries → status='failed'

    Args:
        events: CleanEvent records to geocode (all should have status='new').
        target_countries: ISO-3166-1 alpha-2 codes to accept (e.g. ["NL", "BE"]).
        geocode_fn: Rate-limited geocoding callable.
        llm_address_fn: Optional LLM callable to normalize messy location text
            into a clean geocodable address for a second Nominatim attempt.
        llm_country_fn: Optional LLM callable of last resort — returns country
            code when Nominatim still fails after address normalization.

    Returns:
        Updated events list (same length as input, order preserved).
    """
    geocode_cache: dict[str, GeoResult] = {}
    address_cache: dict[str, str | None] = {}
    country_cache: dict[str, str | None] = {}
    target_set = {c.upper() for c in target_countries}
    updated: list[CleanEvent] = []

    for event in events:
        lat, lng, country_code = geocode_location(
            event.location_text, geocode_fn, geocode_cache
        )

        # Stage 2: LLM normalizes address → second Nominatim attempt
        if country_code is None and llm_address_fn is not None:
            clean_address = llm_extract_address(
                event.location_text, llm_address_fn, address_cache
            )
            if clean_address is not None:
                lat, lng, country_code = geocode_location(
                    clean_address, geocode_fn, geocode_cache
                )
                if country_code is not None:
                    logger.info(
                        "LLM+Nominatim resolved ({}, {}) country='{}'"
                        " for event {} via '{}'",
                        round(lat, 4) if lat else None,
                        round(lng, 4) if lng else None,
                        country_code,
                        event.fingerprint,
                        clean_address,
                    )

        # Stage 3: LLM country-only fallback (no coordinates)
        if country_code is None and llm_country_fn is not None:
            country_code = llm_resolve_country(
                event.location_text, llm_country_fn, country_cache
            )
            if country_code is not None:
                logger.info(
                    "LLM resolved country '{}' (no coordinates) for event {} ('{}')",
                    country_code,
                    event.fingerprint,
                    event.location_text[:60],
                )

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


def _build_llm_address_fn(client: OpenAI, model: str) -> LLMAddressFn:  # pragma: no cover
    """Return an LLM-backed address normalization callable.

    The returned function sends location_text to the LLM and returns
    its raw text response for parsing by llm_extract_address.
    """

    def extract(location_text: str) -> str | None:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": _ADDRESS_SYSTEM},
                {
                    "role": "user",
                    "content": _ADDRESS_PROMPT.format(text=location_text[:500]),
                },
            ],
            temperature=0.0,
            max_tokens=64,
        )
        return response.choices[0].message.content

    return extract


def _build_llm_country_fn(client: OpenAI, model: str) -> LLMCountryFn:  # pragma: no cover
    """Return an LLM-backed country resolution callable.

    The returned function sends location_text to the LLM and returns
    its raw text response for parsing by llm_resolve_country.
    """

    def resolve(location_text: str) -> str | None:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": _COUNTRY_SYSTEM},
                {
                    "role": "user",
                    "content": _COUNTRY_PROMPT.format(text=location_text[:500]),
                },
            ],
            temperature=0.0,
            max_tokens=10,
        )
        return response.choices[0].message.content

    return resolve


def run_geocode(  # pragma: no cover
    raw_events_table: str,
    target_countries: list[str],
    env: str = "dev",
    model: str = "databricks-meta-llama-3-3-70b-instruct",
) -> int:
    """Geocode new CleanEvent rows in raw_events and update via MERGE INTO.

    Reads rows where processing_status='new', geocodes location_text via
    Nominatim (1 req/s rate-limited) with LLM fallback for unresolvable
    strings, applies country filter, and updates lat/lng/country/
    processing_status in the Delta table on fingerprint.

    Args:
        raw_events_table: Fully-qualified raw_events Delta table.
        target_countries: ISO-3166-1 alpha-2 codes to accept (e.g. ["NL", "BE"]).
        env: Deployment environment tag used as Nominatim user_agent suffix.
        model: Databricks Foundation Model ID used for LLM country fallback.

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

    from artlake.clean.events import _build_openai_client

    llm_client = _build_openai_client()
    llm_address_fn = _build_llm_address_fn(llm_client, model)
    llm_country_fn = _build_llm_country_fn(llm_client, model)

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

    updated = apply_geocoding(
        events, target_countries, geocode_fn, llm_address_fn, llm_country_fn
    )

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
    parser.add_argument(
        "--model",
        default="databricks-meta-llama-3-3-70b-instruct",
        help="Databricks Foundation Model ID for LLM country fallback",
    )
    args = parser.parse_args()
    run_geocode(
        raw_events_table=args.raw_events_table,
        target_countries=args.target_countries,
        env=args.env,
        model=args.model,
    )

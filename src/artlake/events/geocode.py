"""Geocoding (artlake-geocode entry point).

Reads EventDate records from bronze.event_dates where event_status IN
('future', 'undefined'), geocodes location_text via Nominatim (geopy) with an
LLM fallback for strings that Nominatim cannot resolve, and writes
EventLocation records to bronze.event_location.

location_status values:
  identified         → country resolved (Nominatim or LLM)
  missing            → unresolvable location
  requires_validation → country resolved but not in target_countries

lat/lng are stored only when Nominatim resolves; LLM fallback provides the
country code without coordinates (lat/lng remain None).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from loguru import logger
from openai import OpenAI

from artlake.models.event import EventDate, EventLocation, LocationStatus

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
    events: list[EventDate],
    target_countries: list[str],
    geocode_fn: GeocodeFn,
    llm_address_fn: LLMAddressFn | None = None,
    llm_country_fn: LLMCountryFn | None = None,
) -> list[EventLocation]:
    """Geocode events and return EventLocation records.

    Resolution happens in three stages, each used only when the previous fails:

    1. Nominatim on raw location_text → lat/lng + country
    2. LLM extracts a clean address → Nominatim on that address → lat/lng + country
    3. LLM returns ISO-3166-1 alpha-2 country code directly (no coordinates)

    Status rules:
      - country resolved and in target_countries → location_status='identified'
      - unresolvable (country=None)              → location_status='missing'
      - resolved but not in target_countries     → location_status='requires_validation'

    Args:
        events: EventDate records to geocode.
        target_countries: ISO-3166-1 alpha-2 codes to accept (e.g. ["NL", "BE"]).
        geocode_fn: Rate-limited geocoding callable.
        llm_address_fn: Optional LLM callable to normalize messy location text.
        llm_country_fn: Optional LLM callable of last resort for country code.

    Returns:
        EventLocation list (same length as input, order preserved).
    """
    geocode_cache: dict[str, GeoResult] = {}
    address_cache: dict[str, str | None] = {}
    country_cache: dict[str, str | None] = {}
    target_set = {c.upper() for c in target_countries}
    result: list[EventLocation] = []

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
            result.append(
                EventLocation(
                    fingerprint=event.fingerprint,
                    location_text=event.location_text,
                    lat=lat,
                    lng=lng,
                    country=None,
                    location_status=LocationStatus.MISSING,
                )
            )
            logger.warning(
                "Unresolvable location for {} ('{}')",
                event.fingerprint,
                event.location_text,
            )
        elif country_code in target_set:
            result.append(
                EventLocation(
                    fingerprint=event.fingerprint,
                    location_text=event.location_text,
                    lat=lat,
                    lng=lng,
                    country=country_code,
                    location_status=LocationStatus.IDENTIFIED,
                )
            )
            logger.info(
                "Identified event {} (country={})", event.fingerprint, country_code
            )
        else:
            result.append(
                EventLocation(
                    fingerprint=event.fingerprint,
                    location_text=event.location_text,
                    lat=lat,
                    lng=lng,
                    country=country_code,
                    location_status=LocationStatus.REQUIRES_VALIDATION,
                )
            )
            logger.info(
                "Requires validation: event {} (country={} not in {})",
                event.fingerprint,
                country_code,
                target_countries,
            )

    return result


# ---------------------------------------------------------------------------
# Spark integration (pragma: no cover — tested via integration marker)
# ---------------------------------------------------------------------------


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


def _build_geocode_fn(env: str) -> GeocodeFn:  # pragma: no cover
    """Build a rate-limited Nominatim geocode callable."""
    from geopy.extra.rate_limiter import RateLimiter
    from geopy.geocoders import Nominatim

    geolocator = Nominatim(user_agent=f"artlake-{env}")
    return RateLimiter(geolocator.geocode, min_delay_seconds=1)  # type: ignore[no-any-return]


def _build_llm_address_fn(client: OpenAI, model: str) -> LLMAddressFn:  # pragma: no cover
    """Return an LLM-backed address normalization callable."""

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
    """Return an LLM-backed country resolution callable."""

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
    event_dates_table: str,
    event_location_table: str,
    target_countries: list[str],
    env: str = "dev",
    model: str = "databricks-meta-llama-3-3-70b-instruct",
) -> int:
    """Geocode future/undefined EventDate rows and write EventLocation records.

    Reads only future/undefined events from event_dates (pipeline gate),
    anti-joins against event_location to skip already-geocoded fingerprints,
    geocodes location_text via Nominatim (1 req/s rate-limited) with LLM
    fallback, and appends EventLocation records to event_location.

    Args:
        event_dates_table: Fully-qualified bronze.event_dates Delta table.
        event_location_table: Fully-qualified bronze.event_location Delta table.
        target_countries: ISO-3166-1 alpha-2 codes to accept (e.g. ["NL", "BE"]).
        env: Deployment environment tag used as Nominatim user_agent suffix.
        model: Databricks Foundation Model ID used for LLM country fallback.

    Returns:
        Number of events with location_status='identified'.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import FloatType, StringType, StructField, StructType

    spark = SparkSession.builder.getOrCreate()

    # Pipeline gate: only future/undefined events proceed
    events_df = spark.table(event_dates_table).filter(
        F.col("event_status").isin("future", "undefined")
    )

    # Anti-join: skip fingerprints already written to event_location
    if spark.catalog.tableExists(event_location_table):
        done_df = spark.table(event_location_table).select("fingerprint")
        events_df = events_df.join(done_df, on="fingerprint", how="left_anti")

    rows = events_df.select("fingerprint", "location_text").collect()

    if not rows:
        logger.info("No new events to geocode")
        return 0

    logger.info("Geocoding {} events", len(rows))

    geocode_fn = _build_geocode_fn(env)
    llm_client = _create_default_client()
    llm_address_fn = _build_llm_address_fn(llm_client, model)
    llm_country_fn = _build_llm_country_fn(llm_client, model)

    # Build lightweight EventDate stubs (only fingerprint + location_text needed)
    events = [
        EventDate(
            fingerprint=row["fingerprint"],
            title="",
            description="",
            location_text=row["location_text"] or "",
            language="UNKNOWN",
            source="",
            url="http://placeholder",  # type: ignore[arg-type]
        )
        for row in rows
    ]

    locations = apply_geocoding(
        events, target_countries, geocode_fn, llm_address_fn, llm_country_fn
    )

    location_schema = StructType(
        [
            StructField("fingerprint", StringType(), False),
            StructField("location_text", StringType(), False),
            StructField("lat", FloatType(), True),
            StructField("lng", FloatType(), True),
            StructField("country", StringType(), True),
            StructField("location_status", StringType(), False),
        ]
    )
    # Deduplicate by fingerprint
    seen: set[str] = set()
    location_rows = []
    for loc in locations:
        if loc.fingerprint not in seen:
            seen.add(loc.fingerprint)
            location_rows.append(
                (
                    loc.fingerprint,
                    loc.location_text,
                    loc.lat,
                    loc.lng,
                    loc.country,
                    str(loc.location_status),
                )
            )

    location_df = spark.createDataFrame(location_rows, schema=location_schema)

    parts = event_location_table.split(".")
    if len(parts) == 3:
        catalog, db, _ = parts
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db}")

    location_df.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(event_location_table)

    identified = sum(
        1 for loc in locations if loc.location_status == LocationStatus.IDENTIFIED
    )
    logger.info(
        "Geocoding complete: {} identified, {} missing, {} requires_validation",
        identified,
        sum(1 for loc in locations if loc.location_status == LocationStatus.MISSING),
        sum(
            1
            for loc in locations
            if loc.location_status == LocationStatus.REQUIRES_VALIDATION
        ),
    )
    return identified


def main() -> None:  # pragma: no cover
    """Entry point for artlake-geocode wheel task."""
    import argparse

    parser = argparse.ArgumentParser(description="ArtLake geocoder")
    parser.add_argument(
        "--event-dates-table",
        default="artlake.bronze.event_dates",
        help="Fully-qualified bronze.event_dates Delta table",
    )
    parser.add_argument(
        "--event-location-table",
        default="artlake.bronze.event_location",
        help="Fully-qualified bronze.event_location Delta table",
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
        event_dates_table=args.event_dates_table,
        event_location_table=args.event_location_table,
        target_countries=args.target_countries,
        env=args.env,
        model=args.model,
    )

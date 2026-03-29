"""DuckDuckGo general web search for art events."""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

from artlake.models.event import RawEvent
from artlake.scrape.pages import fingerprint as make_fingerprint
from artlake.search.load import load_queries
from artlake.search.models import SearchQuery

if TYPE_CHECKING:
    from ddgs import DDGS
    from pyspark.sql import SparkSession


_SOURCE = "duckduckgo"
_RATE_LIMIT_SLEEP_S = 2.0


def _make_event(
    result: dict[str, str], language: str, query_country: str | None = None
) -> RawEvent | None:
    """Map a single DDGS result dict to a RawEvent.

    Returns None if required fields (href, title) are missing.
    """
    url = result.get("href") or result.get("url")
    title = result.get("title", "")
    snippet = result.get("body") or result.get("snippet", "")

    if not url or not title:
        return None

    try:
        return RawEvent(
            fingerprint=make_fingerprint(url),
            url=url,  # type: ignore[arg-type]
            title=title,
            snippet=snippet,
            source=_SOURCE,
            language=language,
            query_country=query_country,
        )
    except Exception:
        logger.warning("Skipping result with invalid URL: {}", url[:120])
        return None


def search_web(
    queries: list[SearchQuery],
    *,
    max_results: int = 10,
    ddgs: DDGS | None = None,
) -> list[RawEvent]:
    """Execute DuckDuckGo searches for a list of pre-generated queries.

    Args:
        queries: Pre-generated search queries (from queries.yml).
        max_results: Maximum results to fetch per query.
        ddgs: Optional DDGS instance (injected for testing).

    Returns:
        List of RawEvent objects tagged with query language.
    """
    from ddgs import DDGS
    from ddgs.exceptions import DDGSException, RatelimitException

    client = ddgs or DDGS()
    events: list[RawEvent] = []

    for query in queries:
        logger.info(
            "Searching: '{}' [lang={}, country={}]",
            query.query,
            query.language,
            query.country_code,
        )
        try:
            results = client.text(query.query, max_results=max_results)
        except RatelimitException:
            logger.warning(
                "Rate limited on query '{}' — sleeping {}s then skipping",
                query.query,
                _RATE_LIMIT_SLEEP_S,
            )
            time.sleep(_RATE_LIMIT_SLEEP_S)
            continue
        except DDGSException as exc:
            # Covers TimeoutException and any other DDG errors
            logger.warning("Search failed for query '{}': {}", query.query, exc)
            continue

        if not results:
            logger.warning("No results for query '{}'", query.query)
            continue

        for raw in results:
            event = _make_event(raw, query.language, query.country_code)
            if event is not None:
                events.append(event)

        time.sleep(_RATE_LIMIT_SLEEP_S)

    logger.info("Collected {} search results from {} queries", len(events), len(queries))
    return events


def _ensure_catalog(spark: SparkSession, catalog: str, env: str = "dev") -> None:
    """Create catalog if it doesn't exist.

    Uses the managed location URL from the external location resource.

    Args:
        spark: Active SparkSession.
        catalog: Catalog name to create.
        env: Deployment environment (dev/tst/acc/prd), used to resolve el-uc-{env}.
    """
    external_location = f"el-uc-{env}"
    from databricks.sdk import WorkspaceClient

    existing = {r[0] for r in spark.sql("SHOW CATALOGS").collect()}
    if catalog in existing:
        return

    w = WorkspaceClient()
    loc = w.external_locations.get(external_location)
    storage_root = loc.url.rstrip("/")
    logger.info("Creating catalog '{}' at {}", catalog, storage_root)
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog} MANAGED LOCATION '{storage_root}'")


def write_results(events: list[RawEvent], table: str, env: str = "dev") -> None:
    """Write RawEvent records to a Delta table via Spark.

    Args:
        events: Search results to persist.
        table: Fully-qualified Delta table name (e.g. artlake.staging.search_results).
        env: Deployment environment (dev/tst/acc/prd).
    """
    import pandas as pd
    from pyspark.sql import SparkSession

    if not events:
        logger.warning("No results to write — skipping Delta write")
        return

    spark = SparkSession.builder.getOrCreate()

    # Ensure catalog and schema exist
    parts = table.split(".")
    if len(parts) == 3:
        catalog, schema, _ = parts
        _ensure_catalog(spark, catalog, env=env)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    rows = [
        {
            "fingerprint": e.fingerprint,
            "url": str(e.url),
            "title": e.title,
            "snippet": e.snippet,
            "source": e.source,
            "language": e.language,
            "query_country": e.query_country,
            "ingested_at": e.ingested_at,
        }
        for e in events
    ]
    df = spark.createDataFrame(pd.DataFrame(rows))
    # Pre-create the table to avoid a race condition when search_web and
    # search_social both start up in parallel and both attempt the first write.
    # CREATE TABLE IF NOT EXISTS is atomic in Unity Catalog.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            fingerprint STRING,
            url STRING,
            title STRING,
            snippet STRING,
            source STRING,
            language STRING,
            query_country STRING,
            ingested_at TIMESTAMP
        ) USING DELTA
    """)
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        table
    )
    logger.info("Wrote {} rows to {}", len(rows), table)


def main() -> None:
    """Entry point for artlake-search wheel task."""
    import argparse

    parser = argparse.ArgumentParser(description="DuckDuckGo art event search.")
    parser.add_argument(
        "--queries",
        type=Path,
        default=Path("config/output/queries.yml"),
    )
    parser.add_argument(
        "--table",
        default="artlake.staging.search_results",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=10,
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of queries to process before writing results to Delta.",
    )
    parser.add_argument(
        "--env",
        default="dev",
        help="Deployment environment (dev/tst/acc/prd), used to resolve el-uc-{env}.",
    )
    args = parser.parse_args()

    queries = load_queries(args.queries)
    logger.info("Loaded {} queries from {}", len(queries), args.queries)

    for i in range(0, len(queries), args.batch_size):
        batch = queries[i : i + args.batch_size]
        logger.info(
            "Processing query batch {}/{} (queries {}-{})",
            i // args.batch_size + 1,
            -(-len(queries) // args.batch_size),
            i + 1,
            min(i + args.batch_size, len(queries)),
        )
        events = search_web(batch, max_results=args.max_results)
        write_results(events, args.table, env=args.env)

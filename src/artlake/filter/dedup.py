"""Deduplication + seen-URL tracking (artlake-dedup entry point)."""

from __future__ import annotations

import hashlib

from loguru import logger
from pydantic import HttpUrl

from artlake.models.event import SeenUrl


def dedup(
    results: list[dict[str, str]],
    seen: list[SeenUrl],
) -> tuple[list[SeenUrl], list[SeenUrl]]:
    """Split *results* into new and duplicate records (pure-Python, for tests).

    Args:
        results: Dicts with keys ``url``, ``title``, ``source``.
        seen: Already-seen URL records loaded from ``staging.seen_urls``.

    Returns:
        ``(new_records, duplicates)``
    """
    seen_fingerprints: set[str] = {s.fingerprint for s in seen}
    batch_fingerprints: set[str] = set()

    new_records: list[SeenUrl] = []
    duplicates: list[SeenUrl] = []

    for row in results:
        raw_url = row["url"]
        title = row.get("title", "")
        source = row.get("source", "")

        fingerprint = hashlib.sha256(raw_url.encode()).hexdigest()
        seen_url = SeenUrl(
            url=HttpUrl(raw_url), title=title, source=source, fingerprint=fingerprint
        )

        if fingerprint in seen_fingerprints or fingerprint in batch_fingerprints:
            logger.debug("Duplicate: {}", raw_url)
            duplicates.append(seen_url)
        else:
            new_records.append(seen_url)
            batch_fingerprints.add(fingerprint)

    return new_records, duplicates


def run_dedup(
    search_results_table: str,
    seen_urls_table: str,
    env: str = "dev",
) -> None:
    """Spark dedup: sha2(url) fingerprint + anti-join against seen_urls."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

    logger.info("Reading search results from {}", search_results_table)
    results_df = (
        spark.table(search_results_table)
        .select("url", "title", "source")
        .withColumn("fingerprint", F.sha2(F.col("url"), 256))
        .dropDuplicates(["fingerprint"])
    )

    if spark.catalog.tableExists(seen_urls_table):
        seen_fingerprints = spark.table(seen_urls_table).select("fingerprint")
        new_df = results_df.join(seen_fingerprints, on="fingerprint", how="left_anti")
    else:
        logger.info("seen_urls table does not exist yet — first run")
        parts = seen_urls_table.split(".")
        if len(parts) == 3:
            catalog, schema, _ = parts
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        new_df = results_df

    new_count = new_df.count()
    logger.info("New URLs to write: {}", new_count)

    if new_count == 0:
        logger.info("No new URLs — skipping write")
        return

    (
        new_df.withColumn("ingested_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(seen_urls_table)
    )
    logger.info("Wrote {} new URLs to {}", new_count, seen_urls_table)


def main() -> None:
    """Entry point for artlake-dedup wheel task."""
    import argparse

    parser = argparse.ArgumentParser(description="ArtLake dedup + seen-URL tracker")
    parser.add_argument(
        "--search-results-table",
        default="artlake.staging.search_results",
        help="Fully-qualified Delta table with search results",
    )
    parser.add_argument(
        "--seen-urls-table",
        default="artlake.staging.seen_urls",
        help="Fully-qualified Delta table for seen URLs",
    )
    parser.add_argument(
        "--env",
        default="dev",
        help="Deployment environment (dev/tst/acc/prd)",
    )
    args = parser.parse_args()
    run_dedup(
        search_results_table=args.search_results_table,
        seen_urls_table=args.seen_urls_table,
        env=args.env,
    )

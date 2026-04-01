"""Silver event enrichment — join bronze event tables (artlake-enrich-events).

Entry point: artlake-enrich-events

Joins bronze.event_dates + bronze.event_location + bronze.event_category on
fingerprint and writes EventDetails records to silver.event_details.

Runs after geocode and categorise_llm have both completed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from artlake.models.event import CategoryStatus, EventStatus, LocationStatus

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_EVENT_DATES_TABLE_DEFAULT = "artlake.bronze.event_dates"
_EVENT_LOCATION_TABLE_DEFAULT = "artlake.bronze.event_location"
_EVENT_CATEGORY_TABLE_DEFAULT = "artlake.bronze.event_category"
_EVENT_DETAILS_TABLE_DEFAULT = "artlake.silver.event_details"


# ---------------------------------------------------------------------------
# Spark integration (pragma: no cover — tested via integration marker)
# ---------------------------------------------------------------------------


def _event_details_schema() -> StructType:  # pragma: no cover
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("fingerprint", StringType(), False),
            StructField("url", StringType(), False),
            StructField("source", StringType(), False),
            StructField("language", StringType(), False),
            StructField("query_country", StringType(), True),
            StructField("domain_country", StringType(), True),
            StructField("title", StringType(), False),
            StructField("description", StringType(), False),
            StructField("date_start", TimestampType(), True),
            StructField("date_end", TimestampType(), True),
            StructField("location_text", StringType(), False),
            StructField("event_status", StringType(), False),
            StructField("lat", DoubleType(), True),
            StructField("lng", DoubleType(), True),
            StructField("country", StringType(), True),
            StructField("location_status", StringType(), False),
            StructField("category", StringType(), True),
            StructField("category_status", StringType(), False),
            StructField("artifact_urls", ArrayType(StringType()), False),
            StructField("ingested_at", TimestampType(), False),
        ]
    )


def run_enrich(  # pragma: no cover
    event_dates_table: str,
    event_location_table: str,
    event_category_table: str,
    event_details_table: str,
) -> int:
    """Join bronze event tables and write EventDetails to silver.event_details.

    Anti-joins against existing event_details so fingerprints are not
    duplicated across runs.

    Args:
        event_dates_table: Fully-qualified bronze.event_dates Delta table.
        event_location_table: Fully-qualified bronze.event_location Delta table.
        event_category_table: Fully-qualified bronze.event_category Delta table.
        event_details_table: Fully-qualified silver.event_details Delta table.

    Returns:
        Number of EventDetails rows written.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

    dates_df = spark.table(event_dates_table)
    location_df = spark.table(event_location_table).select(
        "fingerprint", "lat", "lng", "country", "location_status"
    )
    category_df = spark.table(event_category_table).select(
        "fingerprint", "category", "category_status"
    )

    joined_df = (
        dates_df.join(location_df, on="fingerprint", how="left")
        .join(category_df, on="fingerprint", how="left")
        .withColumn(
            "location_status",
            F.coalesce(F.col("location_status"), F.lit(LocationStatus.MISSING.value)),
        )
        .withColumn(
            "category_status",
            F.coalesce(F.col("category_status"), F.lit(CategoryStatus.MISSING.value)),
        )
        .withColumn(
            "event_status",
            F.coalesce(F.col("event_status"), F.lit(EventStatus.UNDEFINED.value)),
        )
        .select(
            "fingerprint",
            F.col("url").cast("string").alias("url"),
            "source",
            "language",
            "query_country",
            "domain_country",
            "title",
            "description",
            "date_start",
            "date_end",
            "location_text",
            "event_status",
            "lat",
            "lng",
            "country",
            "location_status",
            "category",
            "category_status",
            "artifact_urls",
            "ingested_at",
        )
    )

    # Anti-join: skip fingerprints already written
    if spark.catalog.tableExists(event_details_table):
        done_df = spark.table(event_details_table).select("fingerprint")
        joined_df = joined_df.join(done_df, on="fingerprint", how="left_anti")

    count = joined_df.count()
    if count == 0:
        logger.info("No new event_details to write")
        return 0

    parts = event_details_table.split(".")
    if len(parts) == 3:
        catalog, db, _ = parts
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db}")

    joined_df.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(event_details_table)

    logger.info("Wrote {} EventDetails rows to {}", count, event_details_table)
    return int(count)


def main() -> None:  # pragma: no cover
    """Entry point for artlake-enrich-events wheel task."""
    import argparse

    parser = argparse.ArgumentParser(description="ArtLake silver event enrichment")
    parser.add_argument(
        "--event-dates-table",
        default=_EVENT_DATES_TABLE_DEFAULT,
        help="Fully-qualified bronze.event_dates Delta table",
    )
    parser.add_argument(
        "--event-location-table",
        default=_EVENT_LOCATION_TABLE_DEFAULT,
        help="Fully-qualified bronze.event_location Delta table",
    )
    parser.add_argument(
        "--event-category-table",
        default=_EVENT_CATEGORY_TABLE_DEFAULT,
        help="Fully-qualified bronze.event_category Delta table",
    )
    parser.add_argument(
        "--event-details-table",
        default=_EVENT_DETAILS_TABLE_DEFAULT,
        help="Fully-qualified silver.event_details Delta table",
    )
    args = parser.parse_args()
    run_enrich(
        event_dates_table=args.event_dates_table,
        event_location_table=args.event_location_table,
        event_category_table=args.event_category_table,
        event_details_table=args.event_details_table,
    )

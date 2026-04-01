"""Artifact text extractor (artlake-process-artifacts entry point).

Entry point: artlake-process-artifacts

Reads EventArtifact records from artlake.bronze.event_artifacts where
processing_status = 'downloaded', extracts raw text via ai_parse_document
(Databricks-native SQL function), and writes RawEventArtifact records to
artlake.bronze.event_artifacts_text.

Structured LLM extraction (deadline, requirements, location, fees) is
intentionally NOT performed here — that belongs in the silver layer via
artlake-enrich-artifacts.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from artlake.models.event import ProcessingStatus, RawEventArtifact

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_MODEL = "databricks-meta-llama-3-3-70b-instruct"
_RAW_EVENT_ARTIFACTS_TABLE_DEFAULT = "artlake.bronze.event_artifacts_text"
_EVENT_ARTIFACTS_TABLE_DEFAULT = "artlake.bronze.event_artifacts"


# ---------------------------------------------------------------------------
# Pure helpers — fully testable without Spark or a live LLM
# ---------------------------------------------------------------------------


def make_raw_event_artifact(
    artifact_id: str,
    event_id: str,
    artifact_type: str,
    file_path: str,
    extracted_text: str | None,
    status: ProcessingStatus,
) -> RawEventArtifact:
    """Construct a RawEventArtifact record from extraction results.

    Args:
        artifact_id: Artifact identifier (SHA256 of artifact URL).
        event_id: Parent event identifier.
        artifact_type: ``'pdf'`` or ``'image'``.
        file_path: UC Volume path of the raw artifact file.
        extracted_text: Full text extracted by ai_parse_document, or None.
        status: Final processing status (done or failed).

    Returns:
        A :class:`RawEventArtifact` ready to be written to Delta.
    """
    return RawEventArtifact(
        id=artifact_id,
        event_id=event_id,
        artifact_type=artifact_type,
        file_path=file_path,
        extracted_text=extracted_text,
        processing_status=status,
    )


# ---------------------------------------------------------------------------
# Spark integration (pragma: no cover — tested via integration marker)
# ---------------------------------------------------------------------------


def _raw_event_artifact_schema() -> StructType:  # pragma: no cover
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType(
        [
            StructField("id", StringType(), False),
            StructField("event_id", StringType(), False),
            StructField("artifact_type", StringType(), False),
            StructField("file_path", StringType(), False),
            StructField("extracted_text", StringType(), True),
            StructField("processing_status", StringType(), False),
            StructField("processed_at", StringType(), False),
        ]
    )


def _write_raw_event_artifact(  # pragma: no cover
    spark: SparkSession,
    artifact: RawEventArtifact,
    table: str,
) -> None:
    schema = _raw_event_artifact_schema()
    row = artifact.model_dump(mode="json")
    row["processed_at"] = str(row["processed_at"])

    df = spark.createDataFrame([row], schema=schema)

    parts = table.split(".")
    if len(parts) == 3:
        catalog, db, _ = parts
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db}")

    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table)
    )


def run_list(  # pragma: no cover
    event_artifacts_table: str,
    event_artifacts_text_table: str,
    limit: int = 0,
) -> list[str]:
    """Emit IDs of downloaded artifacts not yet extracted as a task value.

    Reads ``event_artifacts`` where ``processing_status = 'downloaded'`` and
    anti-joins with ``event_artifacts_text`` (done or failed) so already-processed
    artifacts are skipped.

    Returns the list of artifact IDs to process.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

    downloaded_df = (
        spark.table(event_artifacts_table)
        .filter(F.col("processing_status") == ProcessingStatus.DOWNLOADED.value)
        .select("id")
        .distinct()
    )

    _TERMINAL_STATUSES = [ProcessingStatus.DONE, ProcessingStatus.FAILED]

    if spark.catalog.tableExists(event_artifacts_text_table):
        terminal_df = (
            spark.table(event_artifacts_text_table)
            .filter(
                F.col("processing_status").isin([s.value for s in _TERMINAL_STATUSES])
            )
            .select("id")
        )
        downloaded_df = downloaded_df.join(terminal_df, on="id", how="left_anti")
    else:
        logger.info(
            "event_artifacts_text table does not exist yet"
            " — all downloaded artifacts are new"
        )

    if limit > 0:
        downloaded_df = downloaded_df.limit(limit)

    ids: list[str] = [row["id"] for row in downloaded_df.collect()]
    logger.info("Artifact IDs to extract: {}", len(ids))

    try:
        from databricks.sdk.runtime import dbutils

        dbutils.jobs.taskValues.set(key="id", value=ids)
        logger.info("Task value 'id' set with {} entries", len(ids))
    except ImportError:
        logger.warning("dbutils not available — skipping task value set")

    return ids


def run_extract(  # pragma: no cover
    artifact_id: str,
    event_artifacts_table: str,
    event_artifacts_text_table: str,
) -> None:
    """Extract raw text from one artifact via ai_parse_document.

    Looks up the artifact record in ``event_artifacts`` by ID, loads the binary
    from the UC Volume, runs ``ai_parse_document``, and writes a
    :class:`RawEventArtifact` record with ``processing_status = 'done'`` on
    success or ``'failed'`` if any step raises.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.functions import expr

    spark = SparkSession.builder.getOrCreate()

    rows = (
        spark.table(event_artifacts_table)
        .filter(F.col("id") == artifact_id)
        .select("id", "event_id", "artifact_type", "file_path")
        .limit(1)
        .collect()
    )
    if not rows:
        logger.warning("No event_artifact record found for id: {}", artifact_id)
        return

    row = rows[0]
    event_id: str = row["event_id"]
    artifact_type: str = row["artifact_type"]
    file_path: str = row["file_path"]

    extracted_text: str | None = None
    status = ProcessingStatus.FAILED

    try:
        parse_df = (
            spark.read.format("binaryFile")
            .load(file_path)
            .withColumn("parsed", expr("ai_parse_document(content)"))
            .withColumn(
                "extracted_text",
                expr(
                    "concat_ws(' ', transform("
                    "  from_json(to_json(parsed:document:elements),"
                    "    'ARRAY<STRUCT<content: STRING>>'), "
                    "  e -> e.content"
                    "))"
                ),
            )
            .withColumn("parse_error", expr("to_json(parsed:error_status)"))
            .select("extracted_text", "parse_error")
        )

        parse_row = parse_df.first()
        if parse_row is None:
            raise ValueError(f"ai_parse_document returned no rows for {file_path}")

        parse_error = parse_row["parse_error"]
        if parse_error and parse_error not in ("null", "NULL", '""', ""):
            raise ValueError(f"ai_parse_document error for {file_path}: {parse_error}")

        extracted_text = parse_row["extracted_text"] or ""
        status = ProcessingStatus.DONE
        logger.info(
            "Extracted {} chars from {} (id: {})",
            len(extracted_text),
            file_path,
            artifact_id,
        )

    except Exception as exc:
        logger.error(
            "Failed to extract artifact {} ({}): {}", artifact_id, file_path, exc
        )

    artifact = make_raw_event_artifact(
        artifact_id=artifact_id,
        event_id=event_id,
        artifact_type=artifact_type,
        file_path=file_path,
        extracted_text=extracted_text,
        status=status,
    )
    _write_raw_event_artifact(spark, artifact, event_artifacts_text_table)
    logger.info(
        "Wrote RawEventArtifact for id: {} with status {}", artifact_id, status.value
    )


def main() -> None:  # pragma: no cover
    """Entry point for artlake-process-artifacts wheel task.

    Two modes:
      list     — Emit IDs of downloaded artifacts as a Databricks task value
                 for a downstream for_each_task.
      extract  — Extract raw text for one artifact (for_each inner task).
    """
    import argparse

    parser = argparse.ArgumentParser(description="ArtLake artifact text extractor")
    parser.add_argument(
        "--mode",
        choices=["list", "extract"],
        required=True,
        help="'list' emits artifact IDs as a task value; "
        "'extract' extracts text for one artifact",
    )
    parser.add_argument(
        "--id",
        help="Artifact ID to process (required for --mode extract)",
    )
    parser.add_argument(
        "--event-artifacts-table",
        default=_EVENT_ARTIFACTS_TABLE_DEFAULT,
        help="Fully-qualified bronze.event_artifacts Delta table",
    )
    parser.add_argument(
        "--raw-event-artifacts-table",
        default=_RAW_EVENT_ARTIFACTS_TABLE_DEFAULT,
        help="Fully-qualified bronze.event_artifacts_text Delta table",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max artifact IDs to emit in list mode (0 = no limit)",
    )
    parser.add_argument(
        "--env",
        default="dev",
        help="Deployment environment (dev/tst/acc/prd)",
    )
    args = parser.parse_args()

    if args.mode == "list":
        run_list(
            event_artifacts_table=args.event_artifacts_table,
            event_artifacts_text_table=args.event_artifacts_text_table,
            limit=args.limit,
        )
    else:
        if not args.id:
            parser.error("--id is required for --mode extract")
        run_extract(
            artifact_id=args.id,
            event_artifacts_table=args.event_artifacts_table,
            event_artifacts_text_table=args.event_artifacts_text_table,
        )

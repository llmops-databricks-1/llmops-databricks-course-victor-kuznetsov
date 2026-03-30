"""Artifact processor — ai_parse_document extraction + LLM summary.

Entry point: artlake-process-artifacts

Reads EventArtifact records from artlake.bronze.raw_artifacts where
processing_status = 'downloaded', extracts text via ai_parse_document
(Databricks-native SQL function), generates a structured LLM summary
(deadline, requirements, location, fees), and writes ProcessedArtifact
records to artlake.bronze.processed_artifacts.

Runs before artlake-translate; translate joins artifact summaries with
events before translating.

Silver zone note:
  When promoting to silver, merge the structured fields extracted here
  (deadline, requirements, location, fees) with the equivalent fields
  extracted from scraped page text (clean.events).  The artifact-derived
  values are typically more authoritative (they come from the official
  call document), so prefer them where both sources are non-null.
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any

import backoff
from loguru import logger
from openai import OpenAI

from artlake.models.event import ProcessedArtifact, ProcessingStatus

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_MODEL = "databricks-meta-llama-3-3-70b-instruct"
_PROCESSED_ARTIFACTS_TABLE_DEFAULT = "artlake.bronze.processed_artifacts"
_RAW_ARTIFACTS_TABLE_DEFAULT = "artlake.bronze.raw_artifacts"

# ---------------------------------------------------------------------------
# Pure helpers — fully testable without Spark or a live LLM
# ---------------------------------------------------------------------------


def build_system_prompt() -> str:
    """Build the LLM system prompt for artifact summarisation."""
    return (
        "You are an art event document analyser.\n"
        "Extract key information from the following document text.\n"
        "Respond ONLY with a JSON object with exactly these keys:\n"
        '{"deadline": "...", "requirements": "...", "location": "...", "fees": "..."}\n'
        "Use null for any field not found in the text.\n"
        "Be concise — each field should be a short summary (max 2 sentences)."
    )


def parse_llm_response(content: str) -> dict[str, str | None]:
    """Parse the LLM JSON response into a summary dict.

    Strips optional markdown fences before parsing.
    Returns a dict with keys deadline, requirements, location, fees.
    Missing or invalid keys default to None.

    Args:
        content: Raw LLM response text, optionally wrapped in ```json fences.

    Returns:
        Dict with keys ``deadline``, ``requirements``, ``location``, ``fees``.
    """
    cleaned = re.sub(r"```(?:json)?\s*", "", content).strip().rstrip("`").strip()
    data: dict[str, Any] = json.loads(cleaned)
    return {
        "deadline": data.get("deadline")
        if isinstance(data.get("deadline"), str)
        else None,
        "requirements": data.get("requirements")
        if isinstance(data.get("requirements"), str)
        else None,
        "location": data.get("location")
        if isinstance(data.get("location"), str)
        else None,
        "fees": data.get("fees") if isinstance(data.get("fees"), str) else None,
    }


def make_processed_artifact(
    artifact_id: str,
    event_id: str,
    artifact_type: str,
    file_path: str,
    extracted_text: str | None,
    summary: dict[str, str | None],
    status: ProcessingStatus,
) -> ProcessedArtifact:
    """Construct a ProcessedArtifact record from extraction results.

    Args:
        artifact_id: Artifact identifier (SHA256 of artifact URL).
        event_id: Parent event identifier.
        artifact_type: ``'pdf'`` or ``'image'``.
        file_path: UC Volume path of the raw artifact file.
        extracted_text: Full text extracted by ai_parse_document, or None.
        summary: Dict with deadline/requirements/location/fees from the LLM.
        status: Final processing status (done or failed).

    Returns:
        A :class:`ProcessedArtifact` ready to be written to Delta.
    """
    return ProcessedArtifact(
        id=artifact_id,
        event_id=event_id,
        artifact_type=artifact_type,
        file_path=file_path,
        extracted_text=extracted_text,
        deadline=summary.get("deadline"),
        requirements=summary.get("requirements"),
        location=summary.get("location"),
        fees=summary.get("fees"),
        processing_status=status,
    )


# ---------------------------------------------------------------------------
# LLM interaction
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


@backoff.on_exception(backoff.expo, Exception, max_tries=3)
def _summarise_text(
    text: str,
    client: OpenAI,
    model: str,
) -> dict[str, str | None]:
    """Send extracted text to the LLM and return a structured summary.

    Args:
        text: Full text extracted from the artifact.
        client: OpenAI client pointed at Databricks serving endpoints.
        model: Model name.

    Returns:
        Dict with keys ``deadline``, ``requirements``, ``location``, ``fees``.
    """
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": build_system_prompt()},
            {"role": "user", "content": text},
        ],
        temperature=0.0,
        max_tokens=512,
    )
    raw = response.choices[0].message.content or ""
    return parse_llm_response(raw)


# ---------------------------------------------------------------------------
# Spark integration (pragma: no cover — tested via integration marker)
# ---------------------------------------------------------------------------


def _processed_schema() -> StructType:  # pragma: no cover
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType(
        [
            StructField("id", StringType(), False),
            StructField("event_id", StringType(), False),
            StructField("artifact_type", StringType(), False),
            StructField("file_path", StringType(), False),
            StructField("extracted_text", StringType(), True),
            StructField("deadline", StringType(), True),
            StructField("requirements", StringType(), True),
            StructField("location", StringType(), True),
            StructField("fees", StringType(), True),
            StructField("processing_status", StringType(), False),
            StructField("processed_at", StringType(), False),
        ]
    )


def _write_processed_artifact(  # pragma: no cover
    spark: SparkSession,
    artifact: ProcessedArtifact,
    table: str,
) -> None:
    schema = _processed_schema()
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
    raw_artifacts_table: str,
    processed_artifacts_table: str,
    limit: int = 0,
) -> list[str]:
    """Emit IDs of downloaded artifacts not yet processed as a task value.

    Reads ``raw_artifacts`` where ``processing_status = 'downloaded'`` and
    anti-joins with ``processed_artifacts`` (done or failed) so already-processed
    artifacts are skipped.

    Returns the list of artifact IDs to process.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

    downloaded_df = (
        spark.table(raw_artifacts_table)
        .filter(F.col("processing_status") == ProcessingStatus.DOWNLOADED.value)
        .select("id")
        .distinct()
    )

    _TERMINAL_STATUSES = [ProcessingStatus.DONE, ProcessingStatus.FAILED]

    if spark.catalog.tableExists(processed_artifacts_table):
        terminal_df = (
            spark.table(processed_artifacts_table)
            .filter(
                F.col("processing_status").isin([s.value for s in _TERMINAL_STATUSES])
            )
            .select("id")
        )
        downloaded_df = downloaded_df.join(terminal_df, on="id", how="left_anti")
    else:
        logger.info(
            "processed_artifacts table does not exist yet"
            " — all downloaded artifacts are new"
        )

    if limit > 0:
        downloaded_df = downloaded_df.limit(limit)

    ids: list[str] = [row["id"] for row in downloaded_df.collect()]
    logger.info("Artifact IDs to process: {}", len(ids))

    try:
        from databricks.sdk.runtime import dbutils

        dbutils.jobs.taskValues.set(key="id", value=ids)
        logger.info("Task value 'id' set with {} entries", len(ids))
    except ImportError:
        logger.warning("dbutils not available — skipping task value set")

    return ids


def run_extract(  # pragma: no cover
    artifact_id: str,
    raw_artifacts_table: str,
    processed_artifacts_table: str,
    model: str = _DEFAULT_MODEL,
) -> None:
    """Extract text and generate LLM summary for one artifact.

    Looks up the artifact record in ``raw_artifacts`` by ID, loads the binary
    from the UC Volume, runs ``ai_parse_document``, then calls the LLM to
    produce a structured summary.  Writes a :class:`ProcessedArtifact` record
    to ``processed_artifacts`` with ``processing_status = 'done'`` on success
    or ``'failed'`` if any step raises.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.functions import expr

    spark = SparkSession.builder.getOrCreate()

    rows = (
        spark.table(raw_artifacts_table)
        .filter(F.col("id") == artifact_id)
        .select("id", "event_id", "artifact_type", "file_path")
        .limit(1)
        .collect()
    )
    if not rows:
        logger.warning("No raw_artifact record found for id: {}", artifact_id)
        return

    row = rows[0]
    event_id: str = row["event_id"]
    artifact_type: str = row["artifact_type"]
    file_path: str = row["file_path"]

    extracted_text: str | None = None
    summary: dict[str, str | None] = {
        "deadline": None,
        "requirements": None,
        "location": None,
        "fees": None,
    }
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
        logger.info(
            "Extracted {} chars from {} (id: {})",
            len(extracted_text),
            file_path,
            artifact_id,
        )

        client = _create_default_client()
        summary = _summarise_text(extracted_text, client, model)
        status = ProcessingStatus.DONE
        logger.info("LLM summary generated for id: {}", artifact_id)

    except Exception as exc:
        logger.error(
            "Failed to process artifact {} ({}): {}", artifact_id, file_path, exc
        )

    artifact = make_processed_artifact(
        artifact_id=artifact_id,
        event_id=event_id,
        artifact_type=artifact_type,
        file_path=file_path,
        extracted_text=extracted_text,
        summary=summary,
        status=status,
    )
    _write_processed_artifact(spark, artifact, processed_artifacts_table)
    logger.info(
        "Wrote ProcessedArtifact for id: {} with status {}", artifact_id, status.value
    )


def main() -> None:  # pragma: no cover
    """Entry point for artlake-process-artifacts wheel task.

    Two modes:
      list     — Emit IDs of downloaded artifacts as a Databricks task value
                 for a downstream for_each_task.
      extract  — Extract text and generate LLM summary for one artifact
                 (for_each inner task).
    """
    import argparse

    parser = argparse.ArgumentParser(description="ArtLake artifact processor")
    parser.add_argument(
        "--mode",
        choices=["list", "extract"],
        required=True,
        help="'list' emits artifact IDs as a task value; "
        "'extract' processes one artifact",
    )
    parser.add_argument(
        "--id",
        help="Artifact ID to process (required for --mode extract)",
    )
    parser.add_argument(
        "--raw-artifacts-table",
        default=_RAW_ARTIFACTS_TABLE_DEFAULT,
        help="Fully-qualified raw_artifacts Delta table",
    )
    parser.add_argument(
        "--processed-artifacts-table",
        default=_PROCESSED_ARTIFACTS_TABLE_DEFAULT,
        help="Fully-qualified processed_artifacts Delta table",
    )
    parser.add_argument(
        "--model",
        default=_DEFAULT_MODEL,
        help="Databricks Foundation Model endpoint name",
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
            raw_artifacts_table=args.raw_artifacts_table,
            processed_artifacts_table=args.processed_artifacts_table,
            limit=args.limit,
        )
    else:
        if not args.id:
            parser.error("--id is required for --mode extract")
        run_extract(
            artifact_id=args.id,
            raw_artifacts_table=args.raw_artifacts_table,
            processed_artifacts_table=args.processed_artifacts_table,
            model=args.model,
        )

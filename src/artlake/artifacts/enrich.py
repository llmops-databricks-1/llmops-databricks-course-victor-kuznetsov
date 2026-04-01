"""Silver artifact enrichment — LLM field extraction (artlake-enrich-artifacts).

Entry point: artlake-enrich-artifacts

Reads RawEventArtifact records from bronze.event_artifacts_text where
processing_status = 'done', runs LLM extraction (deadline, requirements,
location, fees), and writes EventArtifactsProcessed records to
silver.event_artifacts_details.
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any

import backoff
from loguru import logger
from openai import OpenAI

from artlake.models.event import EventArtifactsProcessed, ProcessingStatus

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_MODEL = "databricks-meta-llama-3-3-70b-instruct"
_RAW_EVENT_ARTIFACTS_TABLE_DEFAULT = "artlake.bronze.event_artifacts_text"
_EVENT_ARTIFACTS_PROCESSED_TABLE_DEFAULT = "artlake.silver.event_artifacts_details"


# ---------------------------------------------------------------------------
# Pure helpers — fully testable without Spark or a live LLM
# ---------------------------------------------------------------------------


def build_system_prompt() -> str:
    """Build the LLM system prompt for artifact field extraction."""
    return (
        "You are an art event document analyser.\n"
        "Extract key information from the following document text.\n"
        "Respond ONLY with a JSON object with exactly these keys:\n"
        '{"deadline": "...", "requirements": "...", "location": "...", "fees": "..."}\n'
        "Use null for any field not found in the text.\n"
        "Be concise — each field should be a short summary (max 2 sentences)."
    )


def parse_llm_response(content: str) -> dict[str, str | None]:
    """Parse the LLM JSON response into an extraction dict.

    Strips optional markdown fences before parsing.
    Returns a dict with keys deadline, requirements, location, fees.
    Missing or invalid keys default to None.
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


def make_event_artifacts_details(
    artifact_id: str,
    event_id: str,
    artifact_type: str,
    file_path: str,
    extracted_text: str | None,
    summary: dict[str, str | None],
    status: ProcessingStatus,
) -> EventArtifactsProcessed:
    """Construct an EventArtifactsProcessed record from extraction results."""
    return EventArtifactsProcessed(
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
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    host = w.config.host or ""
    token = w.tokens.create(lifetime_seconds=1200).token_value
    return OpenAI(
        api_key=token,
        base_url=f"{host.rstrip('/')}/serving-endpoints",
    )


@backoff.on_exception(backoff.expo, Exception, max_tries=3)
def _extract_fields(text: str, client: OpenAI, model: str) -> dict[str, str | None]:
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


def _write_processed(  # pragma: no cover
    spark: SparkSession,
    artifact: EventArtifactsProcessed,
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
    event_artifacts_text_table: str,
    event_artifacts_details_table: str,
    limit: int = 0,
) -> list[str]:
    """Emit IDs of extracted artifacts not yet processed to silver.

    Reads ``event_artifacts_text`` where ``processing_status = 'done'``
    and anti-joins with ``event_artifacts_details``.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

    pending_df = (
        spark.table(event_artifacts_text_table)
        .filter(F.col("processing_status") == ProcessingStatus.DONE.value)
        .select("id")
        .distinct()
    )

    if spark.catalog.tableExists(event_artifacts_details_table):
        done_df = (
            spark.table(event_artifacts_details_table)
            .filter(
                F.col("processing_status").isin(
                    [ProcessingStatus.DONE.value, ProcessingStatus.FAILED.value]
                )
            )
            .select("id")
        )
        pending_df = pending_df.join(done_df, on="id", how="left_anti")
    else:
        logger.info(
            "event_artifacts_details table does not exist yet — all artifacts are new"
        )

    if limit > 0:
        pending_df = pending_df.limit(limit)

    ids: list[str] = [row["id"] for row in pending_df.collect()]
    logger.info("Artifact IDs to enrich: {}", len(ids))

    try:
        from databricks.sdk.runtime import dbutils

        dbutils.jobs.taskValues.set(key="id", value=ids)
        logger.info("Task value 'id' set with {} entries", len(ids))
    except ImportError:
        logger.warning("dbutils not available — skipping task value set")

    return ids


def run_enrich_one(  # pragma: no cover
    artifact_id: str,
    event_artifacts_text_table: str,
    event_artifacts_details_table: str,
    model: str = _DEFAULT_MODEL,
) -> None:
    """Run LLM field extraction for one artifact and write to silver.

    Looks up the RawEventArtifact by ID, runs LLM extraction on extracted_text,
    and writes an EventArtifactsProcessed record.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

    rows = (
        spark.table(event_artifacts_text_table)
        .filter(F.col("id") == artifact_id)
        .select("id", "event_id", "artifact_type", "file_path", "extracted_text")
        .limit(1)
        .collect()
    )
    if not rows:
        logger.warning("No raw_event_artifact found for id: {}", artifact_id)
        return

    row = rows[0]
    extracted_text: str | None = row["extracted_text"]
    summary: dict[str, str | None] = {
        "deadline": None,
        "requirements": None,
        "location": None,
        "fees": None,
    }
    status = ProcessingStatus.FAILED

    try:
        if extracted_text:
            client = _create_default_client()
            summary = _extract_fields(extracted_text, client, model)
        status = ProcessingStatus.DONE
        logger.info("LLM extraction complete for artifact id: {}", artifact_id)
    except Exception as exc:
        logger.error("Failed to enrich artifact {} : {}", artifact_id, exc)

    artifact = make_event_artifacts_details(
        artifact_id=artifact_id,
        event_id=row["event_id"],
        artifact_type=row["artifact_type"],
        file_path=row["file_path"],
        extracted_text=extracted_text,
        summary=summary,
        status=status,
    )
    _write_processed(spark, artifact, event_artifacts_details_table)
    logger.info(
        "Wrote EventArtifactsProcessed for id: {} with status {}",
        artifact_id,
        status.value,
    )


def main() -> None:  # pragma: no cover
    """Entry point for artlake-enrich-artifacts wheel task.

    Two modes:
      list   — Emit IDs of extracted artifacts not yet processed to silver.
      enrich — Run LLM extraction for one artifact (for_each inner task).
    """
    import argparse

    parser = argparse.ArgumentParser(description="ArtLake silver artifact enrichment")
    parser.add_argument(
        "--mode",
        choices=["list", "enrich"],
        required=True,
        help="'list' emits artifact IDs; 'enrich' processes one artifact",
    )
    parser.add_argument(
        "--id",
        help="Artifact ID to enrich (required for --mode enrich)",
    )
    parser.add_argument(
        "--raw-event-artifacts-table",
        default=_RAW_EVENT_ARTIFACTS_TABLE_DEFAULT,
        help="Fully-qualified bronze.event_artifacts_text Delta table",
    )
    parser.add_argument(
        "--event-artifacts-processed-table",
        default=_EVENT_ARTIFACTS_PROCESSED_TABLE_DEFAULT,
        help="Fully-qualified silver.event_artifacts_details Delta table",
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
            event_artifacts_text_table=args.event_artifacts_text_table,
            event_artifacts_details_table=args.event_artifacts_details_table,
            limit=args.limit,
        )
    else:
        if not args.id:
            parser.error("--id is required for --mode enrich")
        run_enrich_one(
            artifact_id=args.id,
            event_artifacts_text_table=args.event_artifacts_text_table,
            event_artifacts_details_table=args.event_artifacts_details_table,
            model=args.model,
        )

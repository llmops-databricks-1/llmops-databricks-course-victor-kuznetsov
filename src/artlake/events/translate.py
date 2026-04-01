"""Content translator — event fields and artifact text to the target language.

Entry point: artlake-translate

Reads EventDetails records from artlake.silver.event_details (category NOT IN
('non_art')), joins their EventArtifactsProcessed rows from
artlake.silver.event_artifacts_details, translates all text in one LLM call
per event, and writes:

  artlake.silver.event_details_translated    — translated event fields
  artlake.silver.event_artifacts_details_translated  — translated artifact content

Events already in the target language are promoted to silver without an
LLM call (original == translated).
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import TYPE_CHECKING, Any

import backoff
import yaml
from loguru import logger
from openai import OpenAI

from artlake.models.event import (
    EventArtifactsTranslated,
    EventDetailsTranslated,
    ProcessingStatus,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_MODEL = "databricks-meta-llama-3-3-70b-instruct"
_EVENT_DETAILS_TABLE_DEFAULT = "artlake.silver.event_details"
_EVENT_ARTIFACTS_PROCESSED_TABLE_DEFAULT = "artlake.silver.event_artifacts_details"
_EVENT_DETAILS_TRANSLATED_TABLE_DEFAULT = "artlake.silver.event_details_translated"
_EVENT_ARTIFACTS_TRANSLATED_TABLE_DEFAULT = (
    "artlake.silver.event_artifacts_details_translated"
)

_EVENT_FIELDS = ["title", "description", "location_text"]
_ARTIFACT_FIELDS = ["extracted_text", "deadline", "requirements", "location", "fees"]

# ---------------------------------------------------------------------------
# Pure helpers — fully testable without Spark or a live LLM
# ---------------------------------------------------------------------------


def build_system_prompt(target_language: str) -> str:
    """Build the LLM system prompt for translation."""
    return (
        f"You are a professional translator.\n"
        f"Translate all non-null string values in the following JSON to"
        f" {target_language}.\n"
        f"Return ONLY a valid JSON object with exactly the same structure and keys.\n"
        f"Preserve null values as null. Do not add, remove, or rename any keys.\n"
        f"Do not add explanations or markdown fences."
    )


def build_translation_payload(
    event_title: str,
    event_description: str,
    event_location_text: str,
    artifacts: list[dict[str, str | None]],
) -> dict[str, Any]:
    """Build the JSON payload sent to the LLM for translation.

    Args:
        event_title: Event title in source language.
        event_description: Event description in source language.
        event_location_text: Location text in source language.
        artifacts: List of artifact dicts, each with keys id,
            extracted_text, deadline, requirements, location, fees.

    Returns:
        Dict with ``event`` and ``artifacts`` keys.
    """
    return {
        "event": {
            "title": event_title,
            "description": event_description,
            "location_text": event_location_text,
        },
        "artifacts": [
            {
                "id": a["id"],
                "extracted_text": a.get("extracted_text"),
                "deadline": a.get("deadline"),
                "requirements": a.get("requirements"),
                "location": a.get("location"),
                "fees": a.get("fees"),
            }
            for a in artifacts
        ],
    }


def _extract_str(data: dict[str, Any], key: str) -> str | None:
    val = data.get(key)
    return val if isinstance(val, str) else None


def _parse_ts(value: datetime | str) -> datetime:
    """Parse a Delta-stored timestamp string or pass through a datetime."""
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def parse_translation_response(
    content: str,
    artifact_ids: list[str],
) -> dict[str, Any]:
    """Parse the LLM translation response.

    Strips optional markdown fences then parses JSON.  Returns a dict::

        {
            "event": {"title": ..., "description": ..., "location_text": ...},
            "artifacts": [
                {"id": ..., "extracted_text": ..., "deadline": ...,
                 "requirements": ..., "location": ..., "fees": ...},
                ...
            ],
        }

    Missing or non-string values default to None.  Artifacts are matched by
    ``id``; any artifact ID absent from the response gets all-None fields.

    Args:
        content: Raw LLM response, optionally wrapped in ```json fences.
        artifact_ids: IDs of artifacts included in the original payload,
            used to fill in defaults for any missing response entries.

    Returns:
        Parsed translation dict.
    """
    cleaned = re.sub(r"```(?:json)?\s*", "", content).strip().rstrip("`").strip()
    data: dict[str, Any] = json.loads(cleaned)

    raw_event: dict[str, Any] = data.get("event") or {}
    event_out = {f: _extract_str(raw_event, f) for f in _EVENT_FIELDS}

    raw_artifacts: list[dict[str, Any]] = data.get("artifacts") or []
    by_id: dict[str, dict[str, Any]] = {
        str(a.get("id", "")): a for a in raw_artifacts if isinstance(a, dict)
    }
    artifacts_out = []
    for art_id in artifact_ids:
        raw = by_id.get(art_id, {})
        artifacts_out.append(
            {
                "id": art_id,
                **{f: _extract_str(raw, f) for f in _ARTIFACT_FIELDS},
            }
        )

    return {"event": event_out, "artifacts": artifacts_out}


def make_event_details_translated(
    fingerprint: str,
    url: str,
    source: str,
    category: str,
    title_original: str,
    description_original: str,
    location_text_original: str,
    date_start: datetime | str | None,
    date_end: datetime | str | None,
    lat: float | None,
    lng: float | None,
    country: str | None,
    query_country: str | None,
    domain_country: str | None,
    language: str,
    target_language: str,
    artifact_urls: list[str],
    ingested_at: datetime | str,
    translated_title: str | None,
    translated_description: str | None,
    translated_location_text: str | None,
) -> EventDetailsTranslated:
    """Construct an EventDetailsTranslated; fall back to originals on null translation."""
    return EventDetailsTranslated(
        fingerprint=fingerprint,
        url=url,  # type: ignore[arg-type]
        source=source,
        category=category,
        title_original=title_original,
        description_original=description_original,
        location_text_original=location_text_original,
        title=translated_title or title_original,
        description=translated_description or description_original,
        location_text=translated_location_text or location_text_original,
        date_start=_parse_ts(date_start) if date_start is not None else None,
        date_end=_parse_ts(date_end) if date_end is not None else None,
        lat=lat,
        lng=lng,
        country=country,
        query_country=query_country,
        domain_country=domain_country,
        language=language,
        target_language=target_language,
        artifact_urls=artifact_urls,
        ingested_at=_parse_ts(ingested_at),
    )


def make_event_artifacts_details_translated(
    artifact_id: str,
    event_id: str,
    artifact_type: str,
    file_path: str,
    extracted_text_original: str | None,
    processed_at: datetime | str,
    target_language: str,
    translated_extracted_text: str | None,
    translated_deadline: str | None,
    translated_requirements: str | None,
    translated_location: str | None,
    translated_fees: str | None,
) -> EventArtifactsTranslated:
    """Construct an EventArtifactsTranslated; fall back to originals for null fields."""
    return EventArtifactsTranslated(
        id=artifact_id,
        event_id=event_id,
        artifact_type=artifact_type,
        file_path=file_path,
        extracted_text_original=extracted_text_original,
        extracted_text=translated_extracted_text or extracted_text_original,
        deadline=translated_deadline,
        requirements=translated_requirements,
        location=translated_location,
        fees=translated_fees,
        target_language=target_language,
        processed_at=_parse_ts(processed_at),
    )


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


def load_target_language(keywords_path: str) -> str:
    """Read target_language from the keywords YAML config.

    Args:
        keywords_path: Path to ``config/input/keywords.yml``.

    Returns:
        Target language code, defaults to ``"en"`` if not set.
    """
    with open(keywords_path) as fh:
        cfg = yaml.safe_load(fh) or {}
    return str(cfg.get("target_language", "EN"))


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
def _translate_text(
    payload: dict[str, Any],
    client: OpenAI,
    model: str,
    target_language: str,
) -> dict[str, Any]:
    """Send the translation payload to the LLM and return parsed translations.

    Args:
        payload: Dict built by :func:`build_translation_payload`.
        client: OpenAI client pointed at Databricks serving endpoints.
        model: Model name.
        target_language: Target language for translation.

    Returns:
        Parsed translation dict from :func:`parse_translation_response`.
    """
    artifact_ids = [a["id"] for a in payload.get("artifacts", [])]
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": build_system_prompt(target_language)},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ],
        temperature=0.0,
        max_tokens=4096,
    )
    raw = response.choices[0].message.content or ""
    return parse_translation_response(raw, artifact_ids)


# ---------------------------------------------------------------------------
# Spark integration (pragma: no cover — tested via integration marker)
# ---------------------------------------------------------------------------


def _event_details_translated_schema() -> StructType:  # pragma: no cover
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
            StructField("category", StringType(), False),
            StructField("title_original", StringType(), False),
            StructField("description_original", StringType(), False),
            StructField("location_text_original", StringType(), False),
            StructField("title", StringType(), False),
            StructField("description", StringType(), False),
            StructField("location_text", StringType(), False),
            StructField("date_start", TimestampType(), True),
            StructField("date_end", TimestampType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lng", DoubleType(), True),
            StructField("country", StringType(), True),
            StructField("query_country", StringType(), True),
            StructField("domain_country", StringType(), True),
            StructField("language", StringType(), False),
            StructField("target_language", StringType(), False),
            StructField("artifact_urls", ArrayType(StringType()), False),
            StructField("ingested_at", TimestampType(), False),
            StructField("translated_at", TimestampType(), False),
            StructField("processing_status", StringType(), False),
        ]
    )


def _event_artifacts_details_translated_schema() -> StructType:  # pragma: no cover
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("id", StringType(), False),
            StructField("event_id", StringType(), False),
            StructField("artifact_type", StringType(), False),
            StructField("file_path", StringType(), False),
            StructField("extracted_text_original", StringType(), True),
            StructField("extracted_text", StringType(), True),
            StructField("deadline", StringType(), True),
            StructField("requirements", StringType(), True),
            StructField("location", StringType(), True),
            StructField("fees", StringType(), True),
            StructField("target_language", StringType(), False),
            StructField("processing_status", StringType(), False),
            StructField("processed_at", TimestampType(), False),
            StructField("translated_at", TimestampType(), False),
        ]
    )


def _write_event_details_translated(  # pragma: no cover
    spark: SparkSession,
    event: EventDetailsTranslated,
    table: str,
) -> None:
    schema = _event_details_translated_schema()
    row = event.model_dump(mode="python")
    row["url"] = str(row["url"])

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


def _write_event_artifacts_details_translated(  # pragma: no cover
    spark: SparkSession,
    artifact: EventArtifactsTranslated,
    table: str,
) -> None:
    schema = _event_artifacts_details_translated_schema()
    row = artifact.model_dump(mode="python")

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
    event_details_table: str,
    event_details_translated_table: str,
    limit: int = 0,
) -> list[str]:
    """Emit fingerprints of events not yet translated as a Databricks task value.

    Reads ``event_details`` where ``category NOT IN ('non_art')`` and anti-joins
    with ``event_details_translated`` so events are not translated twice.

    Returns the list of fingerprints to translate.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

    pending_df = (
        spark.table(event_details_table)
        .filter(F.col("category") != "non_art")
        .select("fingerprint")
        .distinct()
    )

    if spark.catalog.tableExists(event_details_translated_table):
        done_df = (
            spark.table(event_details_translated_table)
            .filter(F.col("processing_status") == ProcessingStatus.DONE.value)
            .select("fingerprint")
        )
        pending_df = pending_df.join(done_df, on="fingerprint", how="left_anti")
    else:
        logger.info(
            "event_details_translated table does not exist yet — all events are new"
        )

    if limit > 0:
        pending_df = pending_df.limit(limit)

    fingerprints: list[str] = [row["fingerprint"] for row in pending_df.collect()]
    logger.info("Event fingerprints to translate: {}", len(fingerprints))

    try:
        from databricks.sdk.runtime import dbutils

        dbutils.jobs.taskValues.set(key="fingerprints", value=fingerprints)
        logger.info("Task value 'fingerprints' set with {} entries", len(fingerprints))
    except ImportError:
        logger.warning("dbutils not available — skipping task value set")

    return fingerprints


def run_translate(  # pragma: no cover
    fingerprint: str,
    event_details_table: str,
    event_artifacts_details_table: str,
    event_details_translated_table: str,
    event_artifacts_details_translated_table: str,
    target_language: str,
    model: str = _DEFAULT_MODEL,
) -> None:
    """Translate one event and its artifacts, writing to the silver translated tables.

    Reads the EventDetails record from ``event_details`` and all
    EventArtifactsProcessed rows for that event from
    ``event_artifacts_details``.  If the event language matches the target
    language, fields are promoted without an LLM call.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

    # -- Read event -----------------------------------------------------------
    event_rows = (
        spark.table(event_details_table)
        .filter(F.col("fingerprint") == fingerprint)
        .limit(1)
        .collect()
    )
    if not event_rows:
        logger.warning("No event_details found for fingerprint: {}", fingerprint)
        return
    ev = event_rows[0]

    # -- Read artifacts -------------------------------------------------------
    artifact_rows: list[Any] = []
    if spark.catalog.tableExists(event_artifacts_details_table):
        artifact_rows = (
            spark.table(event_artifacts_details_table)
            .filter(
                (F.col("event_id") == fingerprint)
                & (F.col("processing_status") == ProcessingStatus.DONE.value)
            )
            .collect()
        )

    artifacts_payload = [
        {
            "id": a["id"],
            "extracted_text": a["extracted_text"],
            "deadline": a["deadline"],
            "requirements": a["requirements"],
            "location": a["location"],
            "fees": a["fees"],
        }
        for a in artifact_rows
    ]

    # -- Translate or copy ----------------------------------------------------
    source_language: str = ev["language"] or ""
    translation: dict[str, Any] = {"event": {}, "artifacts": []}

    if source_language == target_language:
        logger.info(
            "Event {} is already in {} — promoting without translation",
            fingerprint,
            target_language,
        )
    else:
        try:
            client = _create_default_client()
            payload = build_translation_payload(
                event_title=ev["title"],
                event_description=ev["description"],
                event_location_text=ev["location_text"],
                artifacts=artifacts_payload,
            )
            translation = _translate_text(payload, client, model, target_language)
            logger.info("Translation complete for fingerprint: {}", fingerprint)
        except Exception as exc:
            logger.error(
                "Translation failed for fingerprint {} — "
                "promoting with original text: {}",
                fingerprint,
                exc,
            )

    event_translation: dict[str, str | None] = translation.get("event") or {}
    artifact_translations: dict[str, dict[str, str | None]] = {
        a["id"]: a for a in (translation.get("artifacts") or [])
    }

    # -- Write silver.event_details_translated --------------------------------
    translated_event = make_event_details_translated(
        fingerprint=fingerprint,
        url=str(ev["url"]),
        source=ev["source"],
        category=ev["category"],
        title_original=ev["title"],
        description_original=ev["description"],
        location_text_original=ev["location_text"],
        date_start=ev["date_start"],
        date_end=ev["date_end"],
        lat=ev["lat"],
        lng=ev["lng"],
        country=ev["country"],
        query_country=ev["query_country"],
        domain_country=ev["domain_country"],
        language=source_language,
        target_language=target_language,
        artifact_urls=list(ev["artifact_urls"] or []),
        ingested_at=ev["ingested_at"],
        translated_title=event_translation.get("title"),
        translated_description=event_translation.get("description"),
        translated_location_text=event_translation.get("location_text"),
    )
    _write_event_details_translated(
        spark, translated_event, event_details_translated_table
    )
    logger.info("Wrote EventDetailsTranslated for fingerprint: {}", fingerprint)

    # -- Write silver.event_artifacts_details_translated ------------------------------
    for art in artifact_rows:
        art_translation = artifact_translations.get(art["id"], {})
        translated_artifact = make_event_artifacts_details_translated(
            artifact_id=art["id"],
            event_id=art["event_id"],
            artifact_type=art["artifact_type"],
            file_path=art["file_path"],
            extracted_text_original=art["extracted_text"],
            processed_at=art["processed_at"],
            target_language=target_language,
            translated_extracted_text=art_translation.get("extracted_text"),
            translated_deadline=art_translation.get("deadline"),
            translated_requirements=art_translation.get("requirements"),
            translated_location=art_translation.get("location"),
            translated_fees=art_translation.get("fees"),
        )
        _write_event_artifacts_details_translated(
            spark, translated_artifact, event_artifacts_details_translated_table
        )
        logger.info(
            "Wrote EventArtifactsTranslated id: {} for fingerprint: {}",
            art["id"],
            fingerprint,
        )


def main() -> None:  # pragma: no cover
    """Entry point for artlake-translate wheel task.

    Two modes:
      list      — Emit fingerprints of untranslated events as a Databricks
                  task value for a downstream for_each_task.
      translate — Translate one event and its artifacts (for_each inner task).
    """
    import argparse

    parser = argparse.ArgumentParser(description="ArtLake content translator")
    parser.add_argument(
        "--mode",
        choices=["list", "translate"],
        required=True,
        help="'list' emits fingerprints as a task value; 'translate' processes one event",
    )
    parser.add_argument(
        "--fingerprint",
        help="Event fingerprint to translate (required for --mode translate)",
    )
    parser.add_argument(
        "--keywords",
        required=True,
        help="Path to config/input/keywords.yml (provides target_language)",
    )
    parser.add_argument(
        "--event-details-table",
        default=_EVENT_DETAILS_TABLE_DEFAULT,
        help="Fully-qualified silver.event_details Delta table",
    )
    parser.add_argument(
        "--event-artifacts-processed-table",
        default=_EVENT_ARTIFACTS_PROCESSED_TABLE_DEFAULT,
        help="Fully-qualified silver.event_artifacts_details Delta table",
    )
    parser.add_argument(
        "--event-details-translated-table",
        default=_EVENT_DETAILS_TRANSLATED_TABLE_DEFAULT,
        help="Fully-qualified silver.event_details_translated Delta table",
    )
    parser.add_argument(
        "--event-artifacts-translated-table",
        default=_EVENT_ARTIFACTS_TRANSLATED_TABLE_DEFAULT,
        help="Fully-qualified silver.event_artifacts_details_translated Delta table",
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
        help="Max fingerprints to emit in list mode (0 = no limit)",
    )
    parser.add_argument(
        "--env",
        default="dev",
        help="Deployment environment (dev/tst/acc/prd)",
    )
    args = parser.parse_args()

    target_language = load_target_language(args.keywords)

    if args.mode == "list":
        run_list(
            event_details_table=args.event_details_table,
            event_details_translated_table=args.event_details_translated_table,
            limit=args.limit,
        )
    else:
        if not args.fingerprint:
            parser.error("--fingerprint is required for --mode translate")
        run_translate(
            fingerprint=args.fingerprint,
            event_details_table=args.event_details_table,
            event_artifacts_details_table=args.event_artifacts_details_table,
            event_details_translated_table=args.event_details_translated_table,
            event_artifacts_details_translated_table=args.event_artifacts_details_translated_table,
            target_language=target_language,
            model=args.model,
        )

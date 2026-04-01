"""Artifact downloader — PDFs and images to Unity Catalog Volumes.

Entry point: artlake-download-artifacts

Reads artifact_urls from artlake.bronze.event_dates where event_status IN
('future', 'undefined'), downloads files to UC Volume, and writes EventArtifact
records to bronze.event_artifacts.
"""

from __future__ import annotations

import hashlib
from io import BytesIO
from pathlib import PurePosixPath
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import requests
from loguru import logger

from artlake.events.scrape import fingerprint as url_fingerprint
from artlake.models.event import EventArtifact, ProcessingStatus

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_USER_AGENT = "artlake-scraper/1.0"
_TIMEOUT = 30
_MAX_BYTES_DEFAULT = 50 * 1024 * 1024  # 50 MB
_CHUNK_SIZE = 65_536
_VOLUME_ROOT_DEFAULT = "/Volumes/artlake/volumes/event_artifacts"


# ---------------------------------------------------------------------------
# Pure-Python helpers
# ---------------------------------------------------------------------------


def content_hash(data: bytes) -> str:
    """SHA256 hex digest of file content — used for deduplication."""
    return hashlib.sha256(data).hexdigest()


def detect_artifact_type(url: str, content_type: str | None = None) -> str:
    """Infer artifact type from URL extension or HTTP Content-Type.

    Returns 'pdf' or 'image'.
    """
    ext = PurePosixPath(urlparse(url).path).suffix.lower()
    if ext == ".pdf" or (content_type and "pdf" in content_type):
        return "pdf"
    return "image"


def volume_path(volume_root: str, event_id: str, url: str) -> str:
    """Build the UC Volume path for an artifact.

    Format: {volume_root}/{event_id}/{filename}

    When the URL has no filename component, the artifact URL fingerprint is used
    as the filename.
    """
    filename = PurePosixPath(urlparse(url).path).name or url_fingerprint(url)
    return f"{volume_root.rstrip('/')}/{event_id}/{filename}"


def download_artifact(
    url: str,
    max_bytes: int = _MAX_BYTES_DEFAULT,
    timeout: int = _TIMEOUT,
) -> tuple[bytes | None, str | None, str | None]:
    """Fetch a binary artifact from *url*.

    Returns (data, content_type, error).
    data is None when the download fails or the file exceeds max_bytes.
    """
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": _USER_AGENT},
            timeout=timeout,
            stream=True,
        )
        resp.raise_for_status()

        content_length = resp.headers.get("Content-Length")
        if content_length and int(content_length) > max_bytes:
            msg = f"File too large: {content_length} bytes > {max_bytes}"
            logger.warning("Skipping {}: {}", url, msg)
            return None, None, msg

        data = b""
        for chunk in resp.iter_content(chunk_size=_CHUNK_SIZE):
            data += chunk
            if len(data) > max_bytes:
                msg = f"File too large: exceeded {max_bytes} bytes mid-stream"
                logger.warning("Skipping {}: {}", url, msg)
                return None, None, msg

        content_type = resp.headers.get("Content-Type", "").split(";")[0].strip() or None
        return data, content_type, None

    except requests.RequestException as exc:
        error = f"{type(exc).__name__}: {exc}"
        logger.warning("Failed to download {}: {}", url, error)
        return None, None, error


def make_artifact(
    url: str,
    event_id: str,
    artifact_type: str,
    file_path: str | None,
    status: ProcessingStatus,
    content_hash_val: str | None = None,
) -> EventArtifact:
    """Construct an EventArtifact record from download results."""
    from pydantic import HttpUrl as PydanticHttpUrl

    return EventArtifact(
        id=url_fingerprint(url),
        event_id=event_id,
        url=PydanticHttpUrl(url),
        artifact_type=artifact_type,
        content_hash=content_hash_val,
        file_path=file_path,
        processing_status=status,
    )


# ---------------------------------------------------------------------------
# Spark integration (pragma: no cover — tested via integration marker)
# ---------------------------------------------------------------------------


def _artifact_schema() -> StructType:  # pragma: no cover
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType(
        [
            StructField("id", StringType(), False),
            StructField("event_id", StringType(), False),
            StructField("url", StringType(), False),
            StructField("artifact_type", StringType(), False),
            StructField("content_hash", StringType(), True),
            StructField("file_path", StringType(), True),
            StructField("processing_status", StringType(), False),
            StructField("ingested_at", StringType(), False),
        ]
    )


def _ensure_volume(spark: SparkSession, volume_root: str) -> None:  # pragma: no cover
    """Create the UC schema and Volume if they don't exist (first-run setup).

    Parses catalog/schema/volume from a path of the form
    /Volumes/{catalog}/{schema}/{volume}[/...].
    """
    parts = volume_root.strip("/").split("/")
    if len(parts) < 4 or parts[0].lower() != "volumes":
        logger.warning("Cannot auto-create volume — unexpected path: {}", volume_root)
        return
    catalog, schema, volume = parts[1], parts[2], parts[3]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
    logger.info("Ensured UC Volume {}.{}.{}", catalog, schema, volume)


def _write_artifact(  # pragma: no cover
    spark: SparkSession,
    artifact: EventArtifact,
    artifacts_table: str,
) -> None:
    schema = _artifact_schema()
    row = artifact.model_dump(mode="json")
    row["url"] = str(row["url"])
    row["ingested_at"] = str(row["ingested_at"])

    df = spark.createDataFrame([row], schema=schema)

    parts = artifacts_table.split(".")
    if len(parts) == 3:
        catalog, db, _ = parts
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db}")

    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(artifacts_table)
    )


def run_list(  # pragma: no cover
    event_dates_table: str,
    event_artifacts_table: str,
    limit: int = 0,
) -> list[str]:
    """Explode artifact_urls from future/undefined events.

    Emits unseen URLs as a task value.

    Pipeline gate: only events with event_status IN ('future', 'undefined') have
    their artifacts downloaded.  Anti-joins against event_artifacts so
    already-downloaded URLs are skipped.
    Returns the list of artifact URLs to process.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

    events_df = (
        spark.table(event_dates_table)
        .filter(F.col("event_status").isin("future", "undefined"))
        .select(F.explode("artifact_urls").alias("artifact_url"))
        .filter(F.col("artifact_url") != "")
        .distinct()
    )

    _TERMINAL_STATUSES = [ProcessingStatus.DOWNLOADED, ProcessingStatus.FAILED]

    if spark.catalog.tableExists(event_artifacts_table):
        terminal_df = (
            spark.table(event_artifacts_table)
            .filter(
                F.col("processing_status").isin([s.value for s in _TERMINAL_STATUSES])
            )
            .select(F.col("url").alias("artifact_url"))
        )
        events_df = events_df.join(terminal_df, on="artifact_url", how="left_anti")
    else:
        logger.info(
            "event_artifacts table does not exist yet — all artifact URLs are unseen"
        )

    if limit > 0:
        events_df = events_df.limit(limit)

    urls: list[str] = [row["artifact_url"] for row in events_df.collect()]
    logger.info("Artifact URLs to download: {}", len(urls))

    try:
        from databricks.sdk.runtime import dbutils

        dbutils.jobs.taskValues.set(key="urls", value=urls)
        logger.info("Task value 'urls' set with {} entries", len(urls))
    except ImportError:
        logger.warning("dbutils not available — skipping task value set")

    return urls


def run_download(  # pragma: no cover
    artifact_url: str,
    event_dates_table: str,
    event_artifacts_table: str,
    volume_root: str = _VOLUME_ROOT_DEFAULT,
    max_bytes: int = _MAX_BYTES_DEFAULT,
    env: str = "dev",
) -> None:
    """Download one artifact and write an EventArtifact record to bronze.event_artifacts.

    Looks up the parent event fingerprint from event_dates.
    Applies content-hash deduplication — skips identical files already stored.
    The record is only written after a successful volume upload, so a mid-download
    crash leaves no record and the URL will be retried on the next run.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

    _ensure_volume(spark, volume_root)

    # Resolve parent event URL from event_dates
    rows = (
        spark.table(event_dates_table)
        .filter(F.array_contains(F.col("artifact_urls"), artifact_url))
        .select("url")
        .limit(1)
        .collect()
    )
    if not rows:
        logger.warning("No matching event_date found for artifact URL: {}", artifact_url)
        return

    event_id: str = url_fingerprint(rows[0]["url"])
    artifact_type = detect_artifact_type(artifact_url)

    data, content_type, error = download_artifact(artifact_url, max_bytes=max_bytes)

    if data is None:
        artifact = make_artifact(
            artifact_url, event_id, artifact_type, None, ProcessingStatus.FAILED
        )
        _write_artifact(spark, artifact, event_artifacts_table)
        logger.warning(
            "Wrote failed EventArtifact for {} — error: {}", artifact_url, error
        )
        return

    hash_val = content_hash(data)

    # Content-hash dedup
    if spark.catalog.tableExists(event_artifacts_table):
        existing = (
            spark.table(event_artifacts_table)
            .filter(F.col("content_hash") == hash_val)
            .limit(1)
            .collect()
        )
        if existing:
            logger.info(
                "Skipping duplicate content (hash {}): {}", hash_val, artifact_url
            )
            return

    # Upload to UC Volume
    artifact_type = detect_artifact_type(artifact_url, content_type)
    path = volume_path(volume_root, event_id, artifact_url)

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    w.files.upload(file_path=path, contents=BytesIO(data), overwrite=True)
    logger.info("Uploaded {} → {}", artifact_url, path)

    artifact = make_artifact(
        artifact_url,
        event_id,
        artifact_type,
        path,
        ProcessingStatus.DOWNLOADED,
        hash_val,
    )
    _write_artifact(spark, artifact, event_artifacts_table)
    logger.info("Wrote EventArtifact for {} to {}", artifact_url, event_artifacts_table)


def main() -> None:  # pragma: no cover
    """Entry point for artlake-download-artifacts wheel task.

    Two modes:
      list     — Explode artifact_urls from future/undefined events and emit the
                 list as a Databricks task value for a downstream for_each_task.
      download — Download a single artifact URL (for_each inner task).
    """
    import argparse

    parser = argparse.ArgumentParser(description="ArtLake artifact downloader")
    parser.add_argument(
        "--mode",
        choices=["list", "download"],
        required=True,
        help="'list' emits unseen artifact URLs as a task value; "
        "'download' fetches one URL",
    )
    parser.add_argument(
        "--artifact-url",
        help="Artifact URL to download (required for --mode download)",
    )
    parser.add_argument(
        "--event-dates-table",
        default="artlake.bronze.event_dates",
        help="Fully-qualified bronze.event_dates Delta table",
    )
    parser.add_argument(
        "--event-artifacts-table",
        default="artlake.bronze.event_artifacts",
        help="Fully-qualified bronze.event_artifacts Delta table",
    )
    parser.add_argument(
        "--volume-root",
        default=_VOLUME_ROOT_DEFAULT,
        help="UC Volume root path for downloaded artifacts",
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=_MAX_BYTES_DEFAULT,
        help="Maximum file size to download in bytes (default: 50 MB)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max artifact URLs to emit in list mode (0 = no limit)",
    )
    parser.add_argument(
        "--env",
        default="dev",
        help="Deployment environment (dev/tst/acc/prd)",
    )
    args = parser.parse_args()

    if args.mode == "list":
        run_list(
            event_dates_table=args.event_dates_table,
            event_artifacts_table=args.event_artifacts_table,
            limit=args.limit,
        )
    else:
        if not args.artifact_url:
            parser.error("--artifact-url is required for --mode download")
        run_download(
            artifact_url=args.artifact_url,
            event_dates_table=args.event_dates_table,
            event_artifacts_table=args.event_artifacts_table,
            volume_root=args.volume_root,
            max_bytes=args.max_bytes,
            env=args.env,
        )

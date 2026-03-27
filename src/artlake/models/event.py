"""Event and artifact data models."""

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


def _now() -> datetime:
    return datetime.now(UTC)


class ProcessingStatus(StrEnum):
    """Row-level processing status for staging models."""

    NEW = "new"
    PROCESSING = "processing"
    DOWNLOADED = "downloaded"
    DONE = "done"
    FAILED = "failed"
    OUTDATED = "outdated"
    REQUIRES_MANUAL_VALIDATION = "requires_manual_validation"


class RawEvent(BaseModel):
    """Raw search result written to staging.search_results."""

    model_config = ConfigDict(strict=True)

    fingerprint: str
    url: HttpUrl
    title: str
    snippet: str
    source: str
    raw_html: str | None = None
    scraped_at: datetime | None = None
    language: str
    artifact_urls: list[str] = []
    ingested_at: datetime = Field(default_factory=_now)


class CleanEvent(BaseModel):
    """Structured event written to bronze.raw_events."""

    model_config = ConfigDict(strict=True)

    fingerprint: str
    title: str
    description: str
    date_start: datetime | None = None
    date_end: datetime | None = None
    location_text: str
    lat: float | None = None
    lng: float | None = None
    country: str | None = None
    language: str
    source: str
    url: HttpUrl
    artifact_urls: list[str] = []
    artifact_paths: list[str] = []
    processing_status: ProcessingStatus = ProcessingStatus.NEW
    ingested_at: datetime = Field(default_factory=_now)


class GoldEvent(BaseModel):
    """Enriched event written to gold.events."""

    model_config = ConfigDict(strict=True)

    title: str
    description: str
    date_start: datetime | None = None
    date_end: datetime | None = None
    location_text: str
    lat: float | None = None
    lng: float | None = None
    country: str | None = None
    language: str
    source: str
    url: HttpUrl
    artifact_paths: list[str] = []
    category: str
    artifact_summaries: list[str] = []
    ingested_at: datetime = Field(default_factory=_now)


class EventArtifact(BaseModel):
    """Artifact metadata written to bronze.artifacts."""

    model_config = ConfigDict(strict=True)

    fingerprint: str
    event_fingerprint: str
    url: HttpUrl
    artifact_type: str
    content_hash: str | None = None
    file_path: str | None = None
    processing_status: ProcessingStatus = ProcessingStatus.NEW
    ingested_at: datetime = Field(default_factory=_now)


class SeenUrl(BaseModel):
    """Dedup tracker written to staging.seen_urls."""

    model_config = ConfigDict(strict=True)

    url: HttpUrl
    title: str
    source: str
    fingerprint: str
    ingested_at: datetime = Field(default_factory=_now)


class ScrapedPage(BaseModel):
    """Raw scraped page written to staging.scraped_pages."""

    model_config = ConfigDict(strict=True)

    fingerprint: str
    url: HttpUrl
    title: str
    raw_text: str
    artifact_urls: list[str] = []
    processing_status: ProcessingStatus = ProcessingStatus.NEW
    robots_allowed: bool | None = None
    error: str | None = None
    scraped_at: datetime = Field(default_factory=_now)

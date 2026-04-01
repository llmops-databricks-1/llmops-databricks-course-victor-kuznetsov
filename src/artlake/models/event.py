"""Event and artifact data models."""

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


def _now() -> datetime:
    return datetime.now(UTC)


# ---------------------------------------------------------------------------
# Status enums
# ---------------------------------------------------------------------------


class EventStatus(StrEnum):
    """Date-based status for bronze.event_dates."""

    FUTURE = "future"
    FINISHED = "finished"
    UNDEFINED = "undefined"


class LocationStatus(StrEnum):
    """Geocoding resolution status for bronze.event_location."""

    IDENTIFIED = "identified"
    MISSING = "missing"
    REQUIRES_VALIDATION = "requires_validation"


class CategoryStatus(StrEnum):
    """Classification status for bronze.event_category."""

    IDENTIFIED = "identified"
    MISSING = "missing"
    REQUIRES_VALIDATION = "requires_validation"


class ProcessingStatus(StrEnum):
    """Row-level processing status for staging models and artifacts."""

    NEW = "new"
    PROCESSING = "processing"
    DOWNLOADED = "downloaded"
    DONE = "done"
    FAILED = "failed"


# ---------------------------------------------------------------------------
# Staging models
# ---------------------------------------------------------------------------


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
    query_country: str | None = None
    artifact_urls: list[str] = []
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


# ---------------------------------------------------------------------------
# Bronze models
# ---------------------------------------------------------------------------


class EventDate(BaseModel):
    """Structured event written to bronze.event_dates."""

    model_config = ConfigDict(strict=True)

    fingerprint: str
    title: str
    description: str
    date_start: datetime | None = None
    date_end: datetime | None = None
    location_text: str
    query_country: str | None = None
    domain_country: str | None = None
    language: str
    source: str
    url: HttpUrl
    artifact_urls: list[str] = []
    event_status: EventStatus = EventStatus.UNDEFINED
    ingested_at: datetime = Field(default_factory=_now)


class EventLocation(BaseModel):
    """Geocoded location written to bronze.event_location."""

    model_config = ConfigDict(strict=True)

    fingerprint: str
    location_text: str
    lat: float | None = None
    lng: float | None = None
    country: str | None = None
    location_status: LocationStatus = LocationStatus.MISSING


class EventCategory(BaseModel):
    """Classified category written to bronze.event_category."""

    model_config = ConfigDict(strict=True)

    fingerprint: str
    category: str | None = None
    category_status: CategoryStatus = CategoryStatus.MISSING


class EventArtifact(BaseModel):
    """Artifact metadata written to bronze.event_artifacts."""

    model_config = ConfigDict(strict=True)

    id: str
    event_id: str
    url: HttpUrl
    artifact_type: str
    content_hash: str | None = None
    file_path: str | None = None
    processing_status: ProcessingStatus = ProcessingStatus.NEW
    ingested_at: datetime = Field(default_factory=_now)


class RawEventArtifact(BaseModel):
    """Extracted text from artifact written to bronze.event_artifacts_text."""

    model_config = ConfigDict(strict=True)

    id: str
    event_id: str
    artifact_type: str
    file_path: str
    extracted_text: str | None = None
    processing_status: ProcessingStatus = ProcessingStatus.NEW
    processed_at: datetime = Field(default_factory=_now)


# ---------------------------------------------------------------------------
# Silver models
# ---------------------------------------------------------------------------


class EventDetails(BaseModel):
    """Joined event record written to silver.event_details."""

    model_config = ConfigDict(strict=True)

    fingerprint: str
    url: HttpUrl
    source: str
    language: str
    query_country: str | None = None
    domain_country: str | None = None

    # From bronze.event_dates
    title: str
    description: str
    date_start: datetime | None = None
    date_end: datetime | None = None
    location_text: str
    event_status: EventStatus

    # From bronze.event_location
    lat: float | None = None
    lng: float | None = None
    country: str | None = None
    location_status: LocationStatus

    # From bronze.event_category
    category: str | None = None
    category_status: CategoryStatus

    artifact_urls: list[str] = []
    ingested_at: datetime


class EventArtifactsProcessed(BaseModel):
    """LLM-extracted artifact fields written to silver.event_artifacts_details."""

    model_config = ConfigDict(strict=True)

    id: str
    event_id: str
    artifact_type: str
    file_path: str
    extracted_text: str | None = None
    deadline: str | None = None
    requirements: str | None = None
    location: str | None = None
    fees: str | None = None
    processing_status: ProcessingStatus = ProcessingStatus.NEW
    processed_at: datetime = Field(default_factory=_now)


class EventDetailsTranslated(BaseModel):
    """Translated event written to silver.event_details_translated."""

    model_config = ConfigDict(strict=True)

    fingerprint: str
    url: HttpUrl
    source: str
    category: str

    # Original text (source language)
    title_original: str
    description_original: str
    location_text_original: str

    # Translated text (target language)
    title: str
    description: str
    location_text: str

    # Date & geo
    date_start: datetime | None = None
    date_end: datetime | None = None
    lat: float | None = None
    lng: float | None = None
    country: str | None = None
    query_country: str | None = None
    domain_country: str | None = None

    # Language
    language: str
    target_language: str

    artifact_urls: list[str] = []
    ingested_at: datetime
    translated_at: datetime = Field(default_factory=_now)
    processing_status: ProcessingStatus = ProcessingStatus.DONE


class EventArtifactsTranslated(BaseModel):
    """Translated artifact written to silver.event_artifacts_details_translated."""

    model_config = ConfigDict(strict=True)

    id: str
    event_id: str
    artifact_type: str
    file_path: str

    # Original text (source language)
    extracted_text_original: str | None = None

    # Translated text (target language)
    extracted_text: str | None = None
    deadline: str | None = None
    requirements: str | None = None
    location: str | None = None
    fees: str | None = None

    target_language: str
    processing_status: ProcessingStatus = ProcessingStatus.DONE
    processed_at: datetime
    translated_at: datetime = Field(default_factory=_now)

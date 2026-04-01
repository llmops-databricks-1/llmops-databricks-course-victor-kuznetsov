"""Tests for ArtLake Pydantic data models."""

from datetime import UTC, datetime

import pytest
from pydantic import HttpUrl, ValidationError

from artlake.models import (
    ArtLakeConfig,
    EventArtifact,
    EventDate,
    EventStatus,
    ProcessingStatus,
    RawEvent,
    SeenUrl,
)

# ---------------------------------------------------------------------------
# RawEvent
# ---------------------------------------------------------------------------


class TestRawEvent:
    def test_valid_minimal(self) -> None:
        event = RawEvent(
            fingerprint="abc123",
            url="https://example.com/event",
            title="Open Call",
            snippet="Apply now",
            source="duckduckgo",
            language="en",
        )
        assert event.artifact_urls == []
        assert event.raw_html is None

    def test_valid_full(self) -> None:
        event = RawEvent(
            fingerprint="abc123",
            url="https://example.com/event",
            title="Open Call",
            snippet="Apply now",
            source="duckduckgo",
            raw_html="<html></html>",
            scraped_at=datetime(2026, 1, 1, tzinfo=UTC),
            language="nl",
            artifact_urls=["https://example.com/file.pdf"],
        )
        assert len(event.artifact_urls) == 1

    def test_invalid_url(self) -> None:
        with pytest.raises(ValidationError):
            RawEvent(
                fingerprint="abc123",
                url="not-a-url",
                title="Open Call",
                snippet="Apply now",
                source="duckduckgo",
                language="en",
            )

    def test_missing_required_field(self) -> None:
        with pytest.raises(ValidationError):
            RawEvent(
                fingerprint="abc123",
                url="https://example.com/event",
                title="Open Call",
                # missing snippet, source, language
            )


# ---------------------------------------------------------------------------
# EventDate
# ---------------------------------------------------------------------------


class TestEventDate:
    def test_valid(self) -> None:
        event = EventDate(
            fingerprint="abc123",
            title="Art Market",
            description="Annual art market in Amsterdam",
            location_text="Amsterdam, Netherlands",
            language="en",
            source="duckduckgo",
            url="https://example.com/market",
        )
        assert event.date_start is None
        assert event.artifact_urls == []
        assert event.event_status == EventStatus.UNDEFINED

    def test_with_dates_and_query_country(self) -> None:
        event = EventDate(
            fingerprint="abc123",
            title="Art Market",
            description="Annual art market",
            date_start=datetime(2026, 6, 1, tzinfo=UTC),
            date_end=datetime(2026, 6, 3, tzinfo=UTC),
            location_text="Amsterdam",
            query_country="NL",
            domain_country="NL",
            language="en",
            source="duckduckgo",
            url="https://example.com/market",
        )
        assert event.query_country == "NL"
        assert event.domain_country == "NL"

    def test_country_fields_default_to_none(self) -> None:
        event = EventDate(
            fingerprint="abc123",
            title="Art Market",
            description="Annual art market",
            location_text="Amsterdam",
            language="en",
            source="duckduckgo",
            url="https://example.com/market",
        )
        assert event.query_country is None
        assert event.domain_country is None

    def test_event_status_values(self) -> None:
        event = EventDate(
            fingerprint="abc123",
            title="Art Market",
            description="Annual art market",
            location_text="Amsterdam",
            language="en",
            source="duckduckgo",
            url="https://example.com/market",
            event_status=EventStatus.FUTURE,
        )
        assert event.event_status == EventStatus.FUTURE

    def test_missing_required(self) -> None:
        with pytest.raises(ValidationError):
            EventDate(
                title="Art Market",
                # missing fingerprint, description, location_text, language, source, url
            )


# ---------------------------------------------------------------------------
# EventArtifact
# ---------------------------------------------------------------------------


class TestEventArtifact:
    def test_valid_minimal(self) -> None:
        artifact = EventArtifact(
            id="abc123",
            event_id="evfp456",
            url="https://example.com/file.pdf",
            artifact_type="pdf",
        )
        assert artifact.processing_status == ProcessingStatus.NEW
        assert artifact.file_path is None
        assert artifact.content_hash is None

    def test_valid_full(self) -> None:
        artifact = EventArtifact(
            id="abc123",
            event_id="evfp456",
            url="https://example.com/poster.jpg",
            artifact_type="image",
            content_hash="deadbeef" * 8,
            file_path="/volumes/event_artifacts/abc/poster.jpg",
            processing_status=ProcessingStatus.DONE,
        )
        assert artifact.processing_status == ProcessingStatus.DONE
        assert artifact.content_hash == "deadbeef" * 8

    def test_invalid_processing_status(self) -> None:
        with pytest.raises(ValidationError):
            EventArtifact(
                id="abc123",
                event_id="evfp456",
                url="https://example.com/file.pdf",
                artifact_type="pdf",
                processing_status="unknown",
            )


# ---------------------------------------------------------------------------
# SeenUrl
# ---------------------------------------------------------------------------


class TestSeenUrl:
    def test_valid(self) -> None:
        seen = SeenUrl(
            url="https://example.com/event",
            title="Open Call",
            source="duckduckgo",
            fingerprint="abc123",
        )
        assert seen.url == HttpUrl("https://example.com/event")
        assert seen.ingested_at is not None

    def test_with_explicit_timestamp(self) -> None:
        seen = SeenUrl(
            url="https://example.com/event",
            title="Open Call",
            source="duckduckgo",
            fingerprint="abc123",
            ingested_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        assert seen.ingested_at == datetime(2026, 1, 1, tzinfo=UTC)

    def test_missing_fingerprint(self) -> None:
        with pytest.raises(ValidationError):
            SeenUrl(
                url="https://example.com/event",
                title="Open Call",
                source="duckduckgo",
            )


# ---------------------------------------------------------------------------
# ArtLakeConfig
# ---------------------------------------------------------------------------


class TestArtLakeConfig:
    def test_valid(self) -> None:
        config = ArtLakeConfig(
            target_countries=["NL", "BE", "DE", "FR"],
            languages=["EN", "NL", "DE", "FR"],
            categories=["open_call", "market", "exhibition", "workshop"],
            scrape_schedule="0 6 * * *",
        )
        assert config.target_language == "EN"

    def test_custom_target_language(self) -> None:
        config = ArtLakeConfig(
            target_countries=["NL"],
            languages=["NL"],
            target_language="NL",
            categories=["open_call"],
            scrape_schedule="0 6 * * *",
        )
        assert config.target_language == "NL"

    def test_missing_required(self) -> None:
        with pytest.raises(ValidationError):
            ArtLakeConfig(
                target_countries=["NL"],
                # missing languages, categories, scrape_schedule
            )


# ---------------------------------------------------------------------------
# ProcessingStatus enum values
# ---------------------------------------------------------------------------


class TestProcessingStatus:
    def test_values(self) -> None:
        assert set(ProcessingStatus) == {
            ProcessingStatus.NEW,
            ProcessingStatus.PROCESSING,
            ProcessingStatus.DOWNLOADED,
            ProcessingStatus.DONE,
            ProcessingStatus.FAILED,
        }

    def test_string_values(self) -> None:
        assert ProcessingStatus.NEW == "new"
        assert ProcessingStatus.PROCESSING == "processing"
        assert ProcessingStatus.DONE == "done"
        assert ProcessingStatus.FAILED == "failed"


# ---------------------------------------------------------------------------
# EventStatus enum values
# ---------------------------------------------------------------------------


class TestEventStatus:
    def test_values(self) -> None:
        assert set(EventStatus) == {
            EventStatus.FUTURE,
            EventStatus.FINISHED,
            EventStatus.UNDEFINED,
        }

    def test_string_values(self) -> None:
        assert EventStatus.FUTURE == "future"
        assert EventStatus.FINISHED == "finished"
        assert EventStatus.UNDEFINED == "undefined"

"""Tests for ArtLake Pydantic data models."""

from datetime import UTC, datetime

import pytest
from pydantic import HttpUrl, ValidationError

from artlake.models import (
    ArtLakeConfig,
    CleanEvent,
    EventArtifact,
    GoldEvent,
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
# CleanEvent
# ---------------------------------------------------------------------------


class TestCleanEvent:
    def test_valid(self) -> None:
        event = CleanEvent(
            fingerprint="abc123",
            title="Art Market",
            description="Annual art market in Amsterdam",
            location_text="Amsterdam, Netherlands",
            language="en",
            source="duckduckgo",
            url="https://example.com/market",
        )
        assert event.date_start is None
        assert event.lat is None
        assert event.artifact_paths == []

    def test_with_dates_and_geo(self) -> None:
        event = CleanEvent(
            fingerprint="abc123",
            title="Art Market",
            description="Annual art market",
            date_start=datetime(2026, 6, 1, tzinfo=UTC),
            date_end=datetime(2026, 6, 3, tzinfo=UTC),
            location_text="Amsterdam",
            lat=52.3676,
            lng=4.9041,
            query_country="NL",
            domain_country="NL",
            country="NL",
            language="en",
            source="duckduckgo",
            url="https://example.com/market",
        )
        assert event.country == "NL"
        assert event.query_country == "NL"
        assert event.domain_country == "NL"

    def test_country_fields_default_to_none(self) -> None:
        event = CleanEvent(
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
        assert event.country is None

    def test_missing_required(self) -> None:
        with pytest.raises(ValidationError):
            CleanEvent(
                title="Art Market",
                # missing description, location_text, language, source, url
            )


# ---------------------------------------------------------------------------
# GoldEvent
# ---------------------------------------------------------------------------


class TestGoldEvent:
    def test_valid(self) -> None:
        event = GoldEvent(
            title="Art Market",
            description="Annual art market",
            location_text="Amsterdam",
            language="en",
            source="duckduckgo",
            url="https://example.com/market",
            category="market",
        )
        assert event.artifact_summaries == []

    def test_missing_category(self) -> None:
        with pytest.raises(ValidationError):
            GoldEvent(
                title="Art Market",
                description="Annual art market",
                location_text="Amsterdam",
                language="en",
                source="duckduckgo",
                url="https://example.com/market",
                # missing category
            )

    def test_with_summaries(self) -> None:
        event = GoldEvent(
            title="Open Call",
            description="Submit your work",
            location_text="Berlin",
            language="en",
            source="duckduckgo",
            url="https://example.com/call",
            category="open_call",
            artifact_summaries=["Deadline: 2026-07-01, Fee: €25"],
        )
        assert len(event.artifact_summaries) == 1


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
            file_path="/volumes/raw_artifacts/abc/poster.jpg",
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
            languages=["en", "nl", "de", "fr"],
            categories=["open_call", "market", "exhibition", "workshop"],
            scrape_schedule="0 6 * * *",
        )
        assert config.target_language == "en"

    def test_custom_target_language(self) -> None:
        config = ArtLakeConfig(
            target_countries=["NL"],
            languages=["nl"],
            target_language="nl",
            categories=["open_call"],
            scrape_schedule="0 6 * * *",
        )
        assert config.target_language == "nl"

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
            ProcessingStatus.OUTDATED,
            ProcessingStatus.REQUIRES_MANUAL_VALIDATION,
        }

    def test_string_values(self) -> None:
        assert ProcessingStatus.NEW == "new"
        assert ProcessingStatus.PROCESSING == "processing"
        assert ProcessingStatus.DONE == "done"
        assert ProcessingStatus.FAILED == "failed"

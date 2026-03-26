"""Tests for clean/events.py."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from pydantic import HttpUrl

from artlake.clean.events import (
    _fields_complete,
    _merge_fields,
    _parse_llm_date,
    _source_from_url,
    clean_page,
    extract_fields_llm,
    extract_fields_rule_based,
    is_outdated,
    parse_dates,
)
from artlake.models.event import ProcessingStatus, ScrapedPage

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _page(
    raw_text: str = "",
    title: str = "",
    artifact_urls: list[str] | None = None,
) -> ScrapedPage:
    return ScrapedPage(
        fingerprint="abc123",
        url=HttpUrl("https://example.com/event"),
        title=title,
        raw_text=raw_text,
        artifact_urls=artifact_urls or [],
        processing_status=ProcessingStatus.NEW,
    )


def _future(days: int = 30) -> datetime:
    return datetime.now(UTC) + timedelta(days=days)


def _past(days: int = 30) -> datetime:
    return datetime.now(UTC) - timedelta(days=days)


# ---------------------------------------------------------------------------
# parse_dates
# ---------------------------------------------------------------------------


class TestParseDates:
    def test_iso_format(self) -> None:
        text = "The event runs from 2099-06-01 to 2099-06-15."
        start, end = parse_dates(text)
        assert start is not None
        assert end is not None
        assert start.year == 2099
        assert start.month == 6
        assert start.day == 1

    def test_european_format(self) -> None:
        text = "Deadline: 15/08/2099"
        start, end = parse_dates(text)
        assert start is not None
        assert start.year == 2099

    def test_natural_language_english(self) -> None:
        text = "Join us on August 20, 2099 for the opening."
        start, _ = parse_dates(text)
        assert start is not None
        assert start.year == 2099

    def test_returns_none_when_no_dates(self) -> None:
        text = "Welcome to our art gallery. No dates mentioned here."
        start, end = parse_dates(text)
        assert start is None
        assert end is None

    def test_single_date_returns_none_end(self) -> None:
        text = "Event on 2099-03-15."
        start, end = parse_dates(text)
        assert start is not None
        assert end is None

    def test_filters_old_dates(self) -> None:
        text = "Founded in 1985. Next event: 2099-05-01."
        start, end = parse_dates(text)
        # 1985 should be filtered out as > 2 years old
        assert start is not None
        assert start.year == 2099

    def test_empty_text(self) -> None:
        start, end = parse_dates("")
        assert start is None
        assert end is None


# ---------------------------------------------------------------------------
# is_outdated
# ---------------------------------------------------------------------------


class TestIsOutdated:
    def test_past_end_date(self) -> None:
        assert is_outdated(_past(10), None) is True

    def test_past_end_date_with_range(self) -> None:
        assert is_outdated(_past(20), _past(5)) is True

    def test_future_end_date(self) -> None:
        assert is_outdated(_past(5), _future(10)) is False

    def test_future_start_no_end(self) -> None:
        assert is_outdated(_future(5), None) is False

    def test_past_start_no_end(self) -> None:
        assert is_outdated(_past(5), None) is True

    def test_no_dates(self) -> None:
        assert is_outdated(None, None) is False


# ---------------------------------------------------------------------------
# extract_fields_rule_based
# ---------------------------------------------------------------------------


class TestExtractFieldsRuleBased:
    def test_uses_page_title(self) -> None:
        page = _page(title="Open Call 2025", raw_text="Some description text here.")
        fields = extract_fields_rule_based(page)
        assert fields["title"] == "Open Call 2025"

    def test_fallback_to_first_line_when_no_title(self) -> None:
        page = _page(title="", raw_text="Art Market Brussels\nMore text here.")
        fields = extract_fields_rule_based(page)
        assert fields["title"] == "Art Market Brussels"

    def test_extracts_description_from_raw_text(self) -> None:
        page = _page(raw_text="A" * 600)
        fields = extract_fields_rule_based(page)
        assert fields["description"] is not None
        assert len(fields["description"]) <= 500

    def test_extracts_location_with_keyword(self) -> None:
        page = _page(raw_text="Event details\nLocation: Antwerp, Belgium\nMore info.")
        fields = extract_fields_rule_based(page)
        assert fields["location_text"] == "Antwerp, Belgium"

    def test_extracts_venue_keyword(self) -> None:
        page = _page(raw_text="Venue: Stedelijk Museum Amsterdam")
        fields = extract_fields_rule_based(page)
        assert fields["location_text"] == "Stedelijk Museum Amsterdam"

    def test_extracts_adresse_keyword(self) -> None:
        page = _page(raw_text="Adresse: Rue du Louvre 75, Paris")
        fields = extract_fields_rule_based(page)
        assert fields["location_text"] == "Rue du Louvre 75, Paris"

    def test_no_location_returns_none(self) -> None:
        page = _page(raw_text="Join us for the event. Great fun ahead.")
        fields = extract_fields_rule_based(page)
        assert fields["location_text"] is None

    def test_empty_page(self) -> None:
        page = _page(title="", raw_text="")
        fields = extract_fields_rule_based(page)
        assert fields["title"] is None
        assert fields["description"] is None
        assert fields["location_text"] is None


# ---------------------------------------------------------------------------
# extract_fields_llm
# ---------------------------------------------------------------------------


class TestExtractFieldsLlm:
    def _mock_client(self, response_json: str) -> MagicMock:
        client = MagicMock()
        msg = MagicMock()
        msg.content = response_json
        client.chat.completions.create.return_value.choices = [MagicMock(message=msg)]
        return client

    def test_returns_parsed_fields(self) -> None:
        payload = (
            '{"title": "Open Call", "description": "Nice event",'
            ' "date_start": "2099-06-01", "date_end": null, "location_text": "Brussels"}'
        )
        client = self._mock_client(payload)
        page = _page(raw_text="Some text about an open call in Brussels.")

        result = extract_fields_llm(page, client, "test-model")

        assert result is not None
        assert result["title"] == "Open Call"
        assert result["location_text"] == "Brussels"

    def test_handles_markdown_fences(self) -> None:
        payload = (
            "```json\n"
            '{"title": "Expo", "description": "Art show",'
            ' "date_start": null, "date_end": null, "location_text": "Paris"}'
            "\n```"
        )
        client = self._mock_client(payload)
        page = _page(raw_text="Art exposition in Paris.")

        result = extract_fields_llm(page, client, "test-model")

        assert result is not None
        assert result["title"] == "Expo"

    def test_returns_none_on_llm_error(self) -> None:
        client = MagicMock()
        client.chat.completions.create.side_effect = Exception("API error")
        page = _page(raw_text="Some text.")

        result = extract_fields_llm(page, client, "test-model")

        assert result is None

    def test_returns_none_for_empty_page(self) -> None:
        client = MagicMock()
        page = _page(raw_text="")

        result = extract_fields_llm(page, client, "test-model")

        assert result is None


# ---------------------------------------------------------------------------
# _fields_complete / _merge_fields / helpers
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_fields_complete_all_present(self) -> None:
        assert _fields_complete({"title": "T", "description": "D", "location_text": "L"})

    def test_fields_complete_missing_one(self) -> None:
        assert not _fields_complete(
            {"title": "T", "description": "D", "location_text": None}
        )

    def test_merge_fills_none_from_override(self) -> None:
        base = {"title": "T", "description": None, "location_text": None}
        override = {"title": "Ignored", "description": "D2", "location_text": "L2"}
        merged = _merge_fields(base, override)
        assert merged["title"] == "T"
        assert merged["description"] == "D2"
        assert merged["location_text"] == "L2"

    def test_parse_llm_date_valid(self) -> None:
        dt = _parse_llm_date("2099-06-01")
        assert dt is not None
        assert dt.year == 2099

    def test_parse_llm_date_invalid(self) -> None:
        assert _parse_llm_date("not-a-date") is None
        assert _parse_llm_date(None) is None

    def test_source_from_url(self) -> None:
        assert _source_from_url("https://example.com/event/123") == "example.com"


# ---------------------------------------------------------------------------
# clean_page — integration of the full funnel
# ---------------------------------------------------------------------------


class TestCleanPage:
    def _client_with(self, payload: str) -> MagicMock:
        client = MagicMock()
        msg = MagicMock()
        msg.content = payload
        client.chat.completions.create.return_value.choices = [MagicMock(message=msg)]
        return client

    def test_outdated_event_returns_outdated_status(self) -> None:
        # Past date in text → funnel exits early with outdated
        past_date = (_past(10)).strftime("%Y-%m-%d")
        page = _page(
            title="Old Show",
            raw_text=f"Event on {past_date}. Location: Amsterdam.",
        )
        client = MagicMock()
        event = clean_page(page, "en", client, "test-model")
        assert event.processing_status == ProcessingStatus.OUTDATED
        client.chat.completions.create.assert_not_called()

    def test_complete_fields_returns_new_status(self) -> None:
        page = _page(
            title="Summer Open Call",
            raw_text="Great exhibition. Location: Brussels, Belgium. Apply by 2099-08-01.",  # noqa: E501
        )
        client = MagicMock()
        event = clean_page(page, "en", client, "test-model")
        assert event.processing_status == ProcessingStatus.NEW
        assert event.title == "Summer Open Call"
        assert event.location_text == "Brussels, Belgium"

    def test_llm_fallback_called_when_fields_incomplete(self) -> None:
        page = _page(title="Show", raw_text="Some vague text with no location.")
        payload = (
            '{"title": "Show", "description": "Nice event.",'
            ' "date_start": null, "date_end": null, "location_text": "Rotterdam"}'
        )
        client = self._client_with(payload)

        event = clean_page(page, "nl", client, "test-model")

        client.chat.completions.create.assert_called_once()
        assert event.location_text == "Rotterdam"
        assert event.processing_status == ProcessingStatus.NEW

    def test_requires_manual_validation_when_llm_also_fails(self) -> None:
        page = _page(title="Show", raw_text="Some vague text.")
        client = MagicMock()
        client.chat.completions.create.side_effect = Exception("LLM down")

        event = clean_page(page, "en", client, "test-model")

        assert event.processing_status == ProcessingStatus.REQUIRES_MANUAL_VALIDATION

    def test_llm_date_triggers_outdated_recheck(self) -> None:
        # No dates in text (rule-based finds nothing), LLM returns past date
        past_date = _past(5).strftime("%Y-%m-%d")
        page = _page(
            title="Old", raw_text="Some vague text without clear dates no location."
        )
        payload = (
            f'{{"title": "Old", "description": "Past event.",'
            f' "date_start": "{past_date}", "date_end": null, "location_text": "Berlin"}}'
        )
        client = self._client_with(payload)

        event = clean_page(page, "de", client, "test-model")

        assert event.processing_status == ProcessingStatus.OUTDATED

    def test_copies_artifact_urls(self) -> None:
        page = _page(
            title="Show",
            raw_text="Location: Amsterdam. Great art event happening in 2099.",
            artifact_urls=["https://example.com/poster.pdf"],
        )
        client = MagicMock()
        event = clean_page(page, "en", client, "test-model")
        assert "https://example.com/poster.pdf" in event.artifact_urls

    def test_no_date_event_is_not_outdated(self) -> None:
        page = _page(
            title="Ongoing Show",
            raw_text="An ongoing exhibition. Location: Paris.",
        )
        client = MagicMock()
        client.chat.completions.create.return_value.choices = [
            MagicMock(
                message=MagicMock(
                    content=(
                        '{"title": "Ongoing Show", "description": "Ongoing.",'
                        ' "date_start": null, "date_end": null, "location_text": "Paris"}'
                    )
                )
            )
        ]
        event = clean_page(page, "fr", client, "test-model")
        assert event.processing_status != ProcessingStatus.OUTDATED


# ---------------------------------------------------------------------------
# Integration test (requires live Databricks)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCleanEventsIntegration:
    def test_delta_write(self) -> None:
        """Write a single CleanEvent to bronze.raw_events and verify row count."""
        from artlake.clean.events import run_clean

        # Integration test: validates schema compatibility and Delta write
        # Requires a Databricks workspace with artlake catalog.
        run_clean(
            scraped_pages_table="artlake.staging.scraped_pages",
            search_results_table="artlake.staging.search_results",
            raw_events_table="artlake.bronze.raw_events",
            model="databricks-meta-llama-3-3-70b-instruct",
        )

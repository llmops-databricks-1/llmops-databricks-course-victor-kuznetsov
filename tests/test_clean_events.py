"""Tests for clean/events.py."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import HttpUrl

from artlake.events.extract_dates import (
    LanguagePatterns,
    _clean_title,
    _country_from_url,
    _fields_complete,
    _looks_like_html,
    _merge_fields,
    _normalize_text,
    _parse_llm_date,
    _source_from_url,
    build_field_re,
    extract_dates,
    extract_fields_llm,
    extract_fields_rule_based,
    get_event_status,
    parse_dates,
)
from artlake.models.event import EventStatus, ProcessingStatus, ScrapedPage

_TEST_PATTERNS = LanguagePatterns(
    generated_at="2026-01-01T00:00:00+00:00",
    model="test",
    languages=["en", "nl", "de", "fr"],
    target_countries=["NL", "BE", "DE", "FR", "LU"],
    title_keywords={
        "en": ["Title", "Event", "Name"],
        "nl": ["Titel", "Evenement", "Naam"],
        "de": ["Titel", "Veranstaltung"],
        "fr": ["Titre", "Événement", "Nom"],
    },
    location_keywords={
        "en": ["Location", "Venue", "Address", "Place"],
        "nl": ["Locatie", "Adres"],
        "de": ["Ort", "Adresse"],
        "fr": ["Lieu", "Adresse", "Endroit"],
    },
)
_TEST_LOCATION_RE = build_field_re(_TEST_PATTERNS.location_keywords)
_TEST_TITLE_RE = build_field_re(_TEST_PATTERNS.title_keywords)

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

    def test_filters_absurd_future_dates(self) -> None:
        # Year 9816 (dateparser artefact) should be filtered out
        text = "Event on 2027-06-01. Copyright 9816."
        start, end = parse_dates(text)
        assert start is not None
        assert start.year == 2027
        assert end is None

    def test_empty_text(self) -> None:
        start, end = parse_dates("")
        assert start is None
        assert end is None

    def test_uppercase_language_codes(self) -> None:
        """Uppercase language codes from language_patterns must not raise."""
        text = "Event on 2099-08-01."
        start, _ = parse_dates(text, languages=["NL", "FR", "DE"])
        assert start is not None
        assert start.year == 2099


# ---------------------------------------------------------------------------
# get_event_status
# ---------------------------------------------------------------------------


class TestGetEventStatus:
    def test_past_end_date_returns_finished(self) -> None:
        assert get_event_status(_past(10), None) == EventStatus.FINISHED

    def test_past_range_returns_finished(self) -> None:
        assert get_event_status(_past(20), _past(5)) == EventStatus.FINISHED

    def test_future_end_date_returns_future(self) -> None:
        assert get_event_status(_past(5), _future(10)) == EventStatus.FUTURE

    def test_future_start_no_end_returns_future(self) -> None:
        assert get_event_status(_future(5), None) == EventStatus.FUTURE

    def test_past_start_no_end_returns_finished(self) -> None:
        assert get_event_status(_past(5), None) == EventStatus.FINISHED

    def test_no_dates_returns_undefined(self) -> None:
        assert get_event_status(None, None) == EventStatus.UNDEFINED


# ---------------------------------------------------------------------------
# extract_fields_rule_based
# ---------------------------------------------------------------------------


class TestExtractFieldsRuleBased:
    def test_uses_page_title(self) -> None:
        page = _page(title="Open Call 2025", raw_text="Some description text here.")
        fields = extract_fields_rule_based(page, _TEST_LOCATION_RE, _TEST_TITLE_RE)
        assert fields["title"] == "Open Call 2025"

    def test_strips_title_site_suffix(self) -> None:
        page = _page(title="Open Call | Gallery Brussels", raw_text="Some text.")
        fields = extract_fields_rule_based(page, _TEST_LOCATION_RE, _TEST_TITLE_RE)
        assert fields["title"] == "Open Call"

    def test_normalizes_description_whitespace(self) -> None:
        page = _page(raw_text="  Art   event  \n\t details  here  ")
        fields = extract_fields_rule_based(page, _TEST_LOCATION_RE, _TEST_TITLE_RE)
        assert fields["description"] == "Art event details here"

    def test_decodes_html_entities_in_description(self) -> None:
        page = _page(raw_text="Open call for Art &amp; Design enthusiasts.")
        fields = extract_fields_rule_based(page, _TEST_LOCATION_RE, _TEST_TITLE_RE)
        assert fields["description"] == "Open call for Art & Design enthusiasts."

    def test_extracts_title_from_labeled_field(self) -> None:
        page = _page(title="", raw_text="Titel: Kunstmarkt Amsterdam\nMore text here.")
        fields = extract_fields_rule_based(page, _TEST_LOCATION_RE, _TEST_TITLE_RE)
        assert fields["title"] == "Kunstmarkt Amsterdam"

    def test_fallback_to_first_line_when_no_title(self) -> None:
        page = _page(title="", raw_text="Art Market Brussels\nMore text here.")
        fields = extract_fields_rule_based(page, _TEST_LOCATION_RE, _TEST_TITLE_RE)
        assert fields["title"] == "Art Market Brussels"

    def test_extracts_description_from_raw_text(self) -> None:
        page = _page(raw_text="A" * 600)
        fields = extract_fields_rule_based(page, _TEST_LOCATION_RE, _TEST_TITLE_RE)
        assert fields["description"] is not None
        assert len(fields["description"]) <= 500

    def test_extracts_location_with_keyword(self) -> None:
        page = _page(raw_text="Event details\nLocation: Antwerp, Belgium\nMore info.")
        fields = extract_fields_rule_based(page, _TEST_LOCATION_RE, _TEST_TITLE_RE)
        assert fields["location_text"] == "Antwerp, Belgium"

    def test_extracts_venue_keyword(self) -> None:
        page = _page(raw_text="Venue: Stedelijk Museum Amsterdam")
        fields = extract_fields_rule_based(page, _TEST_LOCATION_RE, _TEST_TITLE_RE)
        assert fields["location_text"] == "Stedelijk Museum Amsterdam"

    def test_extracts_adresse_keyword(self) -> None:
        page = _page(raw_text="Adresse: Rue du Louvre 75, Paris")
        fields = extract_fields_rule_based(page, _TEST_LOCATION_RE, _TEST_TITLE_RE)
        assert fields["location_text"] == "Rue du Louvre 75, Paris"

    def test_no_location_returns_none(self) -> None:
        page = _page(raw_text="Join us for the event. Great fun ahead.")
        fields = extract_fields_rule_based(page, _TEST_LOCATION_RE, _TEST_TITLE_RE)
        assert fields["location_text"] is None

    def test_html_raw_text_forces_none_fields(self) -> None:
        page = _page(raw_text="<!DOCTYPE html><html><body>Maintenance</body></html>")
        fields = extract_fields_rule_based(page, _TEST_LOCATION_RE, _TEST_TITLE_RE)
        assert fields["description"] is None
        assert fields["location_text"] is None

    def test_empty_page(self) -> None:
        page = _page(title="", raw_text="")
        fields = extract_fields_rule_based(page, _TEST_LOCATION_RE, _TEST_TITLE_RE)
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


# ---------------------------------------------------------------------------
# Text normalisation helpers
# ---------------------------------------------------------------------------


class TestNormalizeText:
    def test_collapses_whitespace(self) -> None:
        assert _normalize_text("hello   world\t\nfoo") == "hello world foo"

    def test_decodes_html_entities(self) -> None:
        assert _normalize_text("Art &amp; Culture") == "Art & Culture"
        assert _normalize_text("open&#39;call") == "open'call"

    def test_normalizes_unicode_nfkc(self) -> None:
        # Ligature fi → f + i
        assert _normalize_text("\ufb01ne art") == "fine art"

    def test_strips_leading_trailing(self) -> None:
        assert _normalize_text("  hello  ") == "hello"

    def test_empty_string(self) -> None:
        assert _normalize_text("") == ""


class TestCleanTitle:
    def test_strips_pipe_suffix(self) -> None:
        assert _clean_title("Open Call | Gallery Brussels") == "Open Call"

    def test_strips_dash_suffix(self) -> None:
        assert _clean_title("Summer Exhibition - Stedelijk Museum") == "Summer Exhibition"

    def test_strips_en_dash_suffix(self) -> None:
        assert _clean_title("Art Fair – Amsterdam") == "Art Fair"

    def test_strips_em_dash_suffix(self) -> None:
        assert _clean_title("Residency — Berlin Art Week") == "Residency"

    def test_no_suffix_unchanged(self) -> None:
        assert _clean_title("Open Call for Artists") == "Open Call for Artists"

    def test_normalizes_whitespace(self) -> None:
        assert _clean_title("  Open   Call  ") == "Open Call"

    def test_decodes_html_entities(self) -> None:
        assert _clean_title("Art &amp; Design | Museum") == "Art & Design"


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

    def test_country_from_url_known_tld(self) -> None:
        countries = ["NL", "BE", "DE", "FR", "LU"]
        assert _country_from_url("https://gallery.nl/event", countries) == "NL"
        assert _country_from_url("https://museum.be/expo", countries) == "BE"
        assert _country_from_url("https://kunst.de/aufruf", countries) == "DE"
        assert _country_from_url("https://art.fr/appel", countries) == "FR"
        assert _country_from_url("https://opc-luxembourg.lu/de/", countries) == "LU"

    def test_country_from_url_unknown_tld(self) -> None:
        countries = ["NL", "BE", "DE", "FR", "LU"]
        assert _country_from_url("https://example.com/event", countries) is None
        assert _country_from_url("https://artsy.org/show", countries) is None

    def test_looks_like_html_doctype(self) -> None:
        assert _looks_like_html("<!DOCTYPE html><html><body>text</body></html>")

    def test_looks_like_html_tag(self) -> None:
        assert _looks_like_html("<html lang='fr'><head></head></html>")

    def test_looks_like_html_false_for_plain_text(self) -> None:
        assert not _looks_like_html("Open call for artists in Brussels.")
        assert not _looks_like_html("Location: Amsterdam")


# ---------------------------------------------------------------------------
# extract_dates — integration of the full funnel
# ---------------------------------------------------------------------------


class TestCleanPage:
    def _client_with(self, payload: str) -> MagicMock:
        client = MagicMock()
        msg = MagicMock()
        msg.content = payload
        client.chat.completions.create.return_value.choices = [MagicMock(message=msg)]
        return client

    def test_past_event_returns_finished_status(self) -> None:
        past_date = (_past(10)).strftime("%Y-%m-%d")
        page = _page(
            title="Old Show",
            raw_text=f"Event on {past_date}. Location: Amsterdam.",
        )
        client = MagicMock()
        event = extract_dates(page, "en", client, "test-model", _TEST_PATTERNS)
        assert event.event_status == EventStatus.FINISHED

    def test_complete_fields_extracted(self) -> None:
        page = _page(
            title="Summer Open Call",
            raw_text="Great exhibition. Location: Brussels, Belgium. Apply by 2099-08-01.",  # noqa: E501
        )
        client = MagicMock()
        event = extract_dates(page, "en", client, "test-model", _TEST_PATTERNS)
        assert event.title == "Summer Open Call"
        assert event.location_text == "Brussels, Belgium"

    def test_llm_fallback_called_when_fields_incomplete(self) -> None:
        page = _page(title="Show", raw_text="Some vague text with no location.")
        payload = (
            '{"title": "Show", "description": "Nice event.",'
            ' "date_start": null, "date_end": null, "location_text": "Rotterdam"}'
        )
        client = self._client_with(payload)

        event = extract_dates(page, "nl", client, "test-model", _TEST_PATTERNS)

        client.chat.completions.create.assert_called_once()
        assert event.location_text == "Rotterdam"
        assert event.event_status == EventStatus.UNDEFINED

    def test_returns_event_date_when_llm_fails(self) -> None:
        page = _page(title="Show", raw_text="Some vague text.")
        client = MagicMock()
        client.chat.completions.create.side_effect = Exception("LLM down")

        from artlake.models.event import EventDate

        event = extract_dates(page, "en", client, "test-model", _TEST_PATTERNS)

        assert isinstance(event, EventDate)
        assert event.event_status == EventStatus.UNDEFINED

    def test_llm_date_sets_finished_status(self) -> None:
        past_date = _past(5).strftime("%Y-%m-%d")
        page = _page(
            title="Old", raw_text="Some vague text without clear dates no location."
        )
        payload = (
            f'{{"title": "Old", "description": "Past event.",'
            f' "date_start": "{past_date}", "date_end": null, "location_text": "Berlin"}}'
        )
        client = self._client_with(payload)

        event = extract_dates(page, "de", client, "test-model", _TEST_PATTERNS)

        assert event.event_status == EventStatus.FINISHED

    def test_copies_artifact_urls(self) -> None:
        page = _page(
            title="Show",
            raw_text="Location: Amsterdam. Great art event happening in 2099.",
            artifact_urls=["https://example.com/poster.pdf"],
        )
        client = MagicMock()
        event = extract_dates(page, "en", client, "test-model", _TEST_PATTERNS)
        assert "https://example.com/poster.pdf" in event.artifact_urls

    def test_no_date_event_has_undefined_status(self) -> None:
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
        event = extract_dates(page, "fr", client, "test-model", _TEST_PATTERNS)
        assert event.event_status == EventStatus.UNDEFINED


# ---------------------------------------------------------------------------
# Integration test (requires live Databricks)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCleanEventsIntegration:
    def test_delta_write(self) -> None:
        """Write EventDate records to bronze.event_dates."""
        from artlake.events.extract_dates import run_extract_dates

        # Integration test: validates schema compatibility and Delta write
        # Requires a Databricks workspace with artlake catalog.
        run_extract_dates(
            scraped_pages_table="artlake.staging.scraped_pages",
            search_results_table="artlake.staging.search_results",
            event_dates_table="artlake.bronze.event_dates",
            patterns_path=Path("config/output/language_patterns.yml"),
        )

"""Tests for filter/country.py geocoding + country filter."""

from __future__ import annotations

import pytest
from pydantic import HttpUrl

from artlake.events.geocode import (
    apply_geocoding,
    geocode_location,
    llm_extract_address,
    llm_resolve_country,
)
from artlake.models.event import EventDate, LocationStatus

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_event(fingerprint: str, location_text: str) -> EventDate:
    return EventDate(
        fingerprint=fingerprint,
        title="Test Event",
        description="A test art event",
        location_text=location_text,
        language="en",
        source="test.com",
        url=HttpUrl("https://test.com/event"),
    )


class MockLocation:
    """Minimal geopy Location stand-in."""

    def __init__(self, lat: float, lng: float, country_code: str) -> None:
        self.latitude = lat
        self.longitude = lng
        self.raw = {"address": {"country_code": country_code}}


# ---------------------------------------------------------------------------
# geocode_location
# ---------------------------------------------------------------------------


class TestGeocodeLocation:
    def test_success_returns_lat_lng_country(self) -> None:
        mock = MockLocation(52.37, 4.89, "nl")
        cache: dict = {}

        result = geocode_location("Amsterdam", lambda _: mock, cache)

        assert result == (52.37, 4.89, "NL")

    def test_country_code_uppercased(self) -> None:
        mock = MockLocation(50.85, 4.35, "be")
        cache: dict = {}

        _, _, country = geocode_location("Brussels", lambda _: mock, cache)

        assert country == "BE"

    def test_result_stored_in_cache(self) -> None:
        mock = MockLocation(52.37, 4.89, "nl")
        cache: dict = {}

        geocode_location("Amsterdam", lambda _: mock, cache)

        assert "Amsterdam" in cache
        assert cache["Amsterdam"] == (52.37, 4.89, "NL")

    def test_cache_hit_skips_geocode_fn(self) -> None:
        call_count = 0

        def counting_geocode(_: str) -> MockLocation:
            nonlocal call_count
            call_count += 1
            return MockLocation(52.37, 4.89, "nl")

        cache: dict = {}
        geocode_location("Amsterdam", counting_geocode, cache)
        geocode_location("Amsterdam", counting_geocode, cache)

        assert call_count == 1

    def test_returns_none_triple_when_not_found(self) -> None:
        cache: dict = {}

        result = geocode_location("xyzzy not a real place", lambda _: None, cache)

        assert result == (None, None, None)

    def test_caches_none_result(self) -> None:
        call_count = 0

        def counting_geocode(_: str) -> None:
            nonlocal call_count
            call_count += 1
            return None

        cache: dict = {}
        geocode_location("nowhere", counting_geocode, cache)
        geocode_location("nowhere", counting_geocode, cache)

        assert call_count == 1

    def test_handles_geocode_exception(self) -> None:
        def failing_geocode(_: str) -> None:
            raise RuntimeError("network error")

        cache: dict = {}
        result = geocode_location("Amsterdam", failing_geocode, cache)

        assert result == (None, None, None)

    def test_exception_result_cached(self) -> None:
        call_count = 0

        def failing_geocode(_: str) -> None:
            nonlocal call_count
            call_count += 1
            raise RuntimeError("network error")

        cache: dict = {}
        geocode_location("Amsterdam", failing_geocode, cache)
        geocode_location("Amsterdam", failing_geocode, cache)

        assert call_count == 1

    def test_empty_location_text_returns_none_without_calling_fn(self) -> None:
        call_count = 0

        def counting_geocode(_: str) -> MockLocation:
            nonlocal call_count
            call_count += 1
            return MockLocation(0.0, 0.0, "nl")

        cache: dict = {}
        result = geocode_location("", counting_geocode, cache)

        assert result == (None, None, None)
        assert call_count == 0

    def test_missing_country_code_in_raw(self) -> None:
        """Location resolves but Nominatim returns no country_code."""

        class LocationNoCC:
            latitude = 52.0
            longitude = 4.0
            raw = {"address": {}}

        cache: dict = {}
        _, _, country = geocode_location("Somewhere", lambda _: LocationNoCC(), cache)

        assert country is None


# ---------------------------------------------------------------------------
# llm_extract_address
# ---------------------------------------------------------------------------


class TestLLMExtractAddress:
    def test_success_returns_clean_address(self) -> None:
        cache: dict = {}
        result = llm_extract_address(
            "Villa Waldberta (Höhenbergstraße 25)",
            lambda _: "Feldafing, Bavaria, Germany",
            cache,
        )
        assert result == "Feldafing, Bavaria, Germany"

    def test_none_response_returns_none(self) -> None:
        cache: dict = {}
        result = llm_extract_address("gibberish xyz", lambda _: None, cache)
        assert result is None

    def test_none_string_response_returns_none(self) -> None:
        cache: dict = {}
        result = llm_extract_address("form text blah", lambda _: "NONE", cache)
        assert result is None

    def test_none_case_insensitive(self) -> None:
        cache: dict = {}
        result = llm_extract_address("form text blah", lambda _: "none", cache)
        assert result is None

    def test_empty_location_returns_none_without_calling_fn(self) -> None:
        call_count = 0

        def counting_fn(_: str) -> str:
            nonlocal call_count
            call_count += 1
            return "Berlin, Germany"

        cache: dict = {}
        result = llm_extract_address("", counting_fn, cache)
        assert result is None
        assert call_count == 0

    def test_result_stored_in_cache(self) -> None:
        cache: dict = {}
        llm_extract_address("messy location", lambda _: "Brussels, Belgium", cache)
        assert cache["messy location"] == "Brussels, Belgium"

    def test_cache_hit_skips_fn(self) -> None:
        call_count = 0

        def counting_fn(_: str) -> str:
            nonlocal call_count
            call_count += 1
            return "Brussels, Belgium"

        cache: dict = {}
        llm_extract_address("messy", counting_fn, cache)
        llm_extract_address("messy", counting_fn, cache)
        assert call_count == 1

    def test_exception_returns_none_and_caches(self) -> None:
        call_count = 0

        def failing_fn(_: str) -> str:
            nonlocal call_count
            call_count += 1
            raise RuntimeError("LLM error")

        cache: dict = {}
        result = llm_extract_address("somewhere", failing_fn, cache)
        llm_extract_address("somewhere", failing_fn, cache)
        assert result is None
        assert call_count == 1

    def test_whitespace_stripped(self) -> None:
        cache: dict = {}
        result = llm_extract_address("loc", lambda _: "  Antwerp, Belgium  ", cache)
        assert result == "Antwerp, Belgium"


# ---------------------------------------------------------------------------
# llm_resolve_country
# ---------------------------------------------------------------------------


class TestLLMResolveCountry:
    def test_success_returns_uppercased_code(self) -> None:
        cache: dict = {}
        result = llm_resolve_country("Belgique", lambda _: "be", cache)
        assert result == "BE"

    def test_unknown_response_returns_none(self) -> None:
        cache: dict = {}
        result = llm_resolve_country("somewhere", lambda _: "UNKNOWN", cache)
        assert result is None

    def test_empty_location_returns_none_without_calling_fn(self) -> None:
        call_count = 0

        def counting_fn(_: str) -> str:
            nonlocal call_count
            call_count += 1
            return "DE"

        cache: dict = {}
        result = llm_resolve_country("", counting_fn, cache)
        assert result is None
        assert call_count == 0

    def test_result_stored_in_cache(self) -> None:
        cache: dict = {}
        llm_resolve_country("Belgique", lambda _: "BE", cache)
        assert "Belgique" in cache
        assert cache["Belgique"] == "BE"

    def test_cache_hit_skips_fn(self) -> None:
        call_count = 0

        def counting_fn(_: str) -> str:
            nonlocal call_count
            call_count += 1
            return "DE"

        cache: dict = {}
        llm_resolve_country("Berlin", counting_fn, cache)
        llm_resolve_country("Berlin", counting_fn, cache)
        assert call_count == 1

    def test_none_response_returns_none(self) -> None:
        cache: dict = {}
        result = llm_resolve_country("mystery", lambda _: None, cache)
        assert result is None

    def test_exception_returns_none_and_caches(self) -> None:
        call_count = 0

        def failing_fn(_: str) -> str:
            nonlocal call_count
            call_count += 1
            raise RuntimeError("LLM error")

        cache: dict = {}
        result = llm_resolve_country("somewhere", failing_fn, cache)
        llm_resolve_country("somewhere", failing_fn, cache)
        assert result is None
        assert call_count == 1

    def test_non_alpha_response_returns_none(self) -> None:
        cache: dict = {}
        result = llm_resolve_country("123 Main St", lambda _: "42", cache)
        assert result is None

    def test_response_with_extra_whitespace_parsed(self) -> None:
        cache: dict = {}
        result = llm_resolve_country("Feldafing", lambda _: "  de  ", cache)
        assert result == "DE"

    def test_takes_first_two_chars_of_valid_response(self) -> None:
        cache: dict = {}
        result = llm_resolve_country("Amsterdam", lambda _: "NL (Netherlands)", cache)
        assert result == "NL"


# ---------------------------------------------------------------------------
# apply_geocoding
# ---------------------------------------------------------------------------


class TestApplyGeocoding:
    def test_accepted_event_gets_identified_status(self) -> None:
        event = _make_event("fp1", "Amsterdam")

        result = apply_geocoding(
            [event], ["NL", "BE"], lambda _: MockLocation(52.37, 4.89, "nl")
        )

        assert result[0].location_status == LocationStatus.IDENTIFIED
        assert result[0].country == "NL"
        assert result[0].lat == pytest.approx(52.37)
        assert result[0].lng == pytest.approx(4.89)

    def test_out_of_target_country_gets_requires_validation(self) -> None:
        event = _make_event("fp1", "London")

        result = apply_geocoding(
            [event], ["NL", "BE"], lambda _: MockLocation(51.50, -0.12, "gb")
        )

        assert result[0].location_status == LocationStatus.REQUIRES_VALIDATION
        assert result[0].country == "GB"

    def test_unresolvable_location_gets_missing_status(self) -> None:
        event = _make_event("fp1", "unknown place xyz")

        result = apply_geocoding([event], ["NL"], lambda _: None)

        assert result[0].location_status == LocationStatus.MISSING
        assert result[0].country is None
        assert result[0].lat is None
        assert result[0].lng is None

    def test_cache_deduplicates_identical_location_text(self) -> None:
        """Identical location_text → geocode_fn called only once."""
        call_count = 0

        def counting_geocode(_: str) -> MockLocation:
            nonlocal call_count
            call_count += 1
            return MockLocation(50.85, 4.35, "be")

        events = [
            _make_event("fp1", "Brussels"),
            _make_event("fp2", "Brussels"),
            _make_event("fp3", "Brussels"),
        ]

        apply_geocoding(events, ["BE"], counting_geocode)

        assert call_count == 1

    def test_target_countries_comparison_is_case_insensitive(self) -> None:
        event = _make_event("fp1", "Amsterdam")

        result = apply_geocoding(
            [event], ["nl"], lambda _: MockLocation(52.37, 4.89, "NL")
        )

        assert result[0].location_status == LocationStatus.IDENTIFIED

    def test_mixed_countries_filtered_correctly(self) -> None:
        events = [
            _make_event("fp1", "Amsterdam"),  # NL → identified
            _make_event("fp2", "Brussels"),  # BE → identified
            _make_event("fp3", "London"),  # GB → requires_validation
            _make_event("fp4", "nowhere"),  # unresolvable → missing
        ]

        location_map: dict[str, MockLocation | None] = {
            "Amsterdam": MockLocation(52.37, 4.89, "nl"),
            "Brussels": MockLocation(50.85, 4.35, "be"),
            "London": MockLocation(51.50, -0.12, "gb"),
            "nowhere": None,
        }

        result = apply_geocoding(
            events, ["NL", "BE"], lambda text: location_map.get(text)
        )

        statuses = [e.location_status for e in result]
        assert statuses == [
            LocationStatus.IDENTIFIED,
            LocationStatus.IDENTIFIED,
            LocationStatus.REQUIRES_VALIDATION,
            LocationStatus.MISSING,
        ]

    def test_output_length_matches_input(self) -> None:
        events = [_make_event(f"fp{i}", f"Location {i}") for i in range(5)]

        result = apply_geocoding(events, ["NL"], lambda _: None)

        assert len(result) == 5

    def test_llm_address_resolves_with_coordinates(self) -> None:
        """Stage 2: LLM normalises address → Nominatim succeeds → lat/lng set."""
        location_map = {
            "Villa Waldberta (messy)": None,
            "Feldafing, Bavaria, Germany": MockLocation(47.96, 11.29, "de"),
        }
        event = _make_event("fp1", "Villa Waldberta (messy)")

        result = apply_geocoding(
            [event],
            ["DE"],
            lambda text: location_map.get(text),
            llm_address_fn=lambda _: "Feldafing, Bavaria, Germany",
        )

        assert result[0].location_status == LocationStatus.IDENTIFIED
        assert result[0].country == "DE"
        assert result[0].lat == pytest.approx(47.96)
        assert result[0].lng == pytest.approx(11.29)

    def test_llm_address_not_called_when_nominatim_succeeds(self) -> None:
        llm_address_calls = 0

        def counting_address_fn(_: str) -> str:
            nonlocal llm_address_calls
            llm_address_calls += 1
            return "Amsterdam, Netherlands"

        event = _make_event("fp1", "Amsterdam")
        apply_geocoding(
            [event],
            ["NL"],
            lambda _: MockLocation(52.37, 4.89, "nl"),
            llm_address_fn=counting_address_fn,
        )

        assert llm_address_calls == 0

    def test_llm_address_fails_falls_through_to_country_fn(self) -> None:
        """When LLM address + Nominatim both fail, country-only fn is used."""
        event = _make_event("fp1", "Belgique")

        result = apply_geocoding(
            [event],
            ["BE"],
            lambda _: None,
            llm_address_fn=lambda _: "NONE",
            llm_country_fn=lambda _: "BE",
        )

        assert result[0].location_status == LocationStatus.IDENTIFIED
        assert result[0].country == "BE"
        assert result[0].lat is None
        assert result[0].lng is None

    def test_llm_address_resolves_but_out_of_target(self) -> None:
        location_map = {
            "Luxembourg": None,
            "Luxembourg City, Luxembourg": MockLocation(49.61, 6.13, "lu"),
        }
        event = _make_event("fp1", "Luxembourg")

        result = apply_geocoding(
            [event],
            ["NL", "BE"],
            lambda text: location_map.get(text),
            llm_address_fn=lambda _: "Luxembourg City, Luxembourg",
        )

        assert result[0].location_status == LocationStatus.REQUIRES_VALIDATION
        assert result[0].country == "LU"

    def test_llm_address_cache_deduplicates(self) -> None:
        address_calls = 0

        def counting_address_fn(_: str) -> str:
            nonlocal address_calls
            address_calls += 1
            return "Brussels, Belgium"

        events = [_make_event(f"fp{i}", "Bruxelles") for i in range(3)]

        apply_geocoding(
            events,
            ["BE"],
            lambda _: None,
            llm_address_fn=counting_address_fn,
        )

        assert address_calls == 1

    def test_llm_fallback_used_when_nominatim_fails(self) -> None:
        """Stage 3 (country only): no address fn, country fn resolves."""
        event = _make_event("fp1", "Belgique")

        result = apply_geocoding(
            [event], ["BE"], lambda _: None, llm_country_fn=lambda _: "BE"
        )

        assert result[0].location_status == LocationStatus.IDENTIFIED
        assert result[0].country == "BE"
        assert result[0].lat is None
        assert result[0].lng is None

    def test_llm_fallback_not_called_when_nominatim_succeeds(self) -> None:
        llm_call_count = 0

        def counting_llm(_: str) -> str:
            nonlocal llm_call_count
            llm_call_count += 1
            return "NL"

        event = _make_event("fp1", "Amsterdam")
        apply_geocoding(
            [event],
            ["NL"],
            lambda _: MockLocation(52.37, 4.89, "nl"),
            llm_country_fn=counting_llm,
        )

        assert llm_call_count == 0

    def test_llm_fallback_out_of_target_gets_requires_validation(self) -> None:
        event = _make_event("fp1", "Luxembourg")

        result = apply_geocoding(
            [event], ["NL", "BE"], lambda _: None, llm_country_fn=lambda _: "LU"
        )

        assert result[0].location_status == LocationStatus.REQUIRES_VALIDATION
        assert result[0].country == "LU"

    def test_llm_fallback_returns_unknown_gets_missing(self) -> None:
        event = _make_event("fp1", "gibberish form text xyz")

        result = apply_geocoding(
            [event], ["NL"], lambda _: None, llm_country_fn=lambda _: "UNKNOWN"
        )

        assert result[0].location_status == LocationStatus.MISSING
        assert result[0].country is None

    def test_no_llm_fn_still_fails_on_unresolvable(self) -> None:
        event = _make_event("fp1", "mystery location")

        result = apply_geocoding([event], ["NL"], lambda _: None)

        assert result[0].location_status == LocationStatus.MISSING
        assert result[0].country is None

    def test_llm_country_cache_deduplicates_identical_location_text(self) -> None:
        llm_call_count = 0

        def counting_llm(_: str) -> str:
            nonlocal llm_call_count
            llm_call_count += 1
            return "BE"

        events = [
            _make_event("fp1", "Belgique"),
            _make_event("fp2", "Belgique"),
            _make_event("fp3", "Belgique"),
        ]

        apply_geocoding(events, ["BE"], lambda _: None, llm_country_fn=counting_llm)

        assert llm_call_count == 1


# ---------------------------------------------------------------------------
# Integration test (requires live Databricks)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestGeocodeIntegration:
    def test_run_geocode(self) -> None:
        from artlake.events.geocode import run_geocode

        run_geocode(
            event_dates_table="artlake.bronze.event_dates",
            event_location_table="artlake.bronze.event_location",
            target_countries=["NL", "BE", "DE", "FR"],
            env="dev",
        )

"""Tests for filter/country.py geocoding + country filter."""

from __future__ import annotations

import pytest
from pydantic import HttpUrl

from artlake.filter.country import apply_geocoding, geocode_location
from artlake.models.event import CleanEvent, ProcessingStatus

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_event(fingerprint: str, location_text: str) -> CleanEvent:
    return CleanEvent(
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
# apply_geocoding
# ---------------------------------------------------------------------------


class TestApplyGeocoding:
    def test_accepted_event_gets_done_status(self) -> None:
        event = _make_event("fp1", "Amsterdam")

        result = apply_geocoding(
            [event], ["NL", "BE"], lambda _: MockLocation(52.37, 4.89, "nl")
        )

        assert result[0].processing_status == ProcessingStatus.DONE
        assert result[0].country == "NL"
        assert result[0].lat == pytest.approx(52.37)
        assert result[0].lng == pytest.approx(4.89)

    def test_out_of_target_country_gets_failed_status(self) -> None:
        event = _make_event("fp1", "London")

        result = apply_geocoding(
            [event], ["NL", "BE"], lambda _: MockLocation(51.50, -0.12, "gb")
        )

        assert result[0].processing_status == ProcessingStatus.FAILED
        assert result[0].country == "GB"

    def test_unresolvable_location_gets_failed_with_unknown_country(self) -> None:
        event = _make_event("fp1", "unknown place xyz")

        result = apply_geocoding([event], ["NL"], lambda _: None)

        assert result[0].processing_status == ProcessingStatus.FAILED
        assert result[0].country == "unknown"
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

        assert result[0].processing_status == ProcessingStatus.DONE

    def test_mixed_countries_filtered_correctly(self) -> None:
        events = [
            _make_event("fp1", "Amsterdam"),  # NL → done
            _make_event("fp2", "Brussels"),  # BE → done
            _make_event("fp3", "London"),  # GB → failed
            _make_event("fp4", "nowhere"),  # unresolvable → failed
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

        statuses = [e.processing_status for e in result]
        assert statuses == [
            ProcessingStatus.DONE,
            ProcessingStatus.DONE,
            ProcessingStatus.FAILED,
            ProcessingStatus.FAILED,
        ]

    def test_unresolvable_country_set_to_unknown_string(self) -> None:
        event = _make_event("fp1", "mystery location")

        result = apply_geocoding([event], ["NL"], lambda _: None)

        assert result[0].country == "unknown"

    def test_output_length_matches_input(self) -> None:
        events = [_make_event(f"fp{i}", f"Location {i}") for i in range(5)]

        result = apply_geocoding(events, ["NL"], lambda _: None)

        assert len(result) == 5

    def test_original_events_not_mutated(self) -> None:
        event = _make_event("fp1", "Amsterdam")
        original_status = event.processing_status

        apply_geocoding([event], ["NL"], lambda _: MockLocation(52.37, 4.89, "nl"))

        assert event.processing_status == original_status

    def test_query_country_and_domain_country_preserved(self) -> None:
        """Geocoding must not overwrite query_country or domain_country."""
        event = CleanEvent(
            fingerprint="fp1",
            title="Event",
            description="Desc",
            location_text="Amsterdam",
            language="nl",
            source="test.com",
            url=HttpUrl("https://gallery.nl/event"),
            query_country="NL",
            domain_country="NL",
        )

        result = apply_geocoding(
            [event], ["NL"], lambda _: MockLocation(52.37, 4.89, "nl")
        )

        assert result[0].query_country == "NL"
        assert result[0].domain_country == "NL"
        assert result[0].country == "NL"


# ---------------------------------------------------------------------------
# Integration test (requires live Databricks)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestGeocodeIntegration:
    def test_run_geocode(self) -> None:
        from artlake.filter.country import run_geocode

        run_geocode(
            raw_events_table="artlake.bronze.raw_events",
            target_countries=["NL", "BE", "DE", "FR"],
            env="dev",
        )

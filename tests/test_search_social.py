"""Tests for social media site-scoped search (search/social.py)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from artlake.models.event import RawEvent
from artlake.search.models import SearchQuery
from artlake.search.social import (
    _build_social_queries,
    _make_event,
    load_platforms,
    search_social,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_QUERY_NL = SearchQuery(
    keyword_en="art exhibition",
    country_code="NL",
    country_name="Netherlands",
    language="nl",
    query="kunsttentoonstelling Nederland",
)

_QUERY_DE = SearchQuery(
    keyword_en="open call",
    country_code="DE",
    country_name="Deutschland",
    language="de",
    query="open call Kunst Deutschland",
)

_DDG_RESULT = {
    "title": "Open Call — Facebook Events",
    "href": "https://www.facebook.com/events/123456789",
    "body": "Galerie Amsterdam roept kunstenaars op.",
}

_PLATFORMS = {
    "facebook": "site:facebook.com/events",
    "instagram": "site:instagram.com",
    "linkedin": "site:linkedin.com",
}

_PLATFORMS_FB_ONLY = {"facebook": "site:facebook.com/events"}
_PLATFORMS_FB_IG = {
    "facebook": "site:facebook.com/events",
    "instagram": "site:instagram.com",
}


# ---------------------------------------------------------------------------
# load_platforms
# ---------------------------------------------------------------------------


class TestLoadPlatforms:
    def test_parses_yaml(self, tmp_path: Path) -> None:
        yml = tmp_path / "social_platforms.yml"
        yml.write_text(
            'facebook: "site:facebook.com/events"\ninstagram: "site:instagram.com"\n'
        )
        platforms = load_platforms(yml)
        assert platforms == {
            "facebook": "site:facebook.com/events",
            "instagram": "site:instagram.com",
        }

    def test_returns_dict(self, tmp_path: Path) -> None:
        yml = tmp_path / "social_platforms.yml"
        yml.write_text('linkedin: "site:linkedin.com"\n')
        assert isinstance(load_platforms(yml), dict)


# ---------------------------------------------------------------------------
# _build_social_queries
# ---------------------------------------------------------------------------


class TestBuildSocialQueries:
    def test_produces_one_tuple_per_query_per_platform(self) -> None:
        tuples = _build_social_queries([_QUERY_NL], _PLATFORMS_FB_IG)
        assert len(tuples) == 2

    def test_query_string_contains_site_operator(self) -> None:
        tuples = _build_social_queries([_QUERY_NL], _PLATFORMS_FB_ONLY)
        query_str, language, source, country_code = tuples[0]
        assert "site:facebook.com/events" in query_str
        assert "kunsttentoonstelling Nederland" in query_str
        assert language == "nl"
        assert source == "facebook"
        assert country_code == "NL"

    def test_all_three_platforms(self) -> None:
        tuples = _build_social_queries([_QUERY_NL], _PLATFORMS)
        sources = [t[2] for t in tuples]
        assert set(sources) == {"facebook", "instagram", "linkedin"}

    def test_multiple_queries_cross_product(self) -> None:
        tuples = _build_social_queries([_QUERY_NL, _QUERY_DE], _PLATFORMS_FB_IG)
        assert len(tuples) == 4

    def test_country_code_propagated(self) -> None:
        tuples = _build_social_queries([_QUERY_NL, _QUERY_DE], _PLATFORMS_FB_ONLY)
        country_codes = [t[3] for t in tuples]
        assert country_codes == ["NL", "DE"]

    def test_empty_queries_returns_empty(self) -> None:
        assert _build_social_queries([], _PLATFORMS) == []

    def test_empty_platforms_returns_empty(self) -> None:
        assert _build_social_queries([_QUERY_NL], {}) == []


# ---------------------------------------------------------------------------
# _make_event
# ---------------------------------------------------------------------------


class TestMakeEvent:
    def test_valid_result_facebook(self) -> None:
        event = _make_event(_DDG_RESULT, "nl", "facebook")
        assert event is not None
        assert str(event.url) == "https://www.facebook.com/events/123456789"
        assert event.source == "facebook"
        assert event.language == "nl"

    def test_source_reflects_platform(self) -> None:
        for source in ("facebook", "instagram", "linkedin"):
            event = _make_event(_DDG_RESULT, "nl", source)
            assert event is not None
            assert event.source == source

    def test_missing_href_returns_none(self) -> None:
        result = {"title": "Some Title", "body": "body text"}
        assert _make_event(result, "nl", "facebook") is None

    def test_missing_title_returns_none(self) -> None:
        result = {"href": "https://facebook.com/events/1", "body": "body"}
        assert _make_event(result, "nl", "facebook") is None

    def test_empty_snippet_allowed(self) -> None:
        result = {
            "href": "https://www.instagram.com/p/abc",
            "title": "Art Event",
            "body": "",
        }
        event = _make_event(result, "fr", "instagram")
        assert event is not None
        assert event.snippet == ""


# ---------------------------------------------------------------------------
# search_social — happy path
# ---------------------------------------------------------------------------


class TestSearchSocial:
    def _make_ddgs(self, results: list[dict[str, str]]) -> MagicMock:
        mock = MagicMock()
        mock.text.return_value = results
        return mock

    def test_returns_raw_events(self) -> None:
        ddgs = self._make_ddgs([_DDG_RESULT])
        with patch("artlake.search.social.time.sleep"):
            events = search_social([_QUERY_NL], _PLATFORMS_FB_ONLY, ddgs=ddgs)
        assert len(events) == 1
        assert isinstance(events[0], RawEvent)
        assert events[0].source == "facebook"

    def test_source_set_per_platform(self) -> None:
        fb_result = {**_DDG_RESULT, "href": "https://facebook.com/events/1"}
        ig_result = {
            "title": "Instagram Art",
            "href": "https://www.instagram.com/p/abc",
            "body": "snippet",
        }
        ddgs = MagicMock()
        ddgs.text.side_effect = [[fb_result], [ig_result]]

        with patch("artlake.search.social.time.sleep"):
            events = search_social([_QUERY_NL], _PLATFORMS_FB_IG, ddgs=ddgs)

        assert len(events) == 2
        assert {e.source for e in events} == {"facebook", "instagram"}

    def test_language_preserved_from_query(self) -> None:
        ddgs = MagicMock()
        nl_result = {**_DDG_RESULT, "href": "https://facebook.com/events/1"}
        de_result = {
            "title": "LinkedIn Kunst",
            "href": "https://www.linkedin.com/events/2",
            "body": "snippet",
        }
        ddgs.text.side_effect = [[nl_result], [de_result]]

        with patch("artlake.search.social.time.sleep"):
            events = search_social([_QUERY_NL, _QUERY_DE], _PLATFORMS_FB_ONLY, ddgs=ddgs)

        assert events[0].language == "nl"
        assert events[1].language == "de"

    def test_empty_results_skipped(self) -> None:
        ddgs = self._make_ddgs([])
        with patch("artlake.search.social.time.sleep"):
            events = search_social([_QUERY_NL], _PLATFORMS_FB_ONLY, ddgs=ddgs)
        assert events == []

    def test_invalid_results_filtered(self) -> None:
        bad = {"body": "no title or href"}
        ddgs = self._make_ddgs([bad])
        with patch("artlake.search.social.time.sleep"):
            events = search_social([_QUERY_NL], _PLATFORMS_FB_ONLY, ddgs=ddgs)
        assert events == []

    def test_no_queries_returns_empty(self) -> None:
        ddgs = self._make_ddgs([])
        with patch("artlake.search.social.time.sleep"):
            events = search_social([], _PLATFORMS, ddgs=ddgs)
        assert events == []

    def test_sleep_called_between_queries(self) -> None:
        ddgs = MagicMock()
        ddgs.text.return_value = [_DDG_RESULT]

        with patch("artlake.search.social.time.sleep") as mock_sleep:
            search_social([_QUERY_NL], _PLATFORMS_FB_IG, ddgs=ddgs)

        assert mock_sleep.call_count == 2


# ---------------------------------------------------------------------------
# search_social — error handling
# ---------------------------------------------------------------------------


class TestSearchSocialErrorHandling:
    def test_rate_limit_skips_query(self) -> None:
        from ddgs.exceptions import RatelimitException

        ddgs = MagicMock()
        ddgs.text.side_effect = RatelimitException("rate limit")

        with patch("artlake.search.social.time.sleep"):
            events = search_social([_QUERY_NL], _PLATFORMS_FB_ONLY, ddgs=ddgs)

        assert events == []

    def test_rate_limit_continues_to_next_platform(self) -> None:
        from ddgs.exceptions import RatelimitException

        ddgs = MagicMock()
        ddgs.text.side_effect = [
            RatelimitException("rate limit"),
            [_DDG_RESULT],
        ]

        with patch("artlake.search.social.time.sleep"):
            events = search_social([_QUERY_NL], _PLATFORMS_FB_IG, ddgs=ddgs)

        assert len(events) == 1
        assert events[0].source == "instagram"

    def test_timeout_skips_query(self) -> None:
        from ddgs.exceptions import TimeoutException

        ddgs = MagicMock()
        ddgs.text.side_effect = TimeoutException("timeout")

        with patch("artlake.search.social.time.sleep"):
            events = search_social([_QUERY_NL], _PLATFORMS_FB_ONLY, ddgs=ddgs)

        assert events == []

    def test_generic_ddg_exception_skips_query(self) -> None:
        from ddgs.exceptions import DDGSException  # type: ignore[import]

        ddgs = MagicMock()
        ddgs.text.side_effect = DDGSException("error")

        with patch("artlake.search.social.time.sleep"):
            events = search_social([_QUERY_NL], _PLATFORMS_FB_ONLY, ddgs=ddgs)

        assert events == []


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


class TestMain:
    def test_main_wires_load_search_write(self) -> None:
        from artlake.search.social import main

        mock_queries = [_QUERY_NL]
        mock_events = [
            RawEvent(
                fingerprint="abc123",
                url="https://www.facebook.com/events/1",  # type: ignore[arg-type]
                title="Test Event",
                snippet="snippet",
                source="facebook",
                language="nl",
            )
        ]

        with (
            patch(
                "artlake.search.social.load_queries", return_value=mock_queries
            ) as mock_load_queries,
            patch(
                "artlake.search.social.load_platforms", return_value=_PLATFORMS
            ) as mock_load_platforms,
            patch(
                "artlake.search.social.search_social", return_value=mock_events
            ) as mock_search,
            patch("artlake.search.social.write_results") as mock_write,
            patch(
                "sys.argv",
                ["artlake-search-social", "--queries", "config/output/queries.yml"],
            ),
        ):
            main()

        mock_load_queries.assert_called_once()
        mock_load_platforms.assert_called_once()
        mock_search.assert_called_once_with([_QUERY_NL], _PLATFORMS, max_results=10)
        mock_write.assert_called_once_with(
            mock_events, "artlake.staging.search_results", env="dev"
        )

    def test_main_writes_per_batch(self) -> None:
        from artlake.search.social import main

        queries = [_QUERY_NL, _QUERY_DE]
        batch_events = [
            RawEvent(
                fingerprint=f"fp{i}",
                url=f"https://www.facebook.com/events/{i}",  # type: ignore[arg-type]
                title=f"Event {i}",
                snippet="snippet",
                source="facebook",
                language="nl",
            )
            for i in range(2)
        ]

        with (
            patch("artlake.search.social.load_queries", return_value=queries),
            patch("artlake.search.social.load_platforms", return_value=_PLATFORMS),
            patch(
                "artlake.search.social.search_social", return_value=batch_events
            ) as mock_search,
            patch("artlake.search.social.write_results") as mock_write,
            patch(
                "sys.argv",
                [
                    "artlake-search-social",
                    "--queries",
                    "config/output/queries.yml",
                    "--batch-size",
                    "1",
                ],
            ),
        ):
            main()

        # batch_size=1 → one search+write call per query
        assert mock_search.call_count == 2
        assert mock_write.call_count == 2

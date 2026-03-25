"""Tests for DuckDuckGo web search (search/web.py)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from artlake.models.event import ProcessingStatus, RawEvent
from artlake.search.models import SearchQuery
from artlake.search.web import _make_event, search_web

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
    "title": "Galerie Amsterdam — Open Call 2026",
    "href": "https://example.com/open-call",
    "body": "Galerie Amsterdam roept kunstenaars op voor haar jaarlijkse open call.",
}


# ---------------------------------------------------------------------------
# _make_event
# ---------------------------------------------------------------------------


class TestMakeEvent:
    def test_valid_result(self) -> None:
        event = _make_event(_DDG_RESULT, "nl")
        assert event is not None
        assert str(event.url) == "https://example.com/open-call"
        assert event.title == "Galerie Amsterdam — Open Call 2026"
        assert event.snippet == _DDG_RESULT["body"]
        assert event.language == "nl"
        assert event.source == "duckduckgo"
        assert event.processing_status == ProcessingStatus.NEW

    def test_missing_href_returns_none(self) -> None:
        result = {"title": "Some Title", "body": "Some body"}
        assert _make_event(result, "nl") is None

    def test_missing_title_returns_none(self) -> None:
        result = {"href": "https://example.com", "body": "Some body"}
        assert _make_event(result, "nl") is None

    def test_empty_snippet_allowed(self) -> None:
        result = {"href": "https://example.com/event", "title": "Event", "body": ""}
        event = _make_event(result, "fr")
        assert event is not None
        assert event.snippet == ""


# ---------------------------------------------------------------------------
# search_web — happy path
# ---------------------------------------------------------------------------


class TestSearchWeb:
    def _make_ddgs(self, results: list[dict[str, str]]) -> MagicMock:
        mock = MagicMock()
        mock.text.return_value = results
        return mock

    def test_returns_raw_events(self) -> None:
        ddgs = self._make_ddgs([_DDG_RESULT])
        with patch("artlake.search.web.time.sleep"):
            events = search_web([_QUERY_NL], ddgs=ddgs)
        assert len(events) == 1
        assert isinstance(events[0], RawEvent)
        assert events[0].language == "nl"

    def test_language_tagged_per_query(self) -> None:
        nl_result = {**_DDG_RESULT, "href": "https://nl.example.com"}
        de_result = {
            "title": "Kunstausstellung Berlin",
            "href": "https://de.example.com",
            "body": "Offener Aufruf für Künstler in Deutschland.",
        }
        ddgs = MagicMock()
        ddgs.text.side_effect = [[nl_result], [de_result]]

        with patch("artlake.search.web.time.sleep"):
            events = search_web([_QUERY_NL, _QUERY_DE], ddgs=ddgs)

        assert len(events) == 2
        assert events[0].language == "nl"
        assert events[1].language == "de"

    def test_empty_results_skipped(self) -> None:
        ddgs = self._make_ddgs([])
        with patch("artlake.search.web.time.sleep"):
            events = search_web([_QUERY_NL], ddgs=ddgs)
        assert events == []

    def test_invalid_results_filtered(self) -> None:
        bad_result = {"body": "No title or href here"}
        ddgs = self._make_ddgs([bad_result])
        with patch("artlake.search.web.time.sleep"):
            events = search_web([_QUERY_NL], ddgs=ddgs)
        assert events == []

    def test_multiple_results_per_query(self) -> None:
        results = [
            {"title": f"Event {i}", "href": f"https://example.com/{i}", "body": "..."}
            for i in range(5)
        ]
        ddgs = self._make_ddgs(results)
        with patch("artlake.search.web.time.sleep"):
            events = search_web([_QUERY_NL], ddgs=ddgs)
        assert len(events) == 5

    def test_no_queries_returns_empty(self) -> None:
        ddgs = self._make_ddgs([])
        with patch("artlake.search.web.time.sleep"):
            events = search_web([], ddgs=ddgs)
        assert events == []


# ---------------------------------------------------------------------------
# search_web — error handling
# ---------------------------------------------------------------------------


class TestSearchWebErrorHandling:
    def test_rate_limit_skips_query(self) -> None:
        from ddgs.exceptions import RatelimitException

        ddgs = MagicMock()
        ddgs.text.side_effect = RatelimitException("rate limit")

        with patch("artlake.search.web.time.sleep"):
            events = search_web([_QUERY_NL], ddgs=ddgs)

        assert events == []

    def test_rate_limit_continues_to_next_query(self) -> None:
        from ddgs.exceptions import RatelimitException

        ddgs = MagicMock()
        ddgs.text.side_effect = [
            RatelimitException("rate limit"),
            [_DDG_RESULT],
        ]

        with patch("artlake.search.web.time.sleep"):
            events = search_web([_QUERY_NL, _QUERY_DE], ddgs=ddgs)

        assert len(events) == 1
        assert events[0].language == "de"

    def test_timeout_skips_query(self) -> None:
        from ddgs.exceptions import TimeoutException

        ddgs = MagicMock()
        ddgs.text.side_effect = TimeoutException("timeout")

        with patch("artlake.search.web.time.sleep"):
            events = search_web([_QUERY_NL], ddgs=ddgs)

        assert events == []

    def test_generic_ddg_exception_skips_query(self) -> None:
        from ddgs.exceptions import DDGSException  # type: ignore[import]

        ddgs = MagicMock()
        ddgs.text.side_effect = DDGSException("error")

        with patch("artlake.search.web.time.sleep"):
            events = search_web([_QUERY_NL], ddgs=ddgs)

        assert events == []

    def test_sleep_called_between_queries(self) -> None:
        ddgs = MagicMock()
        ddgs.text.return_value = [_DDG_RESULT]

        with patch("artlake.search.web.time.sleep") as mock_sleep:
            search_web([_QUERY_NL, _QUERY_DE], ddgs=ddgs)

        assert mock_sleep.call_count == 2


# ---------------------------------------------------------------------------
# write_results
# ---------------------------------------------------------------------------


class TestWriteResults:
    def test_writes_to_delta(self) -> None:
        import sys

        from artlake.search.web import write_results

        event = RawEvent(
            url="https://example.com/event",  # type: ignore[arg-type]
            title="Test Event",
            snippet="A snippet.",
            source="duckduckgo",
            language="nl",
            processing_status=ProcessingStatus.NEW,
        )

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df
        # Make SHOW CATALOGS return the catalog as already existing so
        # _ensure_catalog returns early without touching WorkspaceClient.
        mock_spark.sql.return_value.collect.return_value = [("artlake",)]

        mock_pyspark = MagicMock()
        mock_pyspark.sql.SparkSession = MagicMock()
        mock_pyspark.sql.SparkSession.builder.getOrCreate.return_value = mock_spark

        mock_pandas = MagicMock()

        with patch.dict(
            sys.modules,
            {
                "pyspark": mock_pyspark,
                "pyspark.sql": mock_pyspark.sql,
                "pandas": mock_pandas,
            },
        ):
            write_results([event], "artlake.staging.search_results")

        mock_spark.createDataFrame.assert_called_once()
        mock_df.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable.assert_called_once_with("artlake.staging.search_results")

    def test_empty_events_skips_write(self) -> None:
        import sys

        from artlake.search.web import write_results

        mock_pyspark = MagicMock()

        with patch.dict(
            sys.modules, {"pyspark": mock_pyspark, "pyspark.sql": mock_pyspark.sql}
        ):
            write_results([], "artlake.staging.search_results")
            mock_pyspark.sql.SparkSession.builder.getOrCreate.assert_not_called()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


class TestMain:
    def test_main_wires_load_search_write(self) -> None:
        from artlake.search.web import main

        mock_queries = [_QUERY_NL]
        mock_events = [
            RawEvent(
                url="https://example.com/event",  # type: ignore[arg-type]
                title="Test",
                snippet="snippet",
                source="duckduckgo",
                language="nl",
            )
        ]

        with (
            patch(
                "artlake.search.web.load_queries", return_value=mock_queries
            ) as mock_load,
            patch(
                "artlake.search.web.search_web", return_value=mock_events
            ) as mock_search,
            patch("artlake.search.web.write_results") as mock_write,
            patch(
                "sys.argv",
                ["artlake-search", "--queries", "config/output/queries.yml"],
            ),
        ):
            main()

        mock_load.assert_called_once()
        mock_search.assert_called_once_with([_QUERY_NL], max_results=10)
        mock_write.assert_called_once_with(
            mock_events, "artlake.staging.search_results", env="dev"
        )

    def test_main_writes_per_batch(self) -> None:
        from artlake.search.web import main

        queries = [_QUERY_NL, _QUERY_DE]
        batch_events = [
            RawEvent(
                url=f"https://example.com/{i}",  # type: ignore[arg-type]
                title=f"Event {i}",
                snippet="snippet",
                source="duckduckgo",
                language="nl",
            )
            for i in range(2)
        ]

        with (
            patch("artlake.search.web.load_queries", return_value=queries),
            patch(
                "artlake.search.web.search_web", return_value=batch_events
            ) as mock_search,
            patch("artlake.search.web.write_results") as mock_write,
            patch(
                "sys.argv",
                [
                    "artlake-search",
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

"""Tests for scrape/pages.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from artlake.models.event import ProcessingStatus
from artlake.scrape.pages import (
    extract_from_html,
    fetch_html,
    fetch_llms_txt,
    fingerprint,
    is_allowed_by_robots,
    scrape_url,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _html(name: str) -> str:
    return (FIXTURES / name).read_text()


# ---------------------------------------------------------------------------
# fingerprint
# ---------------------------------------------------------------------------


class TestFingerprint:
    def test_consistent_with_hashlib(self) -> None:
        import hashlib

        url = "https://example.com/event"
        assert fingerprint(url) == hashlib.sha256(url.encode()).hexdigest()

    def test_different_urls_different_fingerprints(self) -> None:
        assert fingerprint("https://a.com/1") != fingerprint("https://a.com/2")


# ---------------------------------------------------------------------------
# extract_from_html
# ---------------------------------------------------------------------------


class TestExtractFromHtml:
    def test_extracts_title(self) -> None:
        title, _, _ = extract_from_html(
            _html("event_with_pdf.html"), "https://example.com/"
        )
        assert title == "Open Call — Summer Exhibition 2025"

    def test_empty_title_when_missing(self) -> None:
        title, _, _ = extract_from_html(
            _html("event_no_title.html"), "https://kunstmarkt.de/"
        )
        assert title == ""

    def test_strips_script_and_style(self) -> None:
        _, raw_text, _ = extract_from_html(
            _html("event_basic.html"), "https://example.com/"
        )
        assert "console.log" not in raw_text
        assert "font-family" not in raw_text

    def test_raw_text_contains_page_content(self) -> None:
        _, raw_text, _ = extract_from_html(
            _html("event_basic.html"), "https://example.com/"
        )
        assert "Art Market Brussels" in raw_text

    def test_detects_pdf_links(self) -> None:
        _, _, artifact_urls = extract_from_html(
            _html("event_with_pdf.html"), "https://example.com/"
        )
        assert any(".pdf" in u for u in artifact_urls)

    def test_detects_poster_img(self) -> None:
        _, _, artifact_urls = extract_from_html(
            _html("event_with_pdf.html"), "https://example.com/"
        )
        assert any("flyer" in u or "images" in u for u in artifact_urls)

    def test_resolves_relative_pdf_links(self) -> None:
        _, _, artifact_urls = extract_from_html(
            _html("event_with_pdf.html"), "https://example.com/events/"
        )
        pdf_urls = [u for u in artifact_urls if u.endswith(".pdf")]
        assert any(u.startswith("https://example.com/") for u in pdf_urls)

    def test_no_artifacts_on_plain_page(self) -> None:
        _, _, artifact_urls = extract_from_html(
            _html("event_basic.html"), "https://example.com/"
        )
        assert artifact_urls == []

    def test_deduplicates_artifact_urls(self) -> None:
        html = """<html><body>
            <a href="https://example.com/doc.pdf">PDF</a>
            <a href="https://example.com/doc.pdf">PDF again</a>
        </body></html>"""
        _, _, artifact_urls = extract_from_html(html, "https://example.com/")
        assert len(artifact_urls) == 1

    def test_absolute_artifact_urls_from_fixture(self) -> None:
        _, _, artifact_urls = extract_from_html(
            _html("event_no_title.html"), "https://kunstmarkt.de/"
        )
        assert "https://kunstmarkt.de/bewerbung.pdf" in artifact_urls


# ---------------------------------------------------------------------------
# is_allowed_by_robots
# ---------------------------------------------------------------------------


class TestRobotsTxt:
    def test_allowed_when_robots_unreachable(self) -> None:
        with patch("artlake.scrape.pages.RobotFileParser") as mock_cls:
            rp = MagicMock()
            rp.read.side_effect = Exception("timeout")
            mock_cls.return_value = rp
            assert is_allowed_by_robots("https://example.com/event") is True

    def test_disallowed_when_robots_blocks_path(self) -> None:
        with patch("artlake.scrape.pages.RobotFileParser") as mock_cls:
            rp = MagicMock()
            rp.can_fetch.return_value = False
            mock_cls.return_value = rp
            assert is_allowed_by_robots("https://example.com/private") is False

    def test_allowed_when_robots_permits(self) -> None:
        with patch("artlake.scrape.pages.RobotFileParser") as mock_cls:
            rp = MagicMock()
            rp.can_fetch.return_value = True
            mock_cls.return_value = rp
            assert is_allowed_by_robots("https://example.com/event") is True


# ---------------------------------------------------------------------------
# fetch_llms_txt
# ---------------------------------------------------------------------------


class TestFetchLlmsTxt:
    def test_returns_content_on_200(self) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "# My Site\n> A description\n"
        with patch("artlake.scrape.pages.requests.get", return_value=mock_resp):
            result = fetch_llms_txt("https://example.com/event")
        assert result == "# My Site\n> A description\n"

    def test_returns_none_on_404(self) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        with patch("artlake.scrape.pages.requests.get", return_value=mock_resp):
            result = fetch_llms_txt("https://example.com/event")
        assert result is None

    def test_returns_none_on_request_error(self) -> None:
        with patch(
            "artlake.scrape.pages.requests.get",
            side_effect=requests.RequestException("timeout"),
        ):
            result = fetch_llms_txt("https://example.com/event")
        assert result is None


# ---------------------------------------------------------------------------
# fetch_html
# ---------------------------------------------------------------------------


class TestFetchHtml:
    def test_returns_html_on_200(self) -> None:
        mock_resp = MagicMock()
        mock_resp.text = "<html><body>Hello</body></html>"
        mock_resp.raise_for_status.return_value = None
        with patch("artlake.scrape.pages.requests.get", return_value=mock_resp):
            html, error = fetch_html("https://example.com/event")
        assert html == "<html><body>Hello</body></html>"
        assert error is None

    def test_returns_none_on_http_error(self) -> None:
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("404")
        with patch("artlake.scrape.pages.requests.get", return_value=mock_resp):
            html, error = fetch_html("https://example.com/event")
        assert html is None
        assert error is not None
        assert "HTTPError" in error

    def test_returns_none_on_connection_error(self) -> None:
        with patch(
            "artlake.scrape.pages.requests.get",
            side_effect=requests.ConnectionError("refused"),
        ):
            html, error = fetch_html("https://example.com/event")
        assert html is None
        assert error is not None
        assert "ConnectionError" in error


# ---------------------------------------------------------------------------
# scrape_url
# ---------------------------------------------------------------------------


class TestScrapeUrl:
    def _mock_robots(self, allowed: bool) -> MagicMock:
        rp = MagicMock()
        rp.can_fetch.return_value = allowed
        return rp

    def test_returns_failed_when_robots_disallows(self) -> None:
        with patch("artlake.scrape.pages.RobotFileParser") as mock_cls:
            mock_cls.return_value = self._mock_robots(False)
            page = scrape_url("https://example.com/event", respect_robots=True)
        assert page.processing_status == ProcessingStatus.FAILED
        assert page.error is not None

    def test_uses_llms_txt_when_available(self) -> None:
        with (
            patch("artlake.scrape.pages.RobotFileParser") as mock_cls,
            patch("artlake.scrape.pages.fetch_llms_txt", return_value="# Site docs"),
        ):
            mock_cls.return_value = self._mock_robots(True)
            page = scrape_url("https://example.com/event")
        assert page.raw_text == "# Site docs"
        assert page.processing_status == ProcessingStatus.NEW

    def test_falls_back_to_html_when_no_llms_txt(self) -> None:
        html = _html("event_with_pdf.html")
        with (
            patch("artlake.scrape.pages.fetch_llms_txt", return_value=None),
            patch("artlake.scrape.pages.fetch_html", return_value=(html, None)),
        ):
            page = scrape_url("https://example.com/event")
        assert "Open Call" in page.raw_text
        assert page.processing_status == ProcessingStatus.NEW

    def test_returns_failed_when_html_fetch_fails(self) -> None:
        with (
            patch("artlake.scrape.pages.fetch_llms_txt", return_value=None),
            patch(
                "artlake.scrape.pages.fetch_html",
                return_value=(None, "ConnectionError: refused"),
            ),
        ):
            page = scrape_url("https://example.com/event")
        assert page.processing_status == ProcessingStatus.FAILED
        assert page.error == "ConnectionError: refused"

    def test_fingerprint_matches_url(self) -> None:
        url = "https://example.com/event"
        with (
            patch("artlake.scrape.pages.fetch_llms_txt", return_value=None),
            patch(
                "artlake.scrape.pages.fetch_html", return_value=("<html></html>", None)
            ),
        ):
            page = scrape_url(url)
        assert page.fingerprint == fingerprint(url)

    def test_artifact_urls_detected(self) -> None:
        html = _html("event_with_pdf.html")
        with (
            patch("artlake.scrape.pages.fetch_llms_txt", return_value=None),
            patch("artlake.scrape.pages.fetch_html", return_value=(html, None)),
        ):
            page = scrape_url("https://example.com/event")
        assert len(page.artifact_urls) > 0

    @pytest.mark.parametrize("status_code", [400, 403, 404, 500, 503])
    def test_http_errors_produce_failed_status(self, status_code: int) -> None:
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError(str(status_code))
        with (
            patch("artlake.scrape.pages.fetch_llms_txt", return_value=None),
            patch("artlake.scrape.pages.requests.get", return_value=mock_resp),
        ):
            page = scrape_url("https://example.com/event")
        assert page.processing_status == ProcessingStatus.FAILED
        assert page.error is not None
        assert "HTTPError" in page.error

"""Tests for scrape/download.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from artlake.models.event import EventArtifact, ProcessingStatus
from artlake.scrape.download import (
    content_hash,
    detect_artifact_type,
    download_artifact,
    make_artifact,
    volume_path,
)
from artlake.scrape.pages import fingerprint as url_fingerprint

# ---------------------------------------------------------------------------
# content_hash
# ---------------------------------------------------------------------------


class TestContentHash:
    def test_deterministic(self) -> None:
        data = b"hello world"
        assert content_hash(data) == content_hash(data)

    def test_different_data_different_hash(self) -> None:
        assert content_hash(b"abc") != content_hash(b"xyz")

    def test_matches_hashlib(self) -> None:
        import hashlib

        data = b"some pdf content"
        assert content_hash(data) == hashlib.sha256(data).hexdigest()

    def test_empty_bytes(self) -> None:
        result = content_hash(b"")
        assert len(result) == 64  # sha256 hex digest


# ---------------------------------------------------------------------------
# detect_artifact_type
# ---------------------------------------------------------------------------


class TestDetectArtifactType:
    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/rules.pdf",
            "https://example.com/path/to/RULES.PDF",
            "https://example.com/open_call.pdf?token=abc",
        ],
    )
    def test_pdf_from_extension(self, url: str) -> None:
        assert detect_artifact_type(url) == "pdf"

    def test_pdf_from_content_type(self) -> None:
        assert (
            detect_artifact_type("https://example.com/file", "application/pdf") == "pdf"
        )

    def test_pdf_content_type_wins_over_image_ext(self) -> None:
        # Content-Type takes precedence when it says pdf
        assert (
            detect_artifact_type("https://example.com/file.jpg", "application/pdf")
            == "pdf"
        )

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/poster.jpg",
            "https://example.com/poster.jpeg",
            "https://example.com/poster.png",
            "https://example.com/poster.webp",
        ],
    )
    def test_image_from_extension(self, url: str) -> None:
        assert detect_artifact_type(url) == "image"

    def test_image_fallback_for_unknown_extension(self) -> None:
        assert detect_artifact_type("https://example.com/file.xyz") == "image"

    def test_image_when_no_content_type(self) -> None:
        assert detect_artifact_type("https://example.com/poster.png", None) == "image"


# ---------------------------------------------------------------------------
# volume_path
# ---------------------------------------------------------------------------


class TestVolumePath:
    def test_basic_path(self) -> None:
        path = volume_path(
            "/Volumes/artlake/volumes/raw_artifacts",
            "abc123",
            "https://example.com/poster.jpg",
        )
        assert path == "/Volumes/artlake/volumes/raw_artifacts/abc123/poster.jpg"

    def test_strips_trailing_slash_from_root(self) -> None:
        path = volume_path(
            "/Volumes/artlake/volumes/raw_artifacts/",
            "fp1",
            "https://example.com/doc.pdf",
        )
        assert path == "/Volumes/artlake/volumes/raw_artifacts/fp1/doc.pdf"

    def test_no_filename_uses_url_fingerprint(self) -> None:
        url = "https://example.com/"
        path = volume_path("/Volumes/root", "evfp", url)
        assert path == f"/Volumes/root/evfp/{url_fingerprint(url)}"

    def test_preserves_filename_with_query_string(self) -> None:
        # PurePosixPath strips the query string
        path = volume_path(
            "/Volumes/root",
            "evfp",
            "https://example.com/open_call.pdf?token=abc",
        )
        assert path.endswith("/open_call.pdf")

    def test_event_fingerprint_in_path(self) -> None:
        fp = "deadbeef"
        path = volume_path("/Volumes/root", fp, "https://example.com/img.png")
        assert f"/{fp}/" in path


# ---------------------------------------------------------------------------
# download_artifact
# ---------------------------------------------------------------------------


class TestDownloadArtifact:
    def _mock_response(
        self,
        content: bytes = b"file content",
        content_type: str = "application/pdf",
        content_length: str | None = None,
    ) -> MagicMock:
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.headers = {"Content-Type": content_type}
        if content_length is not None:
            resp.headers["Content-Length"] = content_length

        # iter_content yields the full content in one chunk
        resp.iter_content.return_value = iter([content])
        return resp

    def test_success_returns_data_and_content_type(self) -> None:
        data = b"%PDF-1.4 content"
        mock_resp = self._mock_response(data, "application/pdf")
        with patch("artlake.scrape.download.requests.get", return_value=mock_resp):
            result_data, ct, error = download_artifact("https://example.com/doc.pdf")
        assert result_data == data
        assert ct == "application/pdf"
        assert error is None

    def test_strips_charset_from_content_type(self) -> None:
        mock_resp = self._mock_response(b"data", "image/jpeg; charset=utf-8")
        with patch("artlake.scrape.download.requests.get", return_value=mock_resp):
            _, ct, _ = download_artifact("https://example.com/img.jpg")
        assert ct == "image/jpeg"

    def test_skips_when_content_length_exceeds_max(self) -> None:
        mock_resp = self._mock_response(b"x", content_length="104857601")  # 100MB+1
        with patch("artlake.scrape.download.requests.get", return_value=mock_resp):
            data, _, error = download_artifact(
                "https://example.com/big.pdf", max_bytes=1024
            )
        assert data is None
        assert error is not None
        assert "too large" in error.lower()

    def test_skips_when_stream_exceeds_max(self) -> None:
        big_chunk = b"x" * 2048
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.headers = {"Content-Type": "application/pdf"}
        resp.iter_content.return_value = iter([big_chunk])
        with patch("artlake.scrape.download.requests.get", return_value=resp):
            data, _, error = download_artifact(
                "https://example.com/big.pdf", max_bytes=1024
            )
        assert data is None
        assert error is not None
        assert "too large" in error.lower()

    def test_returns_none_on_http_error(self) -> None:
        resp = MagicMock()
        resp.raise_for_status.side_effect = requests.HTTPError("404")
        with patch("artlake.scrape.download.requests.get", return_value=resp):
            data, _, error = download_artifact("https://example.com/gone.pdf")
        assert data is None
        assert error is not None
        assert "HTTPError" in error

    def test_returns_none_on_connection_error(self) -> None:
        with patch(
            "artlake.scrape.download.requests.get",
            side_effect=requests.ConnectionError("refused"),
        ):
            data, _, error = download_artifact("https://example.com/doc.pdf")
        assert data is None
        assert error is not None
        assert "ConnectionError" in error

    def test_none_content_type_when_header_missing(self) -> None:
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.headers = {}
        resp.iter_content.return_value = iter([b"data"])
        with patch("artlake.scrape.download.requests.get", return_value=resp):
            _, ct, _ = download_artifact("https://example.com/doc.pdf")
        assert ct is None


# ---------------------------------------------------------------------------
# make_artifact
# ---------------------------------------------------------------------------


class TestMakeArtifact:
    _URL = "https://example.com/poster.jpg"
    _FP = "evfp123"

    def test_constructs_valid_event_artifact(self) -> None:
        artifact = make_artifact(
            self._URL,
            self._FP,
            "image",
            "/Volumes/root/evfp123/poster.jpg",
            ProcessingStatus.DOWNLOADED,
            "abc123hash",
        )
        assert isinstance(artifact, EventArtifact)
        assert str(artifact.url) == self._URL
        assert artifact.event_id == self._FP
        assert artifact.artifact_type == "image"
        assert artifact.file_path == "/Volumes/root/evfp123/poster.jpg"
        assert artifact.processing_status == ProcessingStatus.DOWNLOADED
        assert artifact.content_hash == "abc123hash"

    def test_id_matches_url_fingerprint(self) -> None:
        artifact = make_artifact(
            self._URL, self._FP, "image", None, ProcessingStatus.FAILED
        )
        assert artifact.id == url_fingerprint(self._URL)

    def test_failed_artifact_has_no_file_path(self) -> None:
        artifact = make_artifact(
            self._URL, self._FP, "pdf", None, ProcessingStatus.FAILED
        )
        assert artifact.file_path is None
        assert artifact.content_hash is None

    def test_processing_status_preserved(self) -> None:
        for status in (ProcessingStatus.DOWNLOADED, ProcessingStatus.FAILED):
            artifact = make_artifact(self._URL, self._FP, "pdf", None, status)
            assert artifact.processing_status == status


# ---------------------------------------------------------------------------
# Integration test — UC Volume write
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_run_download_writes_to_volume() -> None:
    """Integration: download a public artifact and write EventArtifact to staging.

    Requires a live Databricks workspace with UC Volumes configured.
    Run with: uv run pytest -m integration
    """
    from artlake.scrape.download import run_download

    run_download(
        artifact_url="https://www.w3.org/WAI/WCAG21/wcag21.pdf",
        raw_events_table="artlake.bronze.raw_events",
        artifacts_table="artlake.bronze.artifacts",
        volume_root="/Volumes/artlake/volumes/raw_artifacts",
    )

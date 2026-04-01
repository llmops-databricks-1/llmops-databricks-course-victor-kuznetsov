"""Tests for process_artifacts/extract.py."""

from __future__ import annotations

import pytest

from artlake.artifacts.extract import make_raw_event_artifact
from artlake.models.event import ProcessingStatus, RawEventArtifact

# ---------------------------------------------------------------------------
# make_raw_event_artifact
# ---------------------------------------------------------------------------


class TestMakeRawEventArtifact:
    def test_done_status(self) -> None:
        artifact = make_raw_event_artifact(
            artifact_id="fp123",
            event_id="evfp456",
            artifact_type="pdf",
            file_path="/Volumes/artlake/volumes/event_artifacts/evfp456/open_call.pdf",
            extracted_text="Full text extracted from the PDF.",
            status=ProcessingStatus.DONE,
        )
        assert isinstance(artifact, RawEventArtifact)
        assert artifact.id == "fp123"
        assert artifact.event_id == "evfp456"
        assert artifact.artifact_type == "pdf"
        assert artifact.processing_status == ProcessingStatus.DONE
        assert artifact.extracted_text == "Full text extracted from the PDF."

    def test_failed_status(self) -> None:
        artifact = make_raw_event_artifact(
            artifact_id="fp999",
            event_id="evfp000",
            artifact_type="image",
            file_path="/Volumes/artlake/volumes/event_artifacts/evfp000/poster.jpg",
            extracted_text=None,
            status=ProcessingStatus.FAILED,
        )
        assert artifact.processing_status == ProcessingStatus.FAILED
        assert artifact.extracted_text is None

    def test_file_path_preserved(self) -> None:
        path = "/Volumes/artlake/volumes/event_artifacts/evfp/brochure.pdf"
        artifact = make_raw_event_artifact(
            artifact_id="fp",
            event_id="evfp",
            artifact_type="pdf",
            file_path=path,
            extracted_text="some text",
            status=ProcessingStatus.DONE,
        )
        assert artifact.file_path == path

    def test_processed_at_set(self) -> None:
        artifact = make_raw_event_artifact(
            artifact_id="fp",
            event_id="evfp",
            artifact_type="pdf",
            file_path="/Volumes/artlake/volumes/event_artifacts/evfp/f.pdf",
            extracted_text=None,
            status=ProcessingStatus.FAILED,
        )
        assert artifact.processed_at is not None

    def test_image_artifact_type(self) -> None:
        artifact = make_raw_event_artifact(
            artifact_id="fp",
            event_id="evfp",
            artifact_type="image",
            file_path="/Volumes/artlake/volumes/event_artifacts/evfp/poster.png",
            extracted_text="Some OCR text",
            status=ProcessingStatus.DONE,
        )
        assert artifact.artifact_type == "image"
        assert artifact.extracted_text == "Some OCR text"

    @pytest.mark.parametrize("status", [ProcessingStatus.DONE, ProcessingStatus.FAILED])
    def test_all_statuses(self, status: ProcessingStatus) -> None:
        artifact = make_raw_event_artifact(
            artifact_id="fp",
            event_id="evfp",
            artifact_type="pdf",
            file_path="/Volumes/artlake/volumes/event_artifacts/evfp/f.pdf",
            extracted_text=None,
            status=status,
        )
        assert artifact.processing_status == status


# ---------------------------------------------------------------------------
# Integration test (requires live Databricks)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_run_extract_writes_raw_event_artifact() -> None:
    """Integration: extract text from a UC Volume artifact.

    Requires a live Databricks workspace.
    Run with: uv run pytest -m integration
    """
    from artlake.artifacts.extract import run_extract

    run_extract(
        artifact_id="test-artifact-id",
        event_artifacts_table="artlake.bronze.event_artifacts",
        event_artifacts_text_table="artlake.bronze.event_artifacts_text",
    )

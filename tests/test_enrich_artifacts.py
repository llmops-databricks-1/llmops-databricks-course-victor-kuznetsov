"""Tests for enrich/artifacts.py."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from artlake.artifacts.enrich import (
    build_system_prompt,
    make_event_artifacts_details,
    parse_llm_response,
)
from artlake.models.event import EventArtifactsProcessed, ProcessingStatus

_SUMMARY_NONE: dict[str, str | None] = {
    "deadline": None,
    "requirements": None,
    "location": None,
    "fees": None,
}

# ---------------------------------------------------------------------------
# build_system_prompt
# ---------------------------------------------------------------------------


class TestBuildSystemPrompt:
    def test_contains_all_summary_fields(self) -> None:
        prompt = build_system_prompt()
        for field in ("deadline", "requirements", "location", "fees"):
            assert field in prompt

    def test_instructs_json_only_response(self) -> None:
        prompt = build_system_prompt()
        assert "JSON" in prompt

    def test_instructs_null_for_missing(self) -> None:
        prompt = build_system_prompt()
        assert "null" in prompt


# ---------------------------------------------------------------------------
# parse_llm_response
# ---------------------------------------------------------------------------


class TestParseLlmResponse:
    def test_happy_path(self) -> None:
        content = json.dumps(
            {
                "deadline": "15 April 2026",
                "requirements": "Portfolio of 10 works",
                "location": "Amsterdam, NL",
                "fees": "No entry fee",
            }
        )
        result = parse_llm_response(content)
        assert result["deadline"] == "15 April 2026"
        assert result["requirements"] == "Portfolio of 10 works"
        assert result["location"] == "Amsterdam, NL"
        assert result["fees"] == "No entry fee"

    def test_strips_markdown_fences(self) -> None:
        content = (
            "```json\n"
            '{"deadline": "1 May", "requirements": null, "location": null, "fees": null}'
            "\n```"
        )
        result = parse_llm_response(content)
        assert result["deadline"] == "1 May"

    def test_null_fields_become_none(self) -> None:
        content = json.dumps(
            {"deadline": None, "requirements": None, "location": "Berlin", "fees": None}
        )
        result = parse_llm_response(content)
        assert result["deadline"] is None
        assert result["requirements"] is None
        assert result["location"] == "Berlin"
        assert result["fees"] is None

    def test_missing_keys_become_none(self) -> None:
        content = json.dumps({"deadline": "Tomorrow"})
        result = parse_llm_response(content)
        assert result["deadline"] == "Tomorrow"
        assert result["requirements"] is None
        assert result["location"] is None
        assert result["fees"] is None

    def test_non_string_values_become_none(self) -> None:
        content = json.dumps(
            {"deadline": 42, "requirements": ["a", "b"], "location": True, "fees": None}
        )
        result = parse_llm_response(content)
        assert result["deadline"] is None
        assert result["requirements"] is None
        assert result["location"] is None

    def test_invalid_json_raises(self) -> None:
        with pytest.raises(json.JSONDecodeError):
            parse_llm_response("not json at all")


# ---------------------------------------------------------------------------
# make_event_artifacts_details
# ---------------------------------------------------------------------------


class TestMakeEventArtifactsProcessed:
    def _summary(self) -> dict[str, str | None]:
        return {
            "deadline": "1 June 2026",
            "requirements": "Open to all artists",
            "location": "Paris",
            "fees": "€20 entry fee",
        }

    def test_done_status(self) -> None:
        artifact = make_event_artifacts_details(
            artifact_id="fp123",
            event_id="evfp456",
            artifact_type="pdf",
            file_path="/Volumes/artlake/volumes/event_artifacts/evfp456/open_call.pdf",
            extracted_text="Call for submissions deadline 1 June 2026",
            summary=self._summary(),
            status=ProcessingStatus.DONE,
        )
        assert isinstance(artifact, EventArtifactsProcessed)
        assert artifact.id == "fp123"
        assert artifact.event_id == "evfp456"
        assert artifact.artifact_type == "pdf"
        assert artifact.processing_status == ProcessingStatus.DONE
        assert artifact.deadline == "1 June 2026"
        assert artifact.fees == "€20 entry fee"

    def test_failed_status_all_summary_none(self) -> None:
        artifact = make_event_artifacts_details(
            artifact_id="fp999",
            event_id="evfp000",
            artifact_type="image",
            file_path="/Volumes/artlake/volumes/event_artifacts/evfp000/poster.jpg",
            extracted_text=None,
            summary=_SUMMARY_NONE,
            status=ProcessingStatus.FAILED,
        )
        assert artifact.processing_status == ProcessingStatus.FAILED
        assert artifact.extracted_text is None
        assert artifact.deadline is None

    def test_file_path_preserved(self) -> None:
        path = "/Volumes/artlake/volumes/event_artifacts/evfp/brochure.pdf"
        artifact = make_event_artifacts_details(
            artifact_id="fp",
            event_id="evfp",
            artifact_type="pdf",
            file_path=path,
            extracted_text="some text",
            summary=_SUMMARY_NONE,
            status=ProcessingStatus.DONE,
        )
        assert artifact.file_path == path

    def test_processed_at_set(self) -> None:
        artifact = make_event_artifacts_details(
            artifact_id="fp",
            event_id="evfp",
            artifact_type="pdf",
            file_path="/Volumes/artlake/volumes/event_artifacts/evfp/f.pdf",
            extracted_text=None,
            summary=_SUMMARY_NONE,
            status=ProcessingStatus.FAILED,
        )
        assert artifact.processed_at is not None


# ---------------------------------------------------------------------------
# _extract_fields (mocked LLM)
# ---------------------------------------------------------------------------


class TestExtractFields:
    def _mock_client(self, response_content: str) -> MagicMock:
        client = MagicMock()
        choice = MagicMock()
        choice.message.content = response_content
        client.chat.completions.create.return_value = MagicMock(choices=[choice])
        return client

    def test_returns_parsed_summary(self) -> None:
        from artlake.artifacts.enrich import _extract_fields

        payload = json.dumps(
            {
                "deadline": "30 April 2026",
                "requirements": "CV + 5 images",
                "location": "Rotterdam",
                "fees": "Free",
            }
        )
        client = self._mock_client(payload)
        result = _extract_fields("Some extracted text", client, "mock-model")
        assert result["deadline"] == "30 April 2026"
        assert result["location"] == "Rotterdam"

    def test_passes_system_prompt_and_text(self) -> None:
        from artlake.artifacts.enrich import _extract_fields

        payload = json.dumps(
            {"deadline": None, "requirements": None, "location": None, "fees": None}
        )
        client = self._mock_client(payload)
        _extract_fields("artifact text here", client, "mock-model")

        call_kwargs = client.chat.completions.create.call_args
        messages = call_kwargs.kwargs["messages"]
        assert messages[0]["role"] == "system"
        assert messages[1]["role"] == "user"
        assert messages[1]["content"] == "artifact text here"

    def test_uses_zero_temperature(self) -> None:
        from artlake.artifacts.enrich import _extract_fields

        client = self._mock_client(
            json.dumps(
                {"deadline": None, "requirements": None, "location": None, "fees": None}
            )
        )
        _extract_fields("text", client, "mock-model")
        call_kwargs = client.chat.completions.create.call_args
        assert call_kwargs.kwargs["temperature"] == 0.0

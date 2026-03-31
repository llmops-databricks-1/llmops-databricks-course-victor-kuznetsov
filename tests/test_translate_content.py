"""Tests for translate/content.py."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, mock_open, patch

import pytest

from artlake.translate.content import (
    _translate_text,
    build_system_prompt,
    build_translation_payload,
    load_target_language,
    make_silver_artifact,
    make_silver_event,
    parse_translation_response,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 3, 31, 12, 0, 0, tzinfo=UTC)

_ARTIFACT_IDS = ["art001", "art002"]

_FULL_PAYLOAD: dict = {
    "event": {
        "title": "Tentoonstelling: Zichtbaarheid",
        "description": "Een groepstentoonstelling over hedendaagse kunst.",
        "location_text": "Amsterdam, Nederland",
    },
    "artifacts": [
        {
            "id": "art001",
            "extracted_text": "Oproep voor kunstenaars. Deadline 15 april.",
            "deadline": "15 april 2026",
            "requirements": "Portfolio van 10 werken",
            "location": "Rotterdam, NL",
            "fees": "Geen kosten",
        },
        {
            "id": "art002",
            "extracted_text": None,
            "deadline": None,
            "requirements": None,
            "location": None,
            "fees": None,
        },
    ],
}

_FULL_TRANSLATION_RESPONSE: dict = {
    "event": {
        "title": "Exhibition: Visibility",
        "description": "A group exhibition on contemporary art.",
        "location_text": "Amsterdam, Netherlands",
    },
    "artifacts": [
        {
            "id": "art001",
            "extracted_text": "Call for artists. Deadline 15 April.",
            "deadline": "15 April 2026",
            "requirements": "Portfolio of 10 works",
            "location": "Rotterdam, NL",
            "fees": "No costs",
        },
        {
            "id": "art002",
            "extracted_text": None,
            "deadline": None,
            "requirements": None,
            "location": None,
            "fees": None,
        },
    ],
}


# ---------------------------------------------------------------------------
# build_system_prompt
# ---------------------------------------------------------------------------


class TestBuildSystemPrompt:
    def test_mentions_target_language(self) -> None:
        prompt = build_system_prompt("English")
        assert "English" in prompt

    def test_instructs_json_only_response(self) -> None:
        prompt = build_system_prompt("en")
        assert "JSON" in prompt

    def test_preserves_null_instruction(self) -> None:
        prompt = build_system_prompt("en")
        assert "null" in prompt

    def test_no_add_remove_keys_instruction(self) -> None:
        prompt = build_system_prompt("en")
        assert "keys" in prompt


# ---------------------------------------------------------------------------
# build_translation_payload
# ---------------------------------------------------------------------------


class TestBuildTranslationPayload:
    def test_event_fields_present(self) -> None:
        payload = build_translation_payload(
            event_title="Title",
            event_description="Desc",
            event_location_text="Loc",
            artifacts=[],
        )
        assert payload["event"]["title"] == "Title"
        assert payload["event"]["description"] == "Desc"
        assert payload["event"]["location_text"] == "Loc"

    def test_artifacts_list_preserved(self) -> None:
        artifacts = [
            {
                "id": "a1",
                "extracted_text": "text",
                "deadline": "d",
                "requirements": "r",
                "location": "l",
                "fees": "f",
            }
        ]
        payload = build_translation_payload("T", "D", "L", artifacts)
        assert len(payload["artifacts"]) == 1
        assert payload["artifacts"][0]["id"] == "a1"
        assert payload["artifacts"][0]["extracted_text"] == "text"

    def test_artifact_none_fields_preserved(self) -> None:
        artifacts = [
            {
                "id": "a1",
                "extracted_text": None,
                "deadline": None,
                "requirements": None,
                "location": None,
                "fees": None,
            }
        ]
        payload = build_translation_payload("T", "D", "L", artifacts)
        assert payload["artifacts"][0]["extracted_text"] is None

    def test_empty_artifacts(self) -> None:
        payload = build_translation_payload("T", "D", "L", [])
        assert payload["artifacts"] == []


# ---------------------------------------------------------------------------
# parse_translation_response
# ---------------------------------------------------------------------------


class TestParseTranslationResponse:
    def test_happy_path_all_fields(self) -> None:
        content = json.dumps(_FULL_TRANSLATION_RESPONSE)
        result = parse_translation_response(content, _ARTIFACT_IDS)

        assert result["event"]["title"] == "Exhibition: Visibility"
        assert result["event"]["location_text"] == "Amsterdam, Netherlands"

        art001 = next(a for a in result["artifacts"] if a["id"] == "art001")
        assert art001["deadline"] == "15 April 2026"
        assert art001["extracted_text"] == "Call for artists. Deadline 15 April."

        art002 = next(a for a in result["artifacts"] if a["id"] == "art002")
        assert art002["extracted_text"] is None
        assert art002["deadline"] is None

    def test_strips_markdown_fences(self) -> None:
        content = "```json\n" + json.dumps(_FULL_TRANSLATION_RESPONSE) + "\n```"
        result = parse_translation_response(content, _ARTIFACT_IDS)
        assert result["event"]["title"] == "Exhibition: Visibility"

    def test_missing_artifact_in_response_defaults_to_none(self) -> None:
        response = {
            "event": {"title": "T", "description": "D", "location_text": "L"},
            "artifacts": [],
        }
        content = json.dumps(response)
        result = parse_translation_response(content, ["art001"])
        art = result["artifacts"][0]
        assert art["id"] == "art001"
        assert art["extracted_text"] is None
        assert art["deadline"] is None

    def test_non_string_event_values_become_none(self) -> None:
        response = {
            "event": {"title": 42, "description": ["a", "b"], "location_text": None},
            "artifacts": [],
        }
        result = parse_translation_response(json.dumps(response), [])
        assert result["event"]["title"] is None
        assert result["event"]["description"] is None
        assert result["event"]["location_text"] is None

    def test_invalid_json_raises(self) -> None:
        with pytest.raises(json.JSONDecodeError):
            parse_translation_response("not json", [])

    def test_no_artifacts_in_payload(self) -> None:
        response = {
            "event": {"title": "T", "description": "D", "location_text": "L"},
            "artifacts": [],
        }
        result = parse_translation_response(json.dumps(response), [])
        assert result["artifacts"] == []


# ---------------------------------------------------------------------------
# make_silver_event
# ---------------------------------------------------------------------------


class TestMakeSilverEvent:
    def _base_kwargs(self) -> dict:
        return {
            "fingerprint": "fp123",
            "url": "https://example.com/event",
            "source": "example.com",
            "category": "open_call",
            "title_original": "Tentoonstelling",
            "description_original": "Beschrijving",
            "location_text_original": "Amsterdam",
            "date_start": _NOW,
            "date_end": None,
            "lat": 52.37,
            "lng": 4.9,
            "country": "NL",
            "query_country": "NL",
            "domain_country": "NL",
            "language": "nl",
            "target_language": "en",
            "artifact_urls": ["https://example.com/call.pdf"],
            "artifact_paths": ["/Volumes/artlake/volumes/raw_artifacts/fp123/call.pdf"],
            "ingested_at": _NOW,
            "translated_title": "Exhibition",
            "translated_description": "Description",
            "translated_location_text": "Amsterdam",
        }

    def test_translated_fields_applied(self) -> None:
        event = make_silver_event(**self._base_kwargs())
        assert event.title == "Exhibition"
        assert event.description == "Description"

    def test_original_fields_preserved(self) -> None:
        event = make_silver_event(**self._base_kwargs())
        assert event.title_original == "Tentoonstelling"
        assert event.description_original == "Beschrijving"

    def test_fallback_to_original_when_translation_null(self) -> None:
        kwargs = self._base_kwargs()
        kwargs["translated_title"] = None
        kwargs["translated_description"] = None
        kwargs["translated_location_text"] = None
        event = make_silver_event(**kwargs)
        assert event.title == "Tentoonstelling"
        assert event.description == "Beschrijving"
        assert event.location_text == "Amsterdam"

    def test_language_fields_set(self) -> None:
        event = make_silver_event(**self._base_kwargs())
        assert event.language == "nl"
        assert event.target_language == "en"

    def test_fingerprint_and_category(self) -> None:
        event = make_silver_event(**self._base_kwargs())
        assert event.fingerprint == "fp123"
        assert event.category == "open_call"

    def test_geo_fields_preserved(self) -> None:
        event = make_silver_event(**self._base_kwargs())
        assert event.lat == pytest.approx(52.37)
        assert event.lng == pytest.approx(4.9)
        assert event.country == "NL"


# ---------------------------------------------------------------------------
# make_silver_artifact
# ---------------------------------------------------------------------------


class TestMakeSilverArtifact:
    def _base_kwargs(self) -> dict:
        return {
            "artifact_id": "art001",
            "event_id": "fp123",
            "artifact_type": "pdf",
            "file_path": "/Volumes/artlake/volumes/raw_artifacts/fp123/call.pdf",
            "extracted_text_original": "Oproep voor kunstenaars.",
            "processed_at": _NOW,
            "target_language": "en",
            "translated_extracted_text": "Call for artists.",
            "translated_deadline": "15 April 2026",
            "translated_requirements": "Portfolio of 10 works",
            "translated_location": "Rotterdam, NL",
            "translated_fees": "No costs",
        }

    def test_translated_fields_applied(self) -> None:
        artifact = make_silver_artifact(**self._base_kwargs())
        assert artifact.extracted_text == "Call for artists."
        assert artifact.deadline == "15 April 2026"
        assert artifact.fees == "No costs"

    def test_original_preserved(self) -> None:
        artifact = make_silver_artifact(**self._base_kwargs())
        assert artifact.extracted_text_original == "Oproep voor kunstenaars."

    def test_fallback_to_original_when_translation_null(self) -> None:
        kwargs = self._base_kwargs()
        kwargs["translated_extracted_text"] = None
        artifact = make_silver_artifact(**kwargs)
        assert artifact.extracted_text == "Oproep voor kunstenaars."

    def test_all_none_translated_fields(self) -> None:
        kwargs = self._base_kwargs()
        kwargs.update(
            translated_extracted_text=None,
            translated_deadline=None,
            translated_requirements=None,
            translated_location=None,
            translated_fees=None,
        )
        artifact = make_silver_artifact(**kwargs)
        assert artifact.deadline is None
        assert artifact.requirements is None
        assert artifact.location is None
        assert artifact.fees is None

    def test_identity_fields(self) -> None:
        artifact = make_silver_artifact(**self._base_kwargs())
        assert artifact.id == "art001"
        assert artifact.event_id == "fp123"
        assert artifact.artifact_type == "pdf"
        assert artifact.target_language == "en"


# ---------------------------------------------------------------------------
# load_target_language
# ---------------------------------------------------------------------------


class TestLoadTargetLanguage:
    def test_reads_target_language_field(self) -> None:
        yaml_content = "target_language: fr\nkeywords:\n  - open call\n"
        with patch("builtins.open", mock_open(read_data=yaml_content)):
            result = load_target_language("config/input/keywords.yml")
        assert result == "fr"

    def test_defaults_to_EN_when_field_missing(self) -> None:
        yaml_content = "keywords:\n  - open call\n"
        with patch("builtins.open", mock_open(read_data=yaml_content)):
            result = load_target_language("config/input/keywords.yml")
        assert result == "EN"


# ---------------------------------------------------------------------------
# _translate_text (mocked LLM)
# ---------------------------------------------------------------------------


class TestTranslateText:
    def _mock_client(self, response_content: str) -> MagicMock:
        client = MagicMock()
        choice = MagicMock()
        choice.message.content = response_content
        client.chat.completions.create.return_value = MagicMock(choices=[choice])
        return client

    def test_returns_parsed_translation(self) -> None:
        content = json.dumps(_FULL_TRANSLATION_RESPONSE)
        client = self._mock_client(content)
        result = _translate_text(_FULL_PAYLOAD, client, "mock-model", "en")
        assert result["event"]["title"] == "Exhibition: Visibility"
        art001 = next(a for a in result["artifacts"] if a["id"] == "art001")
        assert art001["deadline"] == "15 April 2026"

    def test_passes_system_prompt_and_payload(self) -> None:
        content = json.dumps(_FULL_TRANSLATION_RESPONSE)
        client = self._mock_client(content)
        _translate_text(_FULL_PAYLOAD, client, "mock-model", "en")

        call_kwargs = client.chat.completions.create.call_args
        messages = call_kwargs.kwargs["messages"]
        assert messages[0]["role"] == "system"
        assert "en" in messages[0]["content"]
        assert messages[1]["role"] == "user"
        user_payload = json.loads(messages[1]["content"])
        assert user_payload["event"]["title"] == "Tentoonstelling: Zichtbaarheid"

    def test_uses_zero_temperature(self) -> None:
        client = self._mock_client(json.dumps(_FULL_TRANSLATION_RESPONSE))
        _translate_text(_FULL_PAYLOAD, client, "mock-model", "en")
        call_kwargs = client.chat.completions.create.call_args
        assert call_kwargs.kwargs["temperature"] == 0.0

    def test_event_only_no_artifacts(self) -> None:
        payload_no_artifacts = {
            "event": {
                "title": "Expo",
                "description": "Desc",
                "location_text": "Paris",
            },
            "artifacts": [],
        }
        response = {
            "event": {"title": "Expo", "description": "Desc", "location_text": "Paris"},
            "artifacts": [],
        }
        client = self._mock_client(json.dumps(response))
        result = _translate_text(payload_no_artifacts, client, "mock-model", "en")
        assert result["event"]["title"] == "Expo"
        assert result["artifacts"] == []

"""Unit tests for artlake.categorise.examples."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import yaml

from artlake.categorise.examples import (
    CategoryExamples,
    FewShotExample,
    _call_llm,
    generate_examples,
    load_examples,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAMPLE_KEYWORDS: dict = {
    "category_keywords": {
        "open_call": {
            "en": ["open call", "call for artists"],
            "nl": ["open oproep"],
            "de": ["offener aufruf"],
            "fr": ["appel à candidatures"],
        },
        "exhibition": {
            "en": ["exhibition", "vernissage"],
            "nl": ["tentoonstelling"],
            "de": ["ausstellung"],
            "fr": ["exposition"],
        },
        "workshop": {
            "en": ["workshop", "masterclass"],
            "nl": ["workshop"],
            "de": ["workshop"],
            "fr": ["atelier"],
        },
        "market": {
            "en": ["art market", "art fair"],
            "nl": ["kunstmarkt"],
            "de": ["kunstmarkt"],
            "fr": ["marché d'art"],
        },
        "non_art": {
            "en": ["football match"],
            "nl": ["voetbalwedstrijd"],
            "de": ["fußballspiel"],
            "fr": ["match de football"],
        },
    }
}


def _mock_client(response_json: str) -> MagicMock:
    """Build a mock OpenAI client that returns a fixed response."""
    client = MagicMock()
    msg = MagicMock()
    msg.content = response_json
    client.chat.completions.create.return_value.choices = [MagicMock(message=msg)]
    return client


def _sample_examples_yaml(tmp_path: Path) -> Path:
    """Write a minimal category_examples.yml and return its path."""
    data = {
        "generated_at": "2026-01-01T00:00:00+00:00",
        "model": "test-model",
        "examples": {
            "open_call": {
                "en": [{"title": "Open Call 2025", "description": "Submit by March."}],
                "nl": [{"title": "Open Oproep", "description": "Deadline 1 april."}],
                "de": [{"title": "Offener Aufruf", "description": "Bis Ende März."}],
                "fr": [{"title": "Appel ouvert", "description": "Date limite fin mars."}],
            },
            "exhibition": {
                "en": [
                    {"title": "Summer Show", "description": "Gallery opening Friday."}
                ],
                "nl": [
                    {
                        "title": "Zomertentoonstelling",
                        "description": "Vernissage vrijdag.",
                    }
                ],
                "de": [
                    {"title": "Sommerausstellung", "description": "Eröffnung Freitag."}
                ],
                "fr": [
                    {
                        "title": "Exposition estivale",
                        "description": "Vernissage vendredi.",
                    }
                ],
            },
            "workshop": {
                "en": [
                    {"title": "Painting Workshop", "description": "Learn oil painting."}
                ],
                "nl": [
                    {
                        "title": "Schilderworkshop",
                        "description": "Leer olieverfschilderen.",
                    }
                ],
                "de": [{"title": "Malworkshop", "description": "Ölmalerei lernen."}],
                "fr": [
                    {"title": "Atelier peinture", "description": "Apprenez la peinture."}
                ],
            },
            "market": {
                "en": [{"title": "Art Fair", "description": "100 artists exhibiting."}],
                "nl": [{"title": "Kunstmarkt", "description": "100 kunstenaars."}],
                "de": [{"title": "Kunstmarkt", "description": "100 Künstler."}],
                "fr": [{"title": "Foire d'art", "description": "100 artistes."}],
            },
            "non_art": {
                "en": [{"title": "Football Match", "description": "City vs United."}],
                "nl": [
                    {"title": "Voetbalwedstrijd", "description": "Stad tegen United."}
                ],
                "de": [{"title": "Fußballspiel", "description": "Stadt gegen United."}],
                "fr": [{"title": "Match de foot", "description": "Ville contre United."}],
            },
        },
    }
    path = tmp_path / "category_examples.yml"
    path.write_text(yaml.dump(data, allow_unicode=True))
    return path


# ---------------------------------------------------------------------------
# load_examples
# ---------------------------------------------------------------------------


class TestLoadExamples:
    def test_loads_all_categories(self, tmp_path: Path) -> None:
        path = _sample_examples_yaml(tmp_path)
        result = load_examples(path)
        assert set(result.examples.keys()) == {
            "open_call",
            "exhibition",
            "workshop",
            "market",
            "non_art",
        }

    def test_loads_all_languages(self, tmp_path: Path) -> None:
        path = _sample_examples_yaml(tmp_path)
        result = load_examples(path)
        assert set(result.examples["open_call"].keys()) == {"en", "nl", "de", "fr"}

    def test_examples_are_few_shot_example_instances(self, tmp_path: Path) -> None:
        path = _sample_examples_yaml(tmp_path)
        result = load_examples(path)
        ex = result.examples["open_call"]["en"][0]
        assert isinstance(ex, FewShotExample)
        assert ex.title == "Open Call 2025"
        assert ex.description == "Submit by March."

    def test_metadata_preserved(self, tmp_path: Path) -> None:
        path = _sample_examples_yaml(tmp_path)
        result = load_examples(path)
        assert result.model == "test-model"
        assert result.generated_at == "2026-01-01T00:00:00+00:00"


# ---------------------------------------------------------------------------
# _call_llm
# ---------------------------------------------------------------------------


class TestCallLlm:
    def test_returns_few_shot_examples(self) -> None:
        payload = json.dumps(
            [
                {"title": "Open Call for Artists", "description": "Submit by April."},
                {"title": "Apply Now", "description": "Deadline approaching."},
            ]
        )
        client = _mock_client(payload)

        result = _call_llm(client, "test-model", "open_call", "en", ["open call"], n=2)

        assert len(result) == 2
        assert all(isinstance(ex, FewShotExample) for ex in result)
        assert result[0].title == "Open Call for Artists"

    def test_strips_markdown_fences(self) -> None:
        payload = '```json\n[{"title": "Show", "description": "Nice show."}]\n```'
        client = _mock_client(payload)

        result = _call_llm(client, "test-model", "exhibition", "en", ["exhibition"], n=1)

        assert len(result) == 1
        assert result[0].title == "Show"

    def test_passes_keywords_in_prompt(self) -> None:
        payload = json.dumps([{"title": "X", "description": "Y"}])
        client = _mock_client(payload)

        _call_llm(client, "test-model", "market", "fr", ["marché d'art", "foire"], n=1)

        call_kwargs = client.chat.completions.create.call_args
        user_message = call_kwargs.kwargs["messages"][1]["content"]
        assert "marché d'art" in user_message


# ---------------------------------------------------------------------------
# generate_examples
# ---------------------------------------------------------------------------


class TestGenerateExamples:
    def _keywords_file(self, tmp_path: Path) -> Path:
        path = tmp_path / "category_keywords.yml"
        path.write_text(yaml.dump(_SAMPLE_KEYWORDS))
        return path

    def _mock_client_for_generate(self) -> MagicMock:
        """Returns 2 examples for every call."""
        payload = json.dumps(
            [
                {"title": "Title A", "description": "Description A."},
                {"title": "Title B", "description": "Description B."},
            ]
        )
        return _mock_client(payload)

    def test_generates_and_writes_yaml(self, tmp_path: Path) -> None:
        output_path = tmp_path / "output" / "category_examples.yml"
        client = self._mock_client_for_generate()

        result = generate_examples(
            self._keywords_file(tmp_path),
            output_path,
            model="test-model",
            client=client,
            n_per_language=2,
        )

        assert output_path.exists()
        assert isinstance(result, CategoryExamples)

    def test_output_covers_all_categories(self, tmp_path: Path) -> None:
        output_path = tmp_path / "examples.yml"
        client = self._mock_client_for_generate()

        result = generate_examples(
            self._keywords_file(tmp_path),
            output_path,
            model="test-model",
            client=client,
        )

        assert set(result.examples.keys()) == {
            "open_call",
            "exhibition",
            "workshop",
            "market",
            "non_art",
        }

    def test_output_covers_all_languages(self, tmp_path: Path) -> None:
        output_path = tmp_path / "examples.yml"
        client = self._mock_client_for_generate()

        result = generate_examples(
            self._keywords_file(tmp_path),
            output_path,
            model="test-model",
            client=client,
        )

        for category in result.examples:
            assert set(result.examples[category].keys()) == {"en", "nl", "de", "fr"}

    def test_yaml_is_loadable(self, tmp_path: Path) -> None:
        """Round-trip: written YAML can be read back by load_examples."""
        output_path = tmp_path / "examples.yml"
        client = self._mock_client_for_generate()

        generate_examples(
            self._keywords_file(tmp_path),
            output_path,
            model="test-model",
            client=client,
        )
        reloaded = load_examples(output_path)

        assert isinstance(reloaded.examples["open_call"]["en"][0], FewShotExample)

    def test_llm_called_once_per_category_language_pair(self, tmp_path: Path) -> None:
        output_path = tmp_path / "examples.yml"
        client = self._mock_client_for_generate()

        generate_examples(
            self._keywords_file(tmp_path),
            output_path,
            model="test-model",
            client=client,
        )

        # 5 categories × 4 languages = 20 calls
        assert client.chat.completions.create.call_count == 20

    def test_creates_parent_directory(self, tmp_path: Path) -> None:
        output_path = tmp_path / "nested" / "dir" / "examples.yml"
        client = self._mock_client_for_generate()

        generate_examples(
            self._keywords_file(tmp_path),
            output_path,
            model="test-model",
            client=client,
        )

        assert output_path.exists()

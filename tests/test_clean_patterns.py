"""Tests for clean/patterns.py."""

from pathlib import Path
from unittest.mock import MagicMock

import yaml

from artlake.events.extract_dates import (
    build_field_re,
    generate_patterns,
    load_patterns,
)

_PAYLOAD = (
    '{"title_keywords": {"en": ["Title", "Name"], "nl": ["Titel", "Naam"]},'
    ' "location_keywords": {"en": ["Location", "Venue"], "nl": ["Locatie"]}}'
)


class TestBuildFieldRe:
    def test_matches_english_keyword(self) -> None:
        field_re = build_field_re({"en": ["Location", "Venue"]})
        m = field_re.search("Location: Amsterdam")
        assert m is not None
        assert m.group(1).strip() == "Amsterdam"

    def test_longer_keyword_wins(self) -> None:
        # "Adresse" must match before "Adres" to avoid partial match
        field_re = build_field_re({"nl": ["Adres"], "de": ["Adresse"]})
        m = field_re.search("Adresse: Paris")
        assert m is not None
        assert "Paris" in m.group(1)

    def test_no_match_without_colon(self) -> None:
        field_re = build_field_re({"en": ["Location"]})
        assert field_re.search("The location is Amsterdam") is None

    def test_multilingual(self) -> None:
        field_re = build_field_re({"en": ["Location"], "nl": ["Locatie"]})
        assert field_re.search("Locatie: Rotterdam") is not None

    def test_matches_title_keyword(self) -> None:
        field_re = build_field_re({"en": ["Title", "Name"], "nl": ["Titel"]})
        m = field_re.search("Titel: Kunstmarkt Amsterdam")
        assert m is not None
        assert "Kunstmarkt Amsterdam" in m.group(1)


class TestLoadPatterns:
    def test_loads_valid_yaml(self, tmp_path: Path) -> None:
        data = {
            "generated_at": "2026-01-01T00:00:00+00:00",
            "model": "test-model",
            "languages": ["en", "nl"],
            "target_countries": ["NL"],
            "title_keywords": {"en": ["Title"], "nl": ["Titel"]},
            "location_keywords": {"en": ["Location"], "nl": ["Locatie"]},
        }
        path = tmp_path / "patterns.yml"
        path.write_text(yaml.dump(data))
        patterns = load_patterns(path)
        assert patterns.languages == ["en", "nl"]
        assert patterns.target_countries == ["NL"]
        assert "en" in patterns.title_keywords
        assert "en" in patterns.location_keywords


class TestGeneratePatterns:
    def _mock_client(self, response_json: str) -> MagicMock:
        client = MagicMock()
        msg = MagicMock()
        msg.content = response_json
        client.chat.completions.create.return_value.choices = [MagicMock(message=msg)]
        return client

    def test_generates_and_writes(self, tmp_path: Path) -> None:
        keywords_yml = tmp_path / "keywords.yml"
        keywords_yml.write_text(
            "keywords:\n  - art\ncountries:\n"
            "  - code: NL\n    name: Netherlands\n    languages: [nl]\n"
        )
        output_path = tmp_path / "output" / "language_patterns.yml"
        client = self._mock_client(_PAYLOAD)

        patterns = generate_patterns(
            keywords_yml, output_path, model="test-model", client=client
        )

        assert output_path.exists()
        assert "en" in patterns.languages
        assert "nl" in patterns.languages
        assert "NL" in patterns.target_countries
        assert "Title" in patterns.title_keywords["en"]
        assert "Location" in patterns.location_keywords["en"]

    def test_always_includes_english(self, tmp_path: Path) -> None:
        keywords_yml = tmp_path / "keywords.yml"
        keywords_yml.write_text(
            "keywords:\n  - art\ncountries:\n"
            "  - code: NL\n    name: Netherlands\n    languages: [nl]\n"
        )
        output_path = tmp_path / "patterns.yml"
        client = self._mock_client(_PAYLOAD)
        patterns = generate_patterns(
            keywords_yml, output_path, model="test", client=client
        )
        assert "en" in patterns.languages

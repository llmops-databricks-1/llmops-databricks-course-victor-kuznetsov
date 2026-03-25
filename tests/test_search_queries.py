"""Tests for multilingual keyword generation and query loading."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from pydantic import ValidationError

from artlake.search.generate import (
    _parse_json_response,
    generate_queries,
)
from artlake.search.load import load_queries
from artlake.search.models import (
    CountryDef,
    KeywordConfig,
    QueriesOutput,
    SearchQuery,
)

# ---------------------------------------------------------------------------
# CountryDef
# ---------------------------------------------------------------------------


class TestCountryDef:
    def test_valid(self) -> None:
        country = CountryDef(code="BE", name="Belgium", languages=["nl", "fr"])
        assert country.code == "BE"
        assert country.languages == ["nl", "fr"]

    def test_missing_field(self) -> None:
        with pytest.raises(ValidationError):
            CountryDef(code="BE", name="Belgium")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# KeywordConfig
# ---------------------------------------------------------------------------


class TestKeywordConfig:
    def test_valid(self) -> None:
        config = KeywordConfig(
            keywords=["art exhibition", "art fair"],
            countries=[
                CountryDef(code="BE", name="Belgium", languages=["nl", "fr"]),
                CountryDef(code="NL", name="Netherlands", languages=["nl"]),
            ],
        )
        assert len(config.keywords) == 2
        assert len(config.countries) == 2

    def test_missing_keywords(self) -> None:
        with pytest.raises(ValidationError):
            KeywordConfig(
                countries=[CountryDef(code="BE", name="Belgium", languages=["nl"])],
            )  # type: ignore[call-arg]

    def test_empty_keywords(self) -> None:
        config = KeywordConfig(
            keywords=[],
            countries=[CountryDef(code="BE", name="Belgium", languages=["nl"])],
        )
        assert config.keywords == []


# ---------------------------------------------------------------------------
# SearchQuery
# ---------------------------------------------------------------------------


class TestSearchQuery:
    def test_valid(self) -> None:
        query = SearchQuery(
            keyword_en="art exhibition",
            country_code="BE",
            country_name="Belgium",
            language="nl",
            query="kunsttentoonstelling België",
        )
        assert query.query == "kunsttentoonstelling België"

    def test_missing_query(self) -> None:
        with pytest.raises(ValidationError):
            SearchQuery(
                keyword_en="art exhibition",
                country_code="BE",
                country_name="Belgium",
                language="nl",
            )  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# QueriesOutput
# ---------------------------------------------------------------------------


class TestQueriesOutput:
    def test_valid(self) -> None:
        output = QueriesOutput(
            generated_at="2026-03-25T10:30:00+00:00",
            model="databricks-llama-4-maverick",
            queries=[
                SearchQuery(
                    keyword_en="art exhibition",
                    country_code="BE",
                    country_name="Belgium",
                    language="nl",
                    query="kunsttentoonstelling België",
                )
            ],
        )
        assert len(output.queries) == 1

    def test_empty_queries(self) -> None:
        output = QueriesOutput(
            generated_at="2026-03-25T10:30:00+00:00",
            model="test-model",
            queries=[],
        )
        assert output.queries == []

    def test_missing_model(self) -> None:
        with pytest.raises(ValidationError):
            QueriesOutput(
                generated_at="2026-03-25T10:30:00+00:00",
                queries=[],
            )  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# _parse_json_response
# ---------------------------------------------------------------------------


class TestParseJsonResponse:
    def test_plain_json(self) -> None:
        content = '[{"country_code": "BE", "language": "nl", "query": "test"}]'
        result = _parse_json_response(content)
        assert len(result) == 1
        assert result[0]["country_code"] == "BE"

    def test_markdown_code_fence(self) -> None:
        content = (
            '```json\n[{"country_code": "DE", "language": "de", "query": "Kunst"}]\n```'
        )
        result = _parse_json_response(content)
        assert len(result) == 1
        assert result[0]["language"] == "de"

    def test_markdown_fence_no_language(self) -> None:
        content = '```\n[{"country_code": "FR", "language": "fr", "query": "expo"}]\n```'
        result = _parse_json_response(content)
        assert result[0]["query"] == "expo"


# ---------------------------------------------------------------------------
# generate_queries (with mocked LLM)
# ---------------------------------------------------------------------------


def _make_mock_client(
    triples: list[tuple[str, str, str]],
) -> MagicMock:
    """Create a mock OpenAI client that returns translations for given triples."""
    mock_client = MagicMock()

    def create_side_effect(
        **kwargs: object,
    ) -> MagicMock:
        responses = []
        for code, _name, lang in triples:
            responses.append(
                {
                    "country_code": code,
                    "language": lang,
                    "query": f"translated_{lang}_{code}",
                }
            )
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content=json.dumps(responses)))
        ]
        return mock_response

    mock_client.chat.completions.create = MagicMock(side_effect=create_side_effect)
    return mock_client


class TestGenerateQueries:
    def _write_keywords(self, path: Path) -> None:
        data = {
            "keywords": ["art exhibition", "art fair"],
            "countries": [
                {"code": "BE", "name": "Belgium", "languages": ["nl", "fr"]},
                {"code": "NL", "name": "Netherlands", "languages": ["nl"]},
            ],
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.dump(data))

    def test_generates_correct_count(self, tmp_path: Path) -> None:
        keywords_path = tmp_path / "input" / "keywords.yml"
        output_path = tmp_path / "output" / "queries.yml"
        self._write_keywords(keywords_path)

        # 2 keywords x 3 country-language combos (BE:nl, BE:fr, NL:nl)
        triples = [
            ("BE", "Belgium", "nl"),
            ("BE", "Belgium", "fr"),
            ("NL", "Netherlands", "nl"),
        ]
        mock_client = _make_mock_client(triples)

        result = generate_queries(keywords_path, output_path, client=mock_client)
        assert len(result.queries) == 6  # 2 keywords x 3 triples

    def test_writes_valid_yaml(self, tmp_path: Path) -> None:
        keywords_path = tmp_path / "input" / "keywords.yml"
        output_path = tmp_path / "output" / "queries.yml"
        self._write_keywords(keywords_path)

        triples = [
            ("BE", "Belgium", "nl"),
            ("BE", "Belgium", "fr"),
            ("NL", "Netherlands", "nl"),
        ]
        mock_client = _make_mock_client(triples)

        generate_queries(keywords_path, output_path, client=mock_client)

        assert output_path.exists()
        raw = yaml.safe_load(output_path.read_text())
        validated = QueriesOutput(**raw)
        assert len(validated.queries) == 6

    def test_model_name_in_output(self, tmp_path: Path) -> None:
        keywords_path = tmp_path / "input" / "keywords.yml"
        output_path = tmp_path / "output" / "queries.yml"
        self._write_keywords(keywords_path)

        triples = [
            ("BE", "Belgium", "nl"),
            ("BE", "Belgium", "fr"),
            ("NL", "Netherlands", "nl"),
        ]
        mock_client = _make_mock_client(triples)

        result = generate_queries(
            keywords_path,
            output_path,
            model="my-custom-model",
            client=mock_client,
        )
        assert result.model == "my-custom-model"

    def test_creates_output_directory(self, tmp_path: Path) -> None:
        keywords_path = tmp_path / "input" / "keywords.yml"
        output_path = tmp_path / "deep" / "nested" / "queries.yml"
        self._write_keywords(keywords_path)

        triples = [
            ("BE", "Belgium", "nl"),
            ("BE", "Belgium", "fr"),
            ("NL", "Netherlands", "nl"),
        ]
        mock_client = _make_mock_client(triples)

        generate_queries(keywords_path, output_path, client=mock_client)
        assert output_path.exists()

    def test_query_content(self, tmp_path: Path) -> None:
        keywords_path = tmp_path / "input" / "keywords.yml"
        output_path = tmp_path / "output" / "queries.yml"
        self._write_keywords(keywords_path)

        triples = [
            ("BE", "Belgium", "nl"),
            ("BE", "Belgium", "fr"),
            ("NL", "Netherlands", "nl"),
        ]
        mock_client = _make_mock_client(triples)

        result = generate_queries(keywords_path, output_path, client=mock_client)
        be_nl = [
            q for q in result.queries if q.country_code == "BE" and q.language == "nl"
        ]
        assert len(be_nl) == 2  # one per keyword
        assert all(q.country_name == "Belgium" for q in be_nl)


# ---------------------------------------------------------------------------
# load_queries
# ---------------------------------------------------------------------------


class TestLoadQueries:
    def test_load_valid(self, tmp_path: Path) -> None:
        data = {
            "generated_at": "2026-03-25T10:30:00+00:00",
            "model": "test-model",
            "queries": [
                {
                    "keyword_en": "art exhibition",
                    "country_code": "BE",
                    "country_name": "Belgium",
                    "language": "nl",
                    "query": "kunsttentoonstelling België",
                },
            ],
        }
        path = tmp_path / "queries.yml"
        path.write_text(yaml.dump(data))

        queries = load_queries(path)
        assert len(queries) == 1
        assert queries[0].keyword_en == "art exhibition"

    def test_load_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_queries(tmp_path / "nonexistent.yml")

    def test_load_invalid_yaml(self, tmp_path: Path) -> None:
        path = tmp_path / "bad.yml"
        path.write_text(yaml.dump({"generated_at": "x", "model": "m"}))  # missing queries
        with pytest.raises(ValidationError):
            load_queries(path)

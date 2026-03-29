"""Unit tests for artlake.categorise.llm."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from artlake.categorise.examples import CategoryExamples, FewShotExample
from artlake.categorise.llm import (
    _build_system_prompt,
    _classify_batch,
    _parse_batch_response,
    classify_uncertain_events,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_examples() -> CategoryExamples:
    """Minimal CategoryExamples for prompt construction tests."""
    ex = FewShotExample(title="Open Call 2025", description="Submit by March.")
    return CategoryExamples(
        generated_at="2026-01-01T00:00:00+00:00",
        model="test-model",
        examples={
            "open_call": {"en": [ex], "nl": [], "de": [], "fr": []},
            "exhibition": {
                "en": [FewShotExample(title="Summer Show", description="Opens Friday.")],
                "nl": [],
                "de": [],
                "fr": [],
            },
            "workshop": {"en": [], "nl": [], "de": [], "fr": []},
            "market": {"en": [], "nl": [], "de": [], "fr": []},
            "non_art": {"en": [], "nl": [], "de": [], "fr": []},
        },
    )


def _mock_client(response_json: str) -> MagicMock:
    client = MagicMock()
    msg = MagicMock()
    msg.content = response_json
    client.chat.completions.create.return_value.choices = [MagicMock(message=msg)]
    return client


# ---------------------------------------------------------------------------
# _build_system_prompt
# ---------------------------------------------------------------------------


class TestBuildSystemPrompt:
    def test_contains_all_category_names(self) -> None:
        prompt = _build_system_prompt(_make_examples())
        for cat in ("open_call", "exhibition", "workshop", "market", "non_art"):
            assert cat in prompt

    def test_contains_few_shot_example(self) -> None:
        prompt = _build_system_prompt(_make_examples())
        assert "Open Call 2025" in prompt

    def test_contains_valid_categories_instruction(self) -> None:
        prompt = _build_system_prompt(_make_examples())
        assert "uncertain" in prompt.lower()
        assert "open_call|exhibition|workshop|market|non_art" in prompt

    def test_empty_language_lists_dont_crash(self) -> None:
        """Categories with no examples in some languages should not raise."""
        examples = _make_examples()
        # All language lists empty for workshop
        examples.examples["workshop"] = {"en": [], "nl": [], "de": [], "fr": []}
        prompt = _build_system_prompt(examples)
        assert "workshop" in prompt


# ---------------------------------------------------------------------------
# _parse_batch_response
# ---------------------------------------------------------------------------


class TestParseBatchResponse:
    def test_valid_response_returns_correct_categories(self) -> None:
        fps = ["fp1", "fp2"]
        content = json.dumps(
            [
                {"fingerprint": "fp1", "category": "open_call"},
                {"fingerprint": "fp2", "category": "exhibition"},
            ]
        )
        result = _parse_batch_response(content, fps)
        assert result == [("fp1", "open_call"), ("fp2", "exhibition")]

    def test_missing_fingerprint_defaults_to_non_art(self) -> None:
        fps = ["fp1", "fp2"]
        content = json.dumps(
            [
                {"fingerprint": "fp1", "category": "workshop"},
            ]
        )
        result = _parse_batch_response(content, fps)
        assert result == [("fp1", "workshop"), ("fp2", "non_art")]

    def test_invalid_category_defaults_to_non_art(self) -> None:
        fps = ["fp1"]
        content = json.dumps(
            [
                {"fingerprint": "fp1", "category": "uncertain"},
            ]
        )
        result = _parse_batch_response(content, fps)
        assert result == [("fp1", "non_art")]

    def test_strips_markdown_json_fence(self) -> None:
        fps = ["fp1"]
        content = '```json\n[{"fingerprint": "fp1", "category": "market"}]\n```'
        result = _parse_batch_response(content, fps)
        assert result == [("fp1", "market")]

    def test_all_valid_categories_accepted(self) -> None:
        valid = ("open_call", "exhibition", "workshop", "market", "non_art")
        for cat in valid:
            fps = ["fp1"]
            content = json.dumps([{"fingerprint": "fp1", "category": cat}])
            result = _parse_batch_response(content, fps)
            assert result == [("fp1", cat)]

    def test_preserves_input_order(self) -> None:
        fps = ["c", "a", "b"]
        content = json.dumps(
            [
                {"fingerprint": "a", "category": "exhibition"},
                {"fingerprint": "b", "category": "workshop"},
                {"fingerprint": "c", "category": "market"},
            ]
        )
        result = _parse_batch_response(content, fps)
        assert [fp for fp, _ in result] == ["c", "a", "b"]
        assert result[0] == ("c", "market")


# ---------------------------------------------------------------------------
# _classify_batch
# ---------------------------------------------------------------------------


class TestClassifyBatch:
    def test_calls_api_and_parses_response(self) -> None:
        batch = [
            {"fingerprint": "fp1", "title": "Open call", "description": "Apply now"},
        ]
        response_json = json.dumps([{"fingerprint": "fp1", "category": "open_call"}])
        client = _mock_client(response_json)

        result = _classify_batch(client, "test-model", "system prompt", batch)

        assert result == [("fp1", "open_call")]
        client.chat.completions.create.assert_called_once()

    def test_sends_fingerprint_and_text_to_api(self) -> None:
        batch = [{"fingerprint": "fp1", "title": "Title", "description": "Desc"}]
        response_json = json.dumps([{"fingerprint": "fp1", "category": "exhibition"}])
        client = _mock_client(response_json)

        _classify_batch(client, "test-model", "sys", batch)

        user_message = client.chat.completions.create.call_args.kwargs["messages"][1][
            "content"
        ]
        parsed = json.loads(user_message)
        assert parsed[0]["fingerprint"] == "fp1"
        assert "Title" in parsed[0]["text"]
        assert "Desc" in parsed[0]["text"]

    def test_temperature_zero(self) -> None:
        batch = [{"fingerprint": "fp1", "title": "T", "description": "D"}]
        response_json = json.dumps([{"fingerprint": "fp1", "category": "market"}])
        client = _mock_client(response_json)

        _classify_batch(client, "test-model", "sys", batch)

        call_kwargs = client.chat.completions.create.call_args.kwargs
        assert call_kwargs["temperature"] == 0.0


# ---------------------------------------------------------------------------
# classify_uncertain_events
# ---------------------------------------------------------------------------


class TestClassifyUncertainEvents:
    def _make_events(self, n: int) -> list[dict[str, str]]:
        return [
            {"fingerprint": f"fp{i}", "title": f"Event {i}", "description": f"Desc {i}"}
            for i in range(n)
        ]

    def _client_returning(self, category: str) -> MagicMock:
        """Mock client that classifies all events in any batch as `category`."""
        client = MagicMock()

        def create(**kwargs: object) -> MagicMock:
            user_content = kwargs["messages"][1]["content"]  # type: ignore[index]
            items = json.loads(str(user_content))
            result_json = json.dumps(
                [
                    {"fingerprint": item["fingerprint"], "category": category}
                    for item in items
                ]
            )
            msg = MagicMock()
            msg.content = result_json
            response = MagicMock()
            response.choices = [MagicMock(message=msg)]
            return response

        client.chat.completions.create.side_effect = create
        return client

    def test_classifies_all_events(self) -> None:
        events = self._make_events(5)
        client = self._client_returning("open_call")

        results = classify_uncertain_events(
            events, _make_examples(), client, "model", batch_size=10, max_workers=1
        )

        assert len(results) == 5
        assert all(cat == "open_call" for _, cat in results)

    def test_mini_batching_creates_correct_number_of_calls(self) -> None:
        events = self._make_events(7)
        client = self._client_returning("exhibition")

        classify_uncertain_events(
            events, _make_examples(), client, "model", batch_size=3, max_workers=1
        )

        # 7 events / batch_size 3 → 3 calls (batches of 3, 3, 1)
        assert client.chat.completions.create.call_count == 3

    def test_empty_events_returns_empty(self) -> None:
        client = self._client_returning("workshop")

        results = classify_uncertain_events([], _make_examples(), client, "model")

        assert results == []
        client.chat.completions.create.assert_not_called()

    def test_single_event_single_call(self) -> None:
        events = self._make_events(1)
        client = self._client_returning("market")

        results = classify_uncertain_events(
            events, _make_examples(), client, "model", batch_size=10, max_workers=1
        )

        assert len(results) == 1
        assert client.chat.completions.create.call_count == 1

"""Tests for filter/dedup.py."""

from __future__ import annotations

import hashlib

from artlake.filter.dedup import dedup
from artlake.models.event import SeenUrl

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fp(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def _seen(url: str, title: str = "", source: str = "duckduckgo") -> SeenUrl:
    return SeenUrl(url=url, title=title, source=source, fingerprint=_fp(url))


def _result(url: str, title: str = "Title", source: str = "duckduckgo") -> dict[str, str]:
    return {"url": url, "title": title, "source": source}


# ---------------------------------------------------------------------------
# dedup — exact URL duplicates
# ---------------------------------------------------------------------------


class TestDedupExactDuplicates:
    def test_no_seen_urls_all_new(self) -> None:
        results = [_result("https://example.com/event")]
        new, dupes = dedup(results, seen=[])
        assert len(new) == 1
        assert len(dupes) == 0

    def test_exact_duplicate_against_seen(self) -> None:
        url = "https://example.com/event"
        results = [_result(url)]
        new, dupes = dedup(results, seen=[_seen(url)])
        assert len(new) == 0
        assert len(dupes) == 1

    def test_exact_duplicate_within_batch(self) -> None:
        url = "https://example.com/event"
        results = [_result(url), _result(url)]
        new, dupes = dedup(results, seen=[])
        assert len(new) == 1
        assert len(dupes) == 1

    def test_multiple_new_urls(self) -> None:
        results = [
            _result("https://example.com/event-1"),
            _result("https://example.com/event-2"),
            _result("https://example.com/event-3"),
        ]
        new, dupes = dedup(results, seen=[])
        assert len(new) == 3
        assert len(dupes) == 0

    def test_mix_of_new_and_seen(self) -> None:
        seen_url = "https://example.com/old"
        results = [
            _result("https://example.com/new"),
            _result(seen_url),
        ]
        new, dupes = dedup(results, seen=[_seen(seen_url)])
        assert len(new) == 1
        assert len(dupes) == 1
        assert str(new[0].url) == "https://example.com/new"


# ---------------------------------------------------------------------------
# dedup — aggregator case (same domain, different paths)
# ---------------------------------------------------------------------------


class TestDedupAggregatorCase:
    def test_same_domain_different_paths_are_distinct(self) -> None:
        results = [
            _result("https://artsy.net/show/event-1"),
            _result("https://artsy.net/show/event-2"),
        ]
        new, dupes = dedup(results, seen=[])
        assert len(new) == 2
        assert len(dupes) == 0

    def test_same_domain_same_path_is_duplicate(self) -> None:
        url = "https://artsy.net/show/event-1"
        results = [_result(url)]
        new, dupes = dedup(results, seen=[_seen(url)])
        assert len(new) == 0
        assert len(dupes) == 1


# ---------------------------------------------------------------------------
# dedup — fingerprint correctness
# ---------------------------------------------------------------------------


class TestDedupFingerprint:
    def test_fingerprint_is_sha256_of_url(self) -> None:
        url = "https://example.com/event"
        results = [_result(url)]
        new, _ = dedup(results, seen=[])
        assert new[0].fingerprint == _fp(url)

    def test_empty_results_returns_empty(self) -> None:
        new, dupes = dedup([], seen=[])
        assert new == []
        assert dupes == []

"""Functional test for search/web.py — runs against real DuckDuckGo API.

Usage:
    uv run python tests/functional_test_search_web.py

Requires internet access. Not part of the automated test suite.
"""

from __future__ import annotations

from artlake.search.models import SearchQuery
from artlake.search.web import search_web

_QUERIES = [
    SearchQuery(
        keyword_en="open call painting",
        country_code="NL",
        country_name="Nederland",
        language="nl",
        query="open call schilderkunst Nederland",
    ),
    SearchQuery(
        keyword_en="art exhibition",
        country_code="BE",
        country_name="Belgique",
        language="fr",
        query="exposition d'art Belgique",
    ),
]


def main() -> None:
    print(f"Running search for {len(_QUERIES)} queries...\n")
    events = search_web(_QUERIES, max_results=3)

    print(f"\n{'=' * 60}")
    print(f"Total results: {len(events)}")
    print(f"{'=' * 60}\n")

    for i, event in enumerate(events, 1):
        print(f"[{i}] {event.title}")
        print(f"    URL:      {event.url}")
        print(f"    Language: {event.language}")
        print(f"    Snippet:  {event.snippet[:100]}...")
        print()

    assert len(events) > 0, "Expected at least one result"
    assert all(e.source == "duckduckgo" for e in events)
    assert all(e.language in ("nl", "fr") for e in events)
    print("All assertions passed.")


if __name__ == "__main__":
    main()

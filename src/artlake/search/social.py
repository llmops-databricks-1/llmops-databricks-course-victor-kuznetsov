"""DuckDuckGo social media site-scoped search for art events."""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

import yaml
from loguru import logger

from artlake.models.event import ProcessingStatus, RawEvent
from artlake.search.load import load_queries
from artlake.search.models import SearchQuery
from artlake.search.web import write_results

if TYPE_CHECKING:
    from ddgs import DDGS


_RATE_LIMIT_SLEEP_S = 2.0


def load_platforms(platforms_path: Path) -> dict[str, str]:
    """Load social platform config from YAML.

    Args:
        platforms_path: Path to social_platforms.yml.

    Returns:
        Mapping of source name to site: search operator.
        E.g. {"facebook": "site:facebook.com/events", ...}
    """
    result: dict[str, str] = yaml.safe_load(platforms_path.read_text())
    return result


def _build_social_queries(
    queries: list[SearchQuery],
    platforms: dict[str, str],
) -> list[tuple[str, str, str]]:
    """Return (query_string, language, source) triples.

    Combines each SearchQuery with a site: operator for every platform.
    """
    return [
        (f"{q.query} {site}", q.language, name)
        for q in queries
        for name, site in platforms.items()
    ]


def _make_event(result: dict[str, str], language: str, source: str) -> RawEvent | None:
    """Map a single DDGS result dict to a RawEvent.

    Returns None if required fields (href, title) are missing.
    """
    url = result.get("href") or result.get("url")
    title = result.get("title", "")
    snippet = result.get("body") or result.get("snippet", "")

    if not url or not title:
        return None

    try:
        return RawEvent(
            url=url,  # type: ignore[arg-type]
            title=title,
            snippet=snippet,
            source=source,
            language=language,
            processing_status=ProcessingStatus.NEW,
        )
    except Exception:
        logger.warning("Skipping result with invalid URL: {}", url[:120])
        return None


def search_social(
    queries: list[SearchQuery],
    platforms: dict[str, str],
    *,
    max_results: int = 10,
    ddgs: DDGS | None = None,
) -> list[RawEvent]:
    """Execute site-scoped DuckDuckGo searches across social media platforms.

    Args:
        queries: Pre-generated search queries (from queries.yml).
        platforms: Mapping of source name to site: operator (from social_platforms.yml).
        max_results: Maximum results to fetch per query.
        ddgs: Optional DDGS instance (injected for testing).

    Returns:
        List of RawEvent objects with per-platform source and query language.
    """
    from ddgs import DDGS
    from ddgs.exceptions import DDGSException, RatelimitException

    client = ddgs or DDGS()
    events: list[RawEvent] = []
    social_queries = _build_social_queries(queries, platforms)

    for query_str, language, source in social_queries:
        logger.info("Searching: '{}' [source={}]", query_str, source)
        try:
            results = client.text(query_str, max_results=max_results)
        except RatelimitException:
            logger.warning(
                "Rate limited on query '{}' — sleeping {}s then skipping",
                query_str,
                _RATE_LIMIT_SLEEP_S,
            )
            time.sleep(_RATE_LIMIT_SLEEP_S)
            continue
        except DDGSException as exc:
            logger.warning("Search failed for query '{}': {}", query_str, exc)
            continue

        if not results:
            logger.warning("No results for query '{}'", query_str)
            continue

        for raw in results:
            event = _make_event(raw, language, source)
            if event is not None:
                events.append(event)

        time.sleep(_RATE_LIMIT_SLEEP_S)

    logger.info(
        "Collected {} social search results from {} queries",
        len(events),
        len(social_queries),
    )
    return events


def main() -> None:
    """Entry point for artlake-search-social wheel task."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Social media site-scoped art event search."
    )
    parser.add_argument(
        "--queries",
        type=Path,
        default=Path("config/output/queries.yml"),
    )
    parser.add_argument(
        "--platforms",
        type=Path,
        default=Path("config/input/social_platforms.yml"),
    )
    parser.add_argument(
        "--table",
        default="artlake.staging.search_results",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=10,
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of queries to process before writing results to Delta.",
    )
    parser.add_argument(
        "--env",
        default="dev",
        help="Deployment environment (dev/tst/acc/prd), used to resolve el-uc-{env}.",
    )
    args = parser.parse_args()

    queries = load_queries(args.queries)
    logger.info("Loaded {} queries from {}", len(queries), args.queries)

    platforms = load_platforms(args.platforms)
    logger.info("Loaded {} platforms from {}", len(platforms), args.platforms)

    for i in range(0, len(queries), args.batch_size):
        batch = queries[i : i + args.batch_size]
        logger.info(
            "Processing query batch {}/{} (queries {}-{})",
            i // args.batch_size + 1,
            -(-len(queries) // args.batch_size),
            i + 1,
            min(i + args.batch_size, len(queries)),
        )
        events = search_social(batch, platforms, max_results=args.max_results)
        write_results(events, args.table, env=args.env)

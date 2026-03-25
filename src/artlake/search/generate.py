"""Generate multilingual search queries using an LLM."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import backoff
import yaml
from loguru import logger
from openai import OpenAI

from artlake.search.models import (
    CountryDef,
    KeywordConfig,
    QueriesOutput,
    SearchQuery,
)

_SYSTEM_PROMPT = (
    "You are a multilingual search query translator for art events in Europe. "
    "You translate English search keywords into natural search queries that a "
    "native speaker would type into a search engine. Each query must include "
    "the country name in the target language. Be concise (2-5 words)."
)

_USER_PROMPT_TEMPLATE = """\
Translate the following English keyword into search queries for each \
(country, language) pair listed below.

English keyword: "{keyword}"

Generate a query for each of these:
{pairs}

Respond with ONLY a JSON array, no other text:
[
  {{"country_code": "XX", "language": "xx", "query": "translated query"}}
]"""


def _build_triples(
    countries: list[CountryDef],
) -> list[tuple[str, str, str]]:
    """Expand countries into (code, name, language) triples."""
    triples: list[tuple[str, str, str]] = []
    for country in countries:
        for lang in country.languages:
            triples.append((country.code, country.name, lang))
    return triples


def _parse_json_response(content: str) -> list[dict[str, str]]:
    """Parse JSON from LLM response, handling markdown code fences."""
    cleaned = re.sub(r"```(?:json)?\s*", "", content)
    cleaned = cleaned.strip()
    return json.loads(cleaned)  # type: ignore[no-any-return]


@backoff.on_exception(
    backoff.expo,
    Exception,
    max_tries=3,
)
def _call_llm(
    client: OpenAI,
    model: str,
    keyword: str,
    triples: list[tuple[str, str, str]],
) -> list[dict[str, str]]:
    """Call the LLM to translate a single keyword for all country-language pairs."""
    pairs_text = "\n".join(f"- {name} ({code}) in {lang}" for code, name, lang in triples)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {
                "role": "user",
                "content": _USER_PROMPT_TEMPLATE.format(
                    keyword=keyword, pairs=pairs_text
                ),
            },
        ],
        temperature=0.3,
        max_tokens=1024,
    )
    content = response.choices[0].message.content or ""
    return _parse_json_response(content)


def _create_default_client() -> OpenAI:
    """Create an OpenAI client using Databricks workspace auth (ADR-021)."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    host = w.config.host or ""
    token = w.tokens.create(lifetime_seconds=1200).token_value
    return OpenAI(
        api_key=token,
        base_url=f"{host.rstrip('/')}/serving-endpoints",
    )


def generate_queries(
    keywords_path: Path,
    output_path: Path,
    *,
    model: str = "databricks-llama-4-maverick",
    client: OpenAI | None = None,
) -> QueriesOutput:
    """Read keywords.yml, translate via LLM, write queries.yml.

    Args:
        keywords_path: Path to the input keywords YAML file.
        output_path: Path where the generated queries YAML will be written.
        model: LLM model name for the Databricks serving endpoint.
        client: Optional OpenAI client (injected for testing).

    Returns:
        The validated QueriesOutput with all generated queries.
    """
    raw = yaml.safe_load(keywords_path.read_text())
    config = KeywordConfig(**raw)

    if client is None:
        client = _create_default_client()

    triples = _build_triples(config.countries)

    all_queries: list[SearchQuery] = []
    for keyword in config.keywords:
        logger.info("Translating keyword: {}", keyword)
        results = _call_llm(client, model, keyword, triples)
        for item in results:
            # Find the country name from the triples
            country_name = next(
                name
                for code, name, lang in triples
                if code == item["country_code"] and lang == item["language"]
            )
            all_queries.append(
                SearchQuery(
                    keyword_en=keyword,
                    country_code=item["country_code"],
                    country_name=country_name,
                    language=item["language"],
                    query=item["query"],
                )
            )

    output = QueriesOutput(
        generated_at=datetime.now(tz=UTC).isoformat(),
        model=model,
        queries=all_queries,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.dump(
            output.model_dump(),
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )
    )
    logger.info("Wrote {} queries to {}", len(all_queries), output_path)
    return output


def main() -> None:
    """Databricks wheel task entry point for artlake-generate-queries.

    Reads CLI arguments passed via python_wheel_task parameters and
    delegates to generate_queries() with typed Path arguments.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate multilingual search queries via LLM."
    )
    parser.add_argument(
        "--keywords",
        type=Path,
        default=Path("config/input/keywords.yml"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("config/output/queries.yml"),
    )
    parser.add_argument(
        "--model",
        default="databricks-llama-4-maverick",
    )
    args = parser.parse_args()
    generate_queries(args.keywords, args.output, model=args.model)

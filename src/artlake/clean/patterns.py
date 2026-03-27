"""Generate and load multilingual extraction patterns for art event pages.

Entry point: artlake-generate-language-patterns

The patterns YAML is derived from config/input/keywords.yml via LLM and written
to config/output/language_patterns.yml. It drives all language-specific rule-based
extraction in clean_events:
  - languages       → passed to dateparser for date extraction
  - target_countries → used for TLD → country inference
  - title_keywords   → labels like "Titel:" used to find the event title in body text
  - location_keywords → labels like "Locatie:" used to find the venue/address
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import backoff
import yaml
from loguru import logger
from openai import OpenAI
from pydantic import BaseModel, ConfigDict

from artlake.search.models import KeywordConfig

_SYSTEM_PROMPT = (
    "You are a multilingual data extraction assistant for art event web pages."
)

_USER_PROMPT_TEMPLATE = """\
Given these language codes: {languages}

Generate field labels that appear on art event web pages immediately before a colon,
for two field types:

1. title: labels identifying the event name
   (e.g. "Title: Open Call" or "Titel: Kunstmarkt")
2. location: labels identifying the venue or address
   (e.g. "Location: Amsterdam" or "Adresse: Paris")

Return ONLY a JSON object with this structure:
{{
  "title_keywords": {{
    "en": ["Title", "Event", "Name"],
    "nl": ["Titel", "Evenement", "Naam"],
    ...
  }},
  "location_keywords": {{
    "en": ["Location", "Venue", "Address", "Place"],
    "nl": ["Locatie", "Adres"],
    ...
  }}
}}

Include English ("en") regardless of the input list. Keep each list concise (3-6 labels).
Languages: {languages}"""


class LanguagePatterns(BaseModel):
    """Schema for config/output/language_patterns.yml.

    Drives all language-specific behaviour in the clean_events pipeline.
    """

    model_config = ConfigDict(strict=True)

    generated_at: str
    model: str
    languages: list[str]
    target_countries: list[str]
    title_keywords: dict[str, list[str]]
    location_keywords: dict[str, list[str]]


def build_field_re(keywords: dict[str, list[str]]) -> re.Pattern[str]:
    """Build a compiled colon-based extraction regex from per-language keyword lists.

    Keywords are sorted by length descending so longer variants match first
    (e.g. "Adresse" before "Adres").
    """
    all_keywords: list[str] = []
    for kws in keywords.values():
        all_keywords.extend(kws)
    all_keywords.sort(key=len, reverse=True)
    escaped = [re.escape(kw) for kw in all_keywords]
    pattern = r"(?:^|\b)(?:" + "|".join(escaped) + r")\s*:\s*(.+)"
    return re.compile(pattern, re.IGNORECASE | re.MULTILINE)


def load_patterns(path: Path) -> LanguagePatterns:
    """Load LanguagePatterns from a YAML file."""
    raw = yaml.safe_load(path.read_text())
    return LanguagePatterns(**raw)


def _create_default_client() -> OpenAI:  # pragma: no cover
    """Create an OpenAI client using Databricks workspace auth."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    host = w.config.host or ""
    token = w.tokens.create(lifetime_seconds=1200).token_value
    return OpenAI(
        api_key=token,
        base_url=f"{host.rstrip('/')}/serving-endpoints",
    )


@backoff.on_exception(backoff.expo, Exception, max_tries=3)
def _call_llm(
    client: OpenAI,
    model: str,
    languages: list[str],
) -> dict[str, dict[str, list[str]]]:
    """Call the LLM to generate extraction keyword labels for the given languages.

    Returns a dict with keys ``title_keywords`` and ``location_keywords``,
    each mapping language code → list of field labels.
    """
    languages_str = ", ".join(languages)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {
                "role": "user",
                "content": _USER_PROMPT_TEMPLATE.format(languages=languages_str),
            },
        ],
        temperature=0.0,
        max_tokens=512,
    )
    content = response.choices[0].message.content or ""
    cleaned = re.sub(r"```(?:json)?\s*", "", content).strip()
    result: dict[str, dict[str, list[str]]] = json.loads(cleaned)
    return result


def generate_patterns(
    keywords_path: Path,
    output_path: Path,
    *,
    model: str = "databricks-meta-llama-3-3-70b-instruct",
    client: OpenAI | None = None,
) -> LanguagePatterns:
    """Read keywords.yml, call LLM to generate extraction patterns, write YAML.

    Args:
        keywords_path: Path to the input keywords YAML file.
        output_path: Path where the generated language_patterns YAML will be written.
        model: LLM model name for the Databricks serving endpoint.
        client: Optional OpenAI client (injected for testing).

    Returns:
        The validated LanguagePatterns with all generated keywords.
    """
    raw = yaml.safe_load(keywords_path.read_text())
    config = KeywordConfig(**raw)

    target_countries = [country.code for country in config.countries]

    lang_set: set[str] = {"en"}
    for country in config.countries:
        lang_set.update(country.languages)
    languages = sorted(lang_set)

    if client is None:
        client = _create_default_client()  # pragma: no cover

    logger.info("Generating extraction patterns for languages: {}", languages)
    result = _call_llm(client, model, languages)

    output = LanguagePatterns(
        generated_at=datetime.now(tz=UTC).isoformat(),
        model=model,
        languages=languages,
        target_countries=target_countries,
        title_keywords=result.get("title_keywords", {}),
        location_keywords=result.get("location_keywords", {}),
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
    logger.info("Wrote language patterns to {}", output_path)
    return output


def main() -> None:  # pragma: no cover
    """Databricks wheel task entry point for artlake-generate-language-patterns."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate multilingual extraction patterns via LLM."
    )
    parser.add_argument(
        "--keywords",
        type=Path,
        default=Path("config/input/keywords.yml"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("config/output/language_patterns.yml"),
    )
    parser.add_argument(
        "--model",
        default="databricks-meta-llama-3-3-70b-instruct",
    )
    args = parser.parse_args()
    generate_patterns(args.keywords, args.output, model=args.model)

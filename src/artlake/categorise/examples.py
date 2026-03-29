"""Generate LLM few-shot examples for event categorisation.

Entry point: artlake-generate-category-examples

Reads category_keywords.yml, calls the LLM to generate realistic short event
texts per category per language, and writes the result to
config/output/category_examples.yml.  The generated file is then used by
artlake-categorise-llm as few-shot context in its classification prompt.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import backoff
import yaml
from loguru import logger
from openai import OpenAI
from pydantic import BaseModel, ConfigDict

from artlake.categorise.rules import load_category_keywords

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CATEGORIES = ("open_call", "exhibition", "workshop", "market", "non_art")
_LANGUAGES = ("en", "nl", "de", "fr")

_CATEGORY_DESCRIPTIONS: dict[str, str] = {
    "open_call": (
        "A call for artists to submit work, proposals, or applications. "
        "Includes submission deadlines and application requirements."
    ),
    "exhibition": "A gallery show, vernissage, art display, or group/solo exhibition.",
    "workshop": "An art class, masterclass, residency, or hands-on learning event.",
    "market": "An art fair, craft fair, or artisan market where art is sold.",
    "non_art": "A clearly non-art event (sports, cooking, finance, etc.).",
}

_SYSTEM_PROMPT = (
    "You are a data generation assistant for an art event classification system. "
    "You generate realistic example event texts in multiple languages."
)

_USER_PROMPT_TEMPLATE = """\
Generate {n} realistic short event texts for the category "{category}" \
written in "{language}".

Category definition: {definition}

Keywords associated with this category: {keywords}

Return ONLY a JSON array with this structure:
[
  {{"title": "event title", "description": "1-2 sentence event description"}}
]

Use natural language that a real art event organiser would write. \
Do not include the category name literally in the text."""

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class FewShotExample(BaseModel):
    """A single few-shot example: event title + short description."""

    title: str
    description: str


class CategoryExamples(BaseModel):
    """Schema for config/output/category_examples.yml."""

    model_config = ConfigDict(strict=True)

    generated_at: str
    model: str
    examples: dict[str, dict[str, list[FewShotExample]]]


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------


def load_examples(path: Path) -> CategoryExamples:
    """Load CategoryExamples from a YAML file."""
    raw: dict[str, Any] = yaml.safe_load(path.read_text())
    examples: dict[str, dict[str, list[FewShotExample]]] = {
        category: {
            lang: [FewShotExample(**ex) for ex in exs] for lang, exs in langs.items()
        }
        for category, langs in raw["examples"].items()
    }
    return CategoryExamples(
        generated_at=raw["generated_at"],
        model=raw["model"],
        examples=examples,
    )


# ---------------------------------------------------------------------------
# LLM interaction
# ---------------------------------------------------------------------------


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
    category: str,
    language: str,
    keywords: list[str],
    n: int,
) -> list[FewShotExample]:
    """Call LLM to generate few-shot examples for one category + language pair."""
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {
                "role": "user",
                "content": _USER_PROMPT_TEMPLATE.format(
                    n=n,
                    category=category,
                    language=language,
                    definition=_CATEGORY_DESCRIPTIONS[category],
                    keywords=", ".join(keywords[:8]),
                ),
            },
        ],
        temperature=0.7,
        max_tokens=512,
    )
    content = response.choices[0].message.content or ""
    cleaned = re.sub(r"```(?:json)?\s*", "", content).strip()
    # Extract the first JSON array from the response — guards against preamble
    # text or trailing commentary that makes json.loads fail.
    match = re.search(r"\[.*?\]", cleaned, re.DOTALL)
    if not match:
        raise ValueError(f"No JSON array found in LLM response: {cleaned[:200]}")
    items: list[dict[str, str]] = json.loads(match.group())
    return [FewShotExample(**item) for item in items]


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def generate_examples(
    keywords_path: Path,
    output_path: Path,
    *,
    model: str = "databricks-meta-llama-3-3-70b-instruct",
    client: OpenAI | None = None,
    n_per_language: int = 2,
    overwrite: bool = False,
) -> CategoryExamples:
    """Generate few-shot examples per category per language and write to YAML.

    Reads category_keywords.yml to understand available categories and their
    keywords, then calls the LLM to generate ``n_per_language`` realistic event
    texts for each (category, language) pair.

    Args:
        keywords_path: Path to ``category_keywords.yml``.
        output_path: Path where generated YAML will be written.
        model: LLM model name for the Databricks serving endpoint.
        client: Optional OpenAI client (injected for testing).
        n_per_language: Examples to generate per category-language pair.
        overwrite: If False (default) and ``output_path`` already exists, load
            and return the cached file without calling the LLM.

    Returns:
        The validated :class:`CategoryExamples`.
    """
    if not overwrite and output_path.exists():
        logger.info(
            "Category examples already exist at {} — skipping generation", output_path
        )
        return load_examples(output_path)

    category_keywords = load_category_keywords(keywords_path)

    if client is None:
        client = _create_default_client()  # pragma: no cover

    examples: dict[str, dict[str, list[FewShotExample]]] = {}

    for category in _CATEGORIES:
        logger.info("Generating examples for category: {}", category)
        examples[category] = {}
        lang_kws = category_keywords.get(category, {})
        for language in _LANGUAGES:
            # Fall back to English keywords if language-specific ones are absent
            keywords = lang_kws.get(language, lang_kws.get("en", []))
            logger.info("  language: {}", language)
            result = _call_llm(
                client, model, category, language, keywords, n=n_per_language
            )
            examples[category][language] = result

    output_obj = CategoryExamples(
        generated_at=datetime.now(tz=UTC).isoformat(),
        model=model,
        examples=examples,
    )

    raw: dict[str, Any] = {
        "generated_at": output_obj.generated_at,
        "model": output_obj.model,
        "examples": {
            category: {
                lang: [ex.model_dump() for ex in exs] for lang, exs in langs.items()
            }
            for category, langs in output_obj.examples.items()
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.dump(raw, default_flow_style=False, allow_unicode=True, sort_keys=False)
    )
    logger.info("Wrote category examples to {}", output_path)
    return output_obj


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:  # pragma: no cover
    """Entry point for artlake-generate-category-examples wheel task."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate LLM few-shot examples for event categorisation."
    )
    parser.add_argument(
        "--category-keywords",
        type=Path,
        default=Path("config/input/category_keywords.yml"),
        help="Path to category_keywords.yml",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("config/output/category_examples.yml"),
        help="Path for the generated YAML output",
    )
    parser.add_argument(
        "--model",
        default="databricks-meta-llama-3-3-70b-instruct",
        help="Databricks serving endpoint model name",
    )
    parser.add_argument(
        "--n-per-language",
        type=int,
        default=2,
        help="Number of examples per category-language pair",
    )
    args = parser.parse_args()
    generate_examples(
        args.category_keywords,
        args.output,
        model=args.model,
        n_per_language=args.n_per_language,
    )

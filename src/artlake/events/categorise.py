"""Event categorisation — rules + LLM fallback (artlake-categorise entry point).

Reads EventDate records from bronze.event_dates where event_status IN
('future', 'undefined'), classifies each event via multilingual keyword
matching, resolves uncertain cases via LLM, and writes EventCategory records
to bronze.event_category.  No event is ever written with category='uncertain'.

Output categories: open_call / exhibition / workshop / market / non_art

Separate entry point artlake-generate-category-examples generates the
few-shot examples YAML used to guide LLM classification.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import backoff
import yaml
from loguru import logger
from openai import OpenAI
from pydantic import BaseModel, ConfigDict

from artlake.models.event import CategoryStatus

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ART_CATEGORIES = ("open_call", "exhibition", "workshop", "market")
_NON_ART = "non_art"
_UNCERTAIN = "uncertain"
_VALID_CATEGORIES = frozenset(
    {"open_call", "exhibition", "workshop", "market", "non_art"}
)
_ALL_CATEGORIES = (*_ART_CATEGORIES, _NON_ART)
_LANGUAGES = ("en", "nl", "de", "fr")

_CATEGORY_DESCRIPTIONS: dict[str, str] = {
    "open_call": "A call for artists to submit work, proposals, or applications.",
    "exhibition": "A gallery show, vernissage, or art display.",
    "workshop": "An art class, masterclass, or artist residency.",
    "market": "An art fair, craft fair, or artisan market.",
    "non_art": "Clearly not an art event (sports, cooking, finance, etc.).",
}

_CATEGORY_DESCRIPTIONS_LONG: dict[str, str] = {
    "open_call": (
        "A call for artists to submit work, proposals, or applications. "
        "Includes submission deadlines and application requirements."
    ),
    "exhibition": "A gallery show, vernissage, art display, or group/solo exhibition.",
    "workshop": "An art class, masterclass, residency, or hands-on learning event.",
    "market": "An art fair, craft fair, or artisan market where art is sold.",
    "non_art": "A clearly non-art event (sports, cooking, finance, etc.).",
}

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
# Rules-based classification
# ---------------------------------------------------------------------------


def load_category_keywords(path: Path) -> dict[str, dict[str, list[str]]]:
    """Load per-category keyword dictionaries from a YAML file.

    Expected top-level key: ``category_keywords``.
    Returns an empty dict if the key is absent.
    """
    with path.open() as fh:
        data: dict[str, Any] = yaml.safe_load(fh)
    result: dict[str, dict[str, list[str]]] = data.get("category_keywords", {})
    return result


def classify_text(
    title: str | None,
    description: str | None,
    category_keywords: dict[str, dict[str, list[str]]],
) -> str:
    """Return the best-matching category for an event.

    Matching strategy:
    1. Check specific art categories (open_call, exhibition, workshop, market)
       in priority order — first match wins.
    2. If none match, check non_art keywords as a last-resort filter.
    3. Default to "uncertain" (passed to LLM categoriser).

    Args:
        title: Event title (may be None).
        description: Event description (may be None).
        category_keywords: Mapping of category → language → keyword list,
            as returned by :func:`load_category_keywords`.

    Returns:
        One of: ``open_call``, ``exhibition``, ``workshop``, ``market``,
        ``non_art``, or ``uncertain``.
    """
    text = " ".join(filter(None, [title or "", description or ""])).lower()

    if not text.strip():
        return _UNCERTAIN

    # Art categories — checked first so art trumps non-art signals
    for category in _ART_CATEGORIES:
        for lang_kws in category_keywords.get(category, {}).values():
            if any(kw.lower() in text for kw in lang_kws):
                return category

    # Non-art last-resort filter
    for lang_kws in category_keywords.get(_NON_ART, {}).values():
        if any(kw.lower() in text for kw in lang_kws):
            return _NON_ART

    return _UNCERTAIN


# ---------------------------------------------------------------------------
# Few-shot examples
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
# LLM classification
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


def _format_example(ex: FewShotExample) -> str:
    return f'"{ex.title}: {ex.description}"'


def _build_system_prompt(examples: CategoryExamples) -> str:
    """Build the classification system prompt with category definitions and examples."""
    lines: list[str] = [
        "You are an art event classifier.",
        "Classify each event into exactly one of these categories:",
        "",
    ]
    for cat, desc in _CATEGORY_DESCRIPTIONS.items():
        lines.append(f"- {cat}: {desc}")

    lines.extend(["", "Few-shot examples (format: [category/language] text):"])
    for category, langs in examples.examples.items():
        for lang, exs in langs.items():
            if exs:
                lines.append(f"  [{category}/{lang}] {_format_example(exs[0])}")

    lines.extend(
        [
            "",
            "You will receive a JSON array of events with 'fingerprint' and 'text' fields.",  # noqa: E501
            "Respond ONLY with a JSON array in the same order:",
            '[{"fingerprint": "...", "category": "open_call|exhibition|workshop|market|non_art"}]',  # noqa: E501
            'Never output "uncertain". Always choose the most likely category.',
        ]
    )
    return "\n".join(lines)


def _parse_batch_response(content: str, expected_fps: list[str]) -> list[tuple[str, str]]:
    """Parse LLM batch response into (fingerprint, category) pairs."""
    cleaned = re.sub(r"```(?:json)?\s*", "", content).strip()
    items: list[dict[str, str]] = json.loads(cleaned)

    result_map: dict[str, str] = {}
    for item in items:
        fp = item.get("fingerprint", "")
        cat = item.get("category", "")
        if fp and cat in _VALID_CATEGORIES:
            result_map[fp] = cat

    return [(fp, result_map.get(fp, "non_art")) for fp in expected_fps]


@backoff.on_exception(backoff.expo, Exception, max_tries=3)
def _classify_batch(
    client: OpenAI,
    model: str,
    system_prompt: str,
    batch: list[dict[str, str]],
) -> list[tuple[str, str]]:
    """Send one mini-batch to the LLM and return (fingerprint, category) pairs."""
    expected_fps = [item["fingerprint"] for item in batch]
    user_content = json.dumps(
        [
            {
                "fingerprint": item["fingerprint"],
                "text": f"{item.get('title', '')} {item.get('description', '')}".strip(),
            }
            for item in batch
        ],
        ensure_ascii=False,
    )
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        temperature=0.0,
        max_tokens=1024,
    )
    content = response.choices[0].message.content or ""
    return _parse_batch_response(content, expected_fps)


def classify_uncertain_events(
    events: list[dict[str, str]],
    examples: CategoryExamples,
    client: OpenAI,
    model: str,
    batch_size: int = 10,
    max_workers: int = 4,
) -> list[tuple[str, str]]:
    """Classify uncertain events using mini-batched parallel LLM calls.

    Args:
        events: List of dicts with 'fingerprint', 'title', 'description'.
        examples: Few-shot examples loaded from category_examples.yml.
        client: OpenAI client.
        model: Model name.
        batch_size: Events per LLM call (default 10).
        max_workers: Thread pool size for parallel calls (default 4).

    Returns:
        List of ``(fingerprint, category)`` pairs — one per input event.
    """
    system_prompt = _build_system_prompt(examples)
    batches = [events[i : i + batch_size] for i in range(0, len(events), batch_size)]

    results: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_classify_batch, client, model, system_prompt, batch): batch
            for batch in batches
        }
        for future in as_completed(futures):
            results.extend(future.result())

    return results


# ---------------------------------------------------------------------------
# Spark integration (pragma: no cover — tested via integration marker)
# ---------------------------------------------------------------------------


def run_categorise(  # pragma: no cover
    event_dates_table: str,
    event_category_table: str,
    category_keywords_path: Path,
    examples_path: Path,
    *,
    model: str = "databricks-meta-llama-3-3-70b-instruct",
    batch_size: int = 10,
    max_workers: int = 4,
    client: OpenAI | None = None,
) -> int:
    """Classify EventDate rows (rules → LLM for uncertain); write EventCategory records.

    Pipeline gate: only events with event_status IN ('future', 'undefined') are
    processed.  Rules run first; uncertain results are resolved via LLM in the
    same task.  No event is ever written with category='uncertain'.

    Args:
        event_dates_table: Fully-qualified bronze.event_dates Delta table.
        event_category_table: Fully-qualified bronze.event_category Delta table.
        category_keywords_path: Path to ``category_keywords.yml``.
        examples_path: Path to ``category_examples.yml`` (few-shot LLM context).
        model: LLM model name for the Databricks serving endpoint.
        batch_size: Events per LLM call.
        max_workers: Parallel LLM call threads.
        client: Optional OpenAI client (injected for testing).

    Returns:
        Number of events classified.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    spark = SparkSession.builder.getOrCreate()
    category_keywords = load_category_keywords(category_keywords_path)
    examples = load_examples(examples_path)

    if client is None:
        client = _create_default_client()

    # Pipeline gate: only future/undefined events proceed
    events_df = spark.table(event_dates_table).filter(
        F.col("event_status").isin("future", "undefined")
    )

    # Anti-join: skip fingerprints already in event_category
    if spark.catalog.tableExists(event_category_table):
        done_df = spark.table(event_category_table).select("fingerprint")
        events_df = events_df.join(done_df, on="fingerprint", how="left_anti")

    # Dedup by fingerprint (event_dates uses append mode)
    rows = (
        events_df.select("fingerprint", "title", "description")
        .dropDuplicates(["fingerprint"])
        .collect()
    )

    if not rows:
        logger.info("No new events to categorise")
        return 0

    logger.info("Classifying {} events from {}", len(rows), event_dates_table)

    # Phase 1: rules-based classification
    rule_results: list[
        tuple[str, str, str, str]
    ] = []  # (fp, title, description, category)
    uncertain_events: list[dict[str, str]] = []

    for row in rows:
        fp = row["fingerprint"]
        title = row["title"] or ""
        description = row["description"] or ""
        cat = classify_text(title, description, category_keywords)
        rule_results.append((fp, title, description, cat))
        if cat == _UNCERTAIN:
            uncertain_events.append(
                {"fingerprint": fp, "title": title, "description": description}
            )

    dist = Counter(cat for _, _, _, cat in rule_results)
    logger.info(
        "Rules: {} certain, {} uncertain",
        sum(n for cat, n in dist.items() if cat != _UNCERTAIN),
        dist.get(_UNCERTAIN, 0),
    )

    # Phase 2: LLM resolves uncertain events
    llm_map: dict[str, str] = {}
    if uncertain_events:
        logger.info("Resolving {} uncertain events via LLM", len(uncertain_events))
        llm_results = classify_uncertain_events(
            uncertain_events,
            examples,
            client,
            model,
            batch_size=batch_size,
            max_workers=max_workers,
        )
        llm_map = dict(llm_results)
        llm_dist = Counter(llm_map.values())
        for cat, n in sorted(llm_dist.items()):
            logger.info("  LLM {} → {}", cat, n)

    # Build final records — no 'uncertain' in output
    schema = StructType(
        [
            StructField("fingerprint", StringType(), False),
            StructField("category", StringType(), False),
            StructField("category_status", StringType(), False),
        ]
    )
    records: list[tuple[str, str, str]] = []
    for fp, _, _, cat in rule_results:
        if cat == _UNCERTAIN:
            cat = llm_map.get(fp, _NON_ART)
        status = str(
            CategoryStatus.MISSING if cat == _NON_ART else CategoryStatus.IDENTIFIED
        )
        records.append((fp, cat, status))

    final_dist = Counter(cat for _, cat, _ in records)
    for cat, n in sorted(final_dist.items()):
        logger.info("  {} → {}", cat, n)

    df = spark.createDataFrame(records, schema=schema)

    parts = event_category_table.split(".")
    if len(parts) == 3:
        catalog, db, _ = parts
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db}")

    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        event_category_table
    )

    logger.info("Wrote {} EventCategory rows to {}", len(records), event_category_table)
    return len(records)


# ---------------------------------------------------------------------------
# Generate category examples entry point
# ---------------------------------------------------------------------------

_GENERATE_SYSTEM_PROMPT = (
    "You are a data generation assistant for an art event classification system. "
    "You generate realistic example event texts in multiple languages."
)

_GENERATE_USER_PROMPT_TEMPLATE = """\
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
            {"role": "system", "content": _GENERATE_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": _GENERATE_USER_PROMPT_TEMPLATE.format(
                    n=n,
                    category=category,
                    language=language,
                    definition=_CATEGORY_DESCRIPTIONS_LONG[category],
                    keywords=", ".join(keywords[:8]),
                ),
            },
        ],
        temperature=0.7,
        max_tokens=512,
    )
    content = response.choices[0].message.content or ""
    cleaned = re.sub(r"```(?:json)?\s*", "", content).strip()
    match = re.search(r"\[.*?\]", cleaned, re.DOTALL)
    if not match:
        raise ValueError(f"No JSON array found in LLM response: {cleaned[:200]}")
    items: list[dict[str, str]] = json.loads(match.group())
    return [FewShotExample(**item) for item in items]


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

    for category in (*_ART_CATEGORIES, _NON_ART):
        logger.info("Generating examples for category: {}", category)
        examples[category] = {}
        lang_kws = category_keywords.get(category, {})
        for language in _LANGUAGES:
            keywords = lang_kws.get(language, lang_kws.get("EN", []))
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
# Entry points
# ---------------------------------------------------------------------------


def main() -> None:  # pragma: no cover
    """Entry point for artlake-categorise wheel task."""
    import argparse

    parser = argparse.ArgumentParser(description="ArtLake event categorisation")
    parser.add_argument(
        "--event-dates-table",
        default="artlake.bronze.event_dates",
        help="Fully-qualified bronze.event_dates Delta table",
    )
    parser.add_argument(
        "--event-category-table",
        default="artlake.bronze.event_category",
        help="Fully-qualified bronze.event_category Delta table",
    )
    parser.add_argument(
        "--category-keywords",
        required=True,
        type=Path,
        help="Path to category_keywords.yml",
    )
    parser.add_argument(
        "--category-examples",
        required=True,
        type=Path,
        help="Path to category_examples.yml",
    )
    parser.add_argument(
        "--model",
        default="databricks-meta-llama-3-3-70b-instruct",
        help="Databricks serving endpoint model name",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Events per LLM call",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Parallel LLM call threads",
    )
    args = parser.parse_args()
    run_categorise(
        event_dates_table=args.event_dates_table,
        event_category_table=args.event_category_table,
        category_keywords_path=args.category_keywords,
        examples_path=args.category_examples,
        model=args.model,
        batch_size=args.batch_size,
        max_workers=args.max_workers,
    )


def main_generate_examples() -> None:  # pragma: no cover
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
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Regenerate even if output file already exists",
    )
    args = parser.parse_args()
    generate_examples(
        args.category_keywords,
        args.output,
        model=args.model,
        n_per_language=args.n_per_language,
        overwrite=args.overwrite,
    )

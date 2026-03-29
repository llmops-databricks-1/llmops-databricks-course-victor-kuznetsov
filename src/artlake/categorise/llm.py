"""LLM-based event categorisation for uncertain events.

Entry point: artlake-categorise-llm

Resolves uncertain events from raw_events using the Databricks Foundation Model
API with mini-batched parallel calls, and writes artlake.bronze.categorised_events.

Default mode (comparison=False):
  Runs the LLM only on uncertain events.  Writes categorised_events with a single
  resolved ``category`` column — no uncertain in output, non_art excluded.

Comparison mode (comparison=True, --comparison flag):
  Runs the LLM on ALL rule-categorised events.  Writes categorised_events with two
  columns so rule-based and LLM results can be compared side-by-side:
    category     — rule-based result (may include uncertain)
    category_llm — LLM result on raw text, always definitive

raw_events is never modified.  Downstream (geocode) reads from categorised_events.

Output categories: open_call / exhibition / workshop / market / non_art
  — no 'uncertain' is ever written to categorised_events.
"""

from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import backoff
from loguru import logger
from openai import OpenAI

from artlake.categorise.examples import CategoryExamples, FewShotExample, load_examples

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_VALID_CATEGORIES = frozenset(
    {"open_call", "exhibition", "workshop", "market", "non_art"}
)

_CATEGORY_DESCRIPTIONS: dict[str, str] = {
    "open_call": "A call for artists to submit work, proposals, or applications.",
    "exhibition": "A gallery show, vernissage, or art display.",
    "workshop": "An art class, masterclass, or artist residency.",
    "market": "An art fair, craft fair, or artisan market.",
    "non_art": "Clearly not an art event (sports, cooking, finance, etc.).",
}

# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------


def _format_example(ex: FewShotExample) -> str:
    return f'"{ex.title}: {ex.description}"'


def _build_system_prompt(examples: CategoryExamples) -> str:
    """Build the classification system prompt with category definitions and examples.

    Includes one few-shot example per (category, language) pair to guide the
    LLM across all four supported languages (EN, NL, DE, FR).
    """
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


def _parse_batch_response(content: str, expected_fps: list[str]) -> list[tuple[str, str]]:
    """Parse LLM batch response into (fingerprint, category) pairs.

    Falls back to 'non_art' for any fingerprint missing from the response or
    returned with an invalid category.

    Args:
        content: Raw LLM response text (may include markdown fences).
        expected_fps: Fingerprints in input order — output preserves this order.

    Returns:
        List of ``(fingerprint, category)`` tuples.
    """
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
    """Send one mini-batch to the LLM and return classified (fingerprint, category) pairs.

    Args:
        client: OpenAI client pointed at Databricks serving endpoints.
        model: Model name.
        system_prompt: Pre-built prompt with category definitions + few-shot examples.
        batch: List of dicts with 'fingerprint', 'title', 'description'.

    Returns:
        List of ``(fingerprint, category)`` tuples in input order.
    """
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
        max_tokens=512,
    )
    content = response.choices[0].message.content or ""
    return _parse_batch_response(content, expected_fps)


# ---------------------------------------------------------------------------
# Pure orchestration (testable without Spark)
# ---------------------------------------------------------------------------


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


def run_categorise_llm(  # pragma: no cover
    raw_events_table: str,
    categorised_events_table: str,
    examples_path: Path,
    *,
    model: str = "databricks-meta-llama-3-3-70b-instruct",
    batch_size: int = 10,
    max_workers: int = 4,
    llm_categorization_test: bool = False,
    client: OpenAI | None = None,
) -> int:
    """Resolve uncertain events and write categorised_events.

    Default mode (llm_categorization_test=False):
        Runs LLM only on uncertain events, resolves them to a definitive category,
        and writes categorised_events with a single ``category`` column.
        non_art events (from rules or LLM) are excluded.

    Test mode (llm_categorization_test=True):
        Runs LLM on ALL rule-categorised events and writes categorised_events with:
          ``category``     — rule-based result (may be uncertain)
          ``category_llm`` — LLM result on raw text, always definitive
        Useful for auditing rule/LLM agreement at scale.

    Args:
        raw_events_table: Fully-qualified Delta table (e.g. artlake.bronze.raw_events).
        categorised_events_table: Output table (e.g. artlake.bronze.categorised_events).
        examples_path: Path to ``category_examples.yml``.
        model: LLM model name for the Databricks serving endpoint.
        batch_size: Events per LLM call.
        max_workers: Parallel LLM call threads.
        llm_categorization_test: When True, run LLM on all events and add
            a ``category_llm`` column.
        client: Optional OpenAI client (injected for testing).

    Returns:
        Number of events written to ``categorised_events``.
    """
    from collections import Counter

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    spark = SparkSession.builder.getOrCreate()
    examples = load_examples(examples_path)

    if client is None:
        client = _create_default_client()

    raw_df = spark.table(raw_events_table).filter(F.col("category").isNotNull())

    if llm_categorization_test:
        # --- Test mode: LLM on ALL events, keeps category + category_llm ---
        rows = raw_df.select("fingerprint", "title", "description").collect()
        if not rows:
            logger.info("No categorised events in {} — nothing to do", raw_events_table)
            return 0
        logger.info(
            "LLM categorization test mode: running LLM on all {} events from {}",
            len(rows),
            raw_events_table,
        )
        events = [
            {
                "fingerprint": r["fingerprint"],
                "title": r["title"] or "",
                "description": r["description"] or "",
            }
            for r in rows
        ]
        llm_results = classify_uncertain_events(
            events,
            examples,
            client,
            model,
            batch_size=batch_size,
            max_workers=max_workers,
        )
        dist = Counter(cat for _, cat in llm_results)
        for cat, n in sorted(dist.items()):
            logger.info("  category_llm {} → {}", cat, n)

        llm_schema = StructType(
            [
                StructField("fingerprint", StringType(), False),
                StructField("category_llm", StringType(), False),
            ]
        )
        llm_df = spark.createDataFrame(
            [(fp, cat) for fp, cat in llm_results], schema=llm_schema
        )
        final_df = raw_df.join(llm_df, on="fingerprint", how="left").filter(
            F.col("category_llm") != "non_art"
        )
    else:
        # --- Default mode: LLM only on uncertain, single resolved category ---
        uncertain_df = raw_df.filter(F.col("category") == "uncertain")
        uncertain_rows = uncertain_df.select(
            "fingerprint", "title", "description"
        ).collect()

        if uncertain_rows:
            logger.info("Classifying {} uncertain events via LLM", len(uncertain_rows))
            events = [
                {
                    "fingerprint": r["fingerprint"],
                    "title": r["title"] or "",
                    "description": r["description"] or "",
                }
                for r in uncertain_rows
            ]
            llm_results = classify_uncertain_events(
                events,
                examples,
                client,
                model,
                batch_size=batch_size,
                max_workers=max_workers,
            )
            dist = Counter(cat for _, cat in llm_results)
            for cat, n in sorted(dist.items()):
                logger.info("  {} → {}", cat, n)

            llm_schema = StructType(
                [
                    StructField("fingerprint", StringType(), False),
                    StructField("llm_category", StringType(), False),
                ]
            )
            llm_df = spark.createDataFrame(
                [(fp, cat) for fp, cat in llm_results], schema=llm_schema
            )
            resolved_df = (
                uncertain_df.join(llm_df, on="fingerprint", how="left")
                .withColumn("category", F.col("llm_category"))
                .drop("llm_category")
                .filter(F.col("category") != "non_art")
            )
        else:
            logger.info("No uncertain events in {}", raw_events_table)
            resolved_df = spark.createDataFrame([], uncertain_df.schema)

        rule_df = raw_df.filter(~F.col("category").isin("uncertain", "non_art"))
        # unionByName is robust against column reordering from the join above
        final_df = rule_df.unionByName(resolved_df)

    final_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(categorised_events_table)

    count: int = final_df.count()
    logger.info("Wrote {} events to {}", count, categorised_events_table)
    return count


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:  # pragma: no cover
    """Entry point for artlake-categorise-llm wheel task."""
    import argparse

    parser = argparse.ArgumentParser(description="ArtLake LLM-based event categorisation")
    parser.add_argument(
        "--raw-events-table",
        default="artlake.bronze.raw_events",
        help="Fully-qualified raw_events Delta table",
    )
    parser.add_argument(
        "--categorised-events-table",
        default="artlake.bronze.categorised_events",
        help="Output Delta table for post-categorisation events",
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
    parser.add_argument(
        "--llm-categorization-test",
        action="store_true",
        default=False,
        dest="llm_categorization_test",
        help="Run LLM on all events; adds category_llm column for rule vs LLM audit",
    )
    args = parser.parse_args()
    run_categorise_llm(
        raw_events_table=args.raw_events_table,
        categorised_events_table=args.categorised_events_table,
        examples_path=args.category_examples,
        model=args.model,
        batch_size=args.batch_size,
        max_workers=args.max_workers,
        llm_categorization_test=args.llm_categorization_test,
    )

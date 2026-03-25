"""ArtLake CLI — centralized command-line interface."""

from pathlib import Path

from typer import Option, Typer

app = Typer(name="artlake", help="ArtLake CLI for art event discovery.")

_DEFAULT_KEYWORDS = "config/input/keywords.yml"
_DEFAULT_OUTPUT = "config/output/queries.yml"
_DEFAULT_MODEL = "databricks-llama-4-maverick"


@app.command("generate-queries")
def generate_queries(
    keywords: Path = Option(  # noqa: B008
        _DEFAULT_KEYWORDS,
        help="Path to the input keywords YAML file.",
    ),
    output: Path = Option(  # noqa: B008
        _DEFAULT_OUTPUT,
        help="Path where generated queries YAML will be written.",
    ),
    model: str = Option(
        _DEFAULT_MODEL,
        help="LLM model name for the Databricks serving endpoint.",
    ),
) -> None:
    """Translate English keywords into multilingual search queries."""
    from artlake.search.generate import generate_queries as _generate

    result = _generate(keywords, output, model=model)
    count = len(result.queries)
    print(f"Generated {count} queries → {output}")  # noqa: T201


def main() -> None:
    """Entry point for the artlake CLI."""
    app()

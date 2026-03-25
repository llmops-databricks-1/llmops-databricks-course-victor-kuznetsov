"""Load generated search queries from YAML."""

from pathlib import Path

import yaml

from artlake.search.models import QueriesOutput, SearchQuery


def load_queries(queries_path: Path) -> list[SearchQuery]:
    """Read queries.yml and return a validated list of SearchQuery objects.

    Args:
        queries_path: Path to the generated queries YAML file.

    Returns:
        List of validated SearchQuery objects.

    Raises:
        FileNotFoundError: If the queries file does not exist.
        pydantic.ValidationError: If the YAML content is invalid.
    """
    raw = yaml.safe_load(queries_path.read_text())
    output = QueriesOutput(**raw)
    return output.queries

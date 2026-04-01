"""Pipeline configuration model."""

from datetime import date

from pydantic import BaseModel, ConfigDict


class ArtLakeConfig(BaseModel):
    """Pipeline configuration loaded from DAB variables or YAML."""

    model_config = ConfigDict(strict=True)

    target_countries: list[str]
    languages: list[str]
    target_language: str = "EN"
    categories: list[str]
    scrape_schedule: str
    cutoff_date: date | None = None

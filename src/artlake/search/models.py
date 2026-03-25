"""Pydantic models for keyword configuration and search query output."""

from pydantic import BaseModel, ConfigDict


class CountryDef(BaseModel):
    """Country with its target search languages."""

    model_config = ConfigDict(strict=True)

    code: str
    name: str
    languages: list[str]


class KeywordConfig(BaseModel):
    """Schema for config/input/keywords.yml."""

    model_config = ConfigDict(strict=True)

    keywords: list[str]
    countries: list[CountryDef]


class SearchQuery(BaseModel):
    """A single translated search query."""

    model_config = ConfigDict(strict=True)

    keyword_en: str
    country_code: str
    country_name: str
    language: str
    query: str


class QueriesOutput(BaseModel):
    """Schema for config/output/queries.yml."""

    model_config = ConfigDict(strict=True)

    generated_at: str
    model: str
    queries: list[SearchQuery]

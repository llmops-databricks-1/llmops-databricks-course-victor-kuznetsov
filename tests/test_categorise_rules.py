"""Unit tests for artlake.categorise.rules."""

from pathlib import Path

import pytest
import yaml

from artlake.events.categorise import classify_text, load_category_keywords

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_SAMPLE_KEYWORDS: dict = {
    "category_keywords": {
        "open_call": {
            "en": ["open call", "call for artists", "submission deadline"],
            "nl": ["open oproep", "inschrijvingsdeadline"],
            "de": ["offener aufruf", "bewerbungsschluss"],
            "fr": ["appel à candidatures", "appel ouvert"],
        },
        "exhibition": {
            "en": ["exhibition", "vernissage", "gallery opening"],
            "nl": ["tentoonstelling", "expositie"],
            "de": ["ausstellung", "vernissage"],
            "fr": ["exposition", "vernissage"],
        },
        "workshop": {
            "en": ["workshop", "masterclass", "art residency"],
            "nl": ["workshop", "residentie"],
            "de": ["workshop", "masterclass"],
            "fr": ["atelier", "masterclass"],
        },
        "market": {
            "en": ["art market", "art fair", "craft fair"],
            "nl": ["kunstmarkt", "kunstbeurs"],
            "de": ["kunstmarkt", "kunstmesse"],
            "fr": ["marché d'art", "foire d'art"],
        },
        "non_art": {
            "en": ["football match", "cooking recipe", "mortgage rate"],
            "nl": ["voetbalwedstrijd", "kookrecept"],
            "de": ["fußballspiel", "kochrezept"],
            "fr": ["match de football", "recette de cuisine"],
        },
    }
}


@pytest.fixture
def keywords_file(tmp_path: Path) -> Path:
    path = tmp_path / "category_keywords.yml"
    path.write_text(yaml.dump(_SAMPLE_KEYWORDS))
    return path


@pytest.fixture
def keywords(keywords_file: Path) -> dict:
    return load_category_keywords(keywords_file)


# ---------------------------------------------------------------------------
# load_category_keywords
# ---------------------------------------------------------------------------


def test_load_returns_all_categories(keywords_file: Path) -> None:
    result = load_category_keywords(keywords_file)
    assert set(result.keys()) == {
        "open_call",
        "exhibition",
        "workshop",
        "market",
        "non_art",
    }


def test_load_returns_language_keys(keywords_file: Path) -> None:
    result = load_category_keywords(keywords_file)
    assert set(result["open_call"].keys()) == {"en", "nl", "de", "fr"}


def test_load_missing_key_returns_empty(tmp_path: Path) -> None:
    path = tmp_path / "empty.yml"
    path.write_text(yaml.dump({"other_key": {}}))
    assert load_category_keywords(path) == {}


# ---------------------------------------------------------------------------
# classify_text — art categories (EN)
# ---------------------------------------------------------------------------


def test_classify_open_call_en(keywords: dict) -> None:
    assert (
        classify_text(
            "Open call for artists 2025", "Submit before the deadline", keywords
        )
        == "open_call"
    )


def test_classify_exhibition_en(keywords: dict) -> None:
    assert (
        classify_text(
            "Summer Exhibition at the City Gallery",
            "Join us for the vernissage",
            keywords,
        )
        == "exhibition"
    )


def test_classify_workshop_en(keywords: dict) -> None:
    assert (
        classify_text(
            "Weekend Masterclass in Watercolour",
            "Hands-on workshop with a local artist",
            keywords,
        )
        == "workshop"
    )


def test_classify_market_en(keywords: dict) -> None:
    assert (
        classify_text("Rotterdam Art Fair 2025", "Visit the annual art market", keywords)
        == "market"
    )


# ---------------------------------------------------------------------------
# classify_text — multilingual
# ---------------------------------------------------------------------------


def test_classify_open_call_nl(keywords: dict) -> None:
    assert (
        classify_text(
            "Open Oproep voor Kunstenaars",
            "Stuur je werk in voor de inschrijvingsdeadline",
            keywords,
        )
        == "open_call"
    )


def test_classify_open_call_de(keywords: dict) -> None:
    assert (
        classify_text(
            "Offener Aufruf Kunstprojekt", "Bewerbungsschluss 15 März", keywords
        )
        == "open_call"
    )


def test_classify_open_call_fr(keywords: dict) -> None:
    assert (
        classify_text("Appel à candidatures", "Soumettez votre projet", keywords)
        == "open_call"
    )


def test_classify_exhibition_nl(keywords: dict) -> None:
    assert (
        classify_text(
            "Tentoonstelling in het Stedelijk Museum", "Bezoek onze expositie", keywords
        )
        == "exhibition"
    )


def test_classify_workshop_fr(keywords: dict) -> None:
    assert (
        classify_text(
            "Atelier de peinture", "Rejoignez notre masterclass artistique", keywords
        )
        == "workshop"
    )


def test_classify_market_de(keywords: dict) -> None:
    assert (
        classify_text("Kunstmarkt Berlin", "Besuchen Sie die Kunstmesse", keywords)
        == "market"
    )


# ---------------------------------------------------------------------------
# classify_text — non_art and uncertain
# ---------------------------------------------------------------------------


def test_classify_non_art_en(keywords: dict) -> None:
    assert (
        classify_text("Football Match This Saturday", "Come watch the game", keywords)
        == "non_art"
    )


def test_classify_non_art_nl(keywords: dict) -> None:
    assert (
        classify_text("Voetbalwedstrijd Amsterdam", "Kaartjes te koop", keywords)
        == "non_art"
    )


def test_classify_uncertain_no_match(keywords: dict) -> None:
    assert (
        classify_text("Gallery Newsletter", "Updates from our team this month", keywords)
        == "uncertain"
    )


def test_classify_uncertain_empty_text(keywords: dict) -> None:
    assert classify_text(None, None, keywords) == "uncertain"


def test_classify_uncertain_whitespace_only(keywords: dict) -> None:
    assert classify_text("   ", "  ", keywords) == "uncertain"


# ---------------------------------------------------------------------------
# classify_text — edge cases
# ---------------------------------------------------------------------------


def test_art_category_beats_non_art(keywords: dict) -> None:
    # A football photo exhibition should be classified as exhibition, not non_art
    assert (
        classify_text(
            "Football Photo Exhibition", "A photography show about the sport", keywords
        )
        == "exhibition"
    )


def test_classify_title_only(keywords: dict) -> None:
    assert classify_text("Open call for artists", None, keywords) == "open_call"


def test_classify_description_only(keywords: dict) -> None:
    assert (
        classify_text(None, "Come to the vernissage this Friday", keywords)
        == "exhibition"
    )


def test_classify_empty_keywords(keywords: dict) -> None:
    assert classify_text("Open call for artists", "Submit your work", {}) == "uncertain"


def test_classify_case_insensitive(keywords: dict) -> None:
    assert (
        classify_text("OPEN CALL For Artists", "SUBMISSION DEADLINE next week", keywords)
        == "open_call"
    )

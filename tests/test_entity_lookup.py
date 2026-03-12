"""Tests for entity_lookup enrichment module."""

from prospect_engine.enrichment.entity_lookup import (
    _extract_entity_fields,
    _select_best_match,
    _parse_year,
)
from prospect_engine.models.prospect import Prospect


def test_extract_entity_fields_valid():
    """entityStartDate is parsed into founded_year, UEI and state extracted."""
    entity = {
        "entityRegistration": {
            "ueiSAM": "ABC123DEF456",
            "legalBusinessName": "Test Aerospace Inc",
        },
        "coreData": {
            "entityInformation": {
                "entityStartDate": "2019-05-15",
            },
            "physicalAddress": {
                "stateOrProvinceCode": "AZ",
            },
        },
    }
    uei, state, year = _extract_entity_fields(entity)
    assert uei == "ABC123DEF456"
    assert state == "AZ"
    assert year == 2019


def test_extract_entity_fields_missing_date():
    """Missing entityStartDate returns None for founded_year."""
    entity = {
        "entityRegistration": {
            "ueiSAM": "XYZ789",
            "legalBusinessName": "No Date Corp",
        },
        "coreData": {
            "entityInformation": {},
            "physicalAddress": {
                "stateOrProvinceCode": "TX",
            },
        },
    }
    uei, state, year = _extract_entity_fields(entity)
    assert uei == "XYZ789"
    assert state == "TX"
    assert year is None


def test_extract_entity_fields_slash_date():
    """MM/DD/YYYY date format is parsed correctly."""
    entity = {
        "entityRegistration": {"ueiSAM": ""},
        "coreData": {
            "entityInformation": {"entityStartDate": "06/15/2020"},
            "physicalAddress": {},
        },
    }
    _, _, year = _extract_entity_fields(entity)
    assert year == 2020


def test_select_best_match_exact():
    """Exact normalized name match is selected."""
    entities = [
        {
            "entityRegistration": {"legalBusinessName": "DESERT DEFENSE INC"},
        },
        {
            "entityRegistration": {"legalBusinessName": "DESERT DYNAMICS LLC"},
        },
    ]
    result = _select_best_match(entities, "Desert Defense Inc")
    assert result is not None
    assert result["entityRegistration"]["legalBusinessName"] == "DESERT DEFENSE INC"


def test_select_best_match_substring():
    """Substring match works as fallback."""
    entities = [
        {
            "entityRegistration": {
                "legalBusinessName": "SPACE SYSTEMS INTERNATIONAL LLC",
            },
        },
    ]
    result = _select_best_match(entities, "SPACE SYSTEMS INTERNATIONAL")
    assert result is not None


def test_select_best_match_no_match():
    """Returns None when no entity name is close enough."""
    entities = [
        {
            "entityRegistration": {"legalBusinessName": "TOTALLY DIFFERENT COMPANY"},
        },
    ]
    result = _select_best_match(entities, "Desert Defense Inc")
    assert result is None


def test_parse_year_iso():
    assert _parse_year("2020-06-15") == 2020


def test_parse_year_slash():
    assert _parse_year("06/15/2020") == 2020


def test_parse_year_bare():
    assert _parse_year("2018") == 2018


def test_parse_year_empty():
    assert _parse_year("") is None
    assert _parse_year(None) is None

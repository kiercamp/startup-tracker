"""Tests for config module."""

from prospect_engine.config import (
    TARGET_STATES,
    TARGET_NAICS,
    DOD_INNOVATION_PROGRAMS,
    EXISTING_PIPELINE,
    LOGS_DIR,
    OUTPUT_DIR,
    TARGET_AGENCIES_USASPENDING,
    TARGET_AGENCIES_SAM_GOV,
    AEROSPACE_DEFENSE_KEYWORDS,
)


def test_target_states():
    assert TARGET_STATES == ["AZ", "NM", "CO", "UT", "TX"]


def test_target_naics_all_six_digit():
    for code in TARGET_NAICS:
        assert len(code) == 6
        assert code.isdigit()


def test_dod_innovation_programs_not_empty():
    assert len(DOD_INNOVATION_PROGRAMS) > 0
    assert "AFWERX" in DOD_INNOVATION_PROGRAMS
    assert "SpaceWERX" in DOD_INNOVATION_PROGRAMS


def test_existing_pipeline():
    assert "Loft Orbital" in EXISTING_PIPELINE


def test_dirs_exist():
    assert LOGS_DIR.exists()
    assert OUTPUT_DIR.exists()


def test_target_agencies_usaspending():
    assert len(TARGET_AGENCIES_USASPENDING) == 2
    names = [a["name"] for a in TARGET_AGENCIES_USASPENDING]
    assert "Department of Defense" in names
    assert "National Aeronautics and Space Administration" in names
    for entry in TARGET_AGENCIES_USASPENDING:
        assert entry["type"] == "awarding"
        assert entry["tier"] == "toptier"


def test_target_agencies_sam_gov():
    assert "9700" in TARGET_AGENCIES_SAM_GOV  # DoD
    assert "8000" in TARGET_AGENCIES_SAM_GOV  # NASA
    assert len(TARGET_AGENCIES_SAM_GOV) == 2


def test_aerospace_keywords_all_lowercase():
    for kw in AEROSPACE_DEFENSE_KEYWORDS:
        assert kw == kw.lower(), "Keyword '{}' must be lowercase".format(kw)


def test_aerospace_keywords_not_empty():
    assert len(AEROSPACE_DEFENSE_KEYWORDS) > 10  # Expect a substantial list
    # Check a few key terms are present
    assert "aerospace" in AEROSPACE_DEFENSE_KEYWORDS
    assert "radar" in AEROSPACE_DEFENSE_KEYWORDS
    assert "satellite" in AEROSPACE_DEFENSE_KEYWORDS
    assert "sbir" in AEROSPACE_DEFENSE_KEYWORDS

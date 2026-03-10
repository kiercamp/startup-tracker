"""Tests for config module."""

from prospect_engine.config import (
    TARGET_STATES,
    TARGET_NAICS,
    DOD_INNOVATION_PROGRAMS,
    EXISTING_PIPELINE,
    LOGS_DIR,
    OUTPUT_DIR,
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

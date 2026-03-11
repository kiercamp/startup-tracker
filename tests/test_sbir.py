"""Tests for SBIR source module."""

from datetime import date

from prospect_engine.models.prospect import SbirAward
from prospect_engine.sources.sbir import (
    _parse_award,
    _filter_by_territory,
    _filter_by_amount,
    _group_by_firm,
)


SAMPLE_RAW = {
    "firm": "Tucson Antenna Systems",
    "award_title": "Advanced Phased Array for UAV",
    "agency": "DOD",
    "branch": "Air Force",
    "phase": "Phase II",
    "program": "SBIR",
    "contract": "FA8650-24-C-5678",
    "proposal_award_date": "2024-04-15",
    "award_year": "2024",
    "award_amount": 749860,
    "uei": "TAS123456",
    "city": "Tucson",
    "state": "AZ",
    "abstract": "Novel antenna design for UAV applications.",
}


def test_parse_award_valid():
    award = _parse_award(SAMPLE_RAW)
    assert award is not None
    assert award.firm == "Tucson Antenna Systems"
    assert award.agency == "DOD"
    assert award.phase == "Phase II"
    assert award.program == "SBIR"
    assert award.award_amount == 749860
    assert award.award_date == date(2024, 4, 15)
    assert award.state == "AZ"
    assert award.uei == "TAS123456"


def test_parse_award_phase_normalization():
    raw = dict(SAMPLE_RAW, phase="1")
    award = _parse_award(raw)
    assert award is not None
    assert award.phase == "Phase I"

    raw2 = dict(SAMPLE_RAW, phase="Phase 2")
    award2 = _parse_award(raw2)
    assert award2 is not None
    assert award2.phase == "Phase II"


def test_parse_award_missing_firm():
    raw = dict(SAMPLE_RAW, firm="")
    assert _parse_award(raw) is None


def test_parse_award_iso_date():
    raw = dict(SAMPLE_RAW, proposal_award_date="2024-06-01")
    award = _parse_award(raw)
    assert award is not None
    assert award.award_date == date(2024, 6, 1)


def test_parse_award_bad_date():
    raw = dict(SAMPLE_RAW, proposal_award_date="not-a-date")
    award = _parse_award(raw)
    assert award is not None
    assert award.award_date is None


def test_filter_by_territory():
    awards = [
        SbirAward(
            award_id="A-001",
            firm="AZ Firm",
            agency="DOD",
            phase="Phase I",
            program="SBIR",
            award_title="Test",
            award_amount=100_000,
            award_date=date(2024, 1, 1),
            state="AZ",
            city="Phoenix",
        ),
        SbirAward(
            award_id="A-002",
            firm="CA Firm",
            agency="DOD",
            phase="Phase I",
            program="SBIR",
            award_title="Test",
            award_amount=100_000,
            award_date=date(2024, 1, 1),
            state="CA",
            city="LA",
        ),
        SbirAward(
            award_id="A-003",
            firm="TX Firm",
            agency="NASA",
            phase="Phase II",
            program="STTR",
            award_title="Test",
            award_amount=500_000,
            award_date=date(2024, 6, 1),
            state="TX",
            city="Houston",
        ),
    ]

    filtered = _filter_by_territory(awards, ["AZ", "TX"])
    assert len(filtered) == 2
    states = {a.state for a in filtered}
    assert states == {"AZ", "TX"}


def test_filter_by_amount():
    awards = [
        SbirAward(
            award_id="A-001",
            firm="Small Grant Co",
            agency="DOD",
            phase="Phase I",
            program="SBIR",
            award_title="Small grant",
            award_amount=500_000,
            award_date=date(2024, 1, 1),
            state="AZ",
            city="Phoenix",
        ),
        SbirAward(
            award_id="A-002",
            firm="Big Contract Co",
            agency="NASA",
            phase="Phase II",
            program="SBIR",
            award_title="Big contract",
            award_amount=2_000_000,
            award_date=date(2024, 6, 1),
            state="TX",
            city="Houston",
        ),
        SbirAward(
            award_id="A-003",
            firm="Exact Threshold Co",
            agency="DOD",
            phase="Phase II",
            program="STTR",
            award_title="Exact threshold",
            award_amount=1_000_000,
            award_date=date(2024, 3, 1),
            state="CO",
            city="Denver",
        ),
    ]

    filtered = _filter_by_amount(awards, 1_000_000)
    assert len(filtered) == 2
    amounts = {a.award_amount for a in filtered}
    assert amounts == {2_000_000, 1_000_000}


def test_filter_by_amount_zero_floor():
    """A floor of zero should keep all awards."""
    awards = [
        SbirAward(
            award_id="A-001",
            firm="Any Firm",
            agency="DOD",
            phase="Phase I",
            program="SBIR",
            award_title="Test",
            award_amount=100,
            award_date=date(2024, 1, 1),
            state="AZ",
            city="Tucson",
        ),
    ]
    assert len(_filter_by_amount(awards, 0)) == 1


def test_group_by_firm():
    awards = [
        SbirAward(
            award_id="A-001",
            firm="Acme Space",
            agency="DOD",
            phase="Phase I",
            program="SBIR",
            award_title="Test 1",
            award_amount=100_000,
            award_date=date(2024, 1, 1),
            state="AZ",
            city="Tucson",
            uei="UEI-ACME",
        ),
        SbirAward(
            award_id="A-002",
            firm="Acme Space",
            agency="NASA",
            phase="Phase II",
            program="SBIR",
            award_title="Test 2",
            award_amount=500_000,
            award_date=date(2024, 6, 1),
            state="AZ",
            city="Tucson",
            uei="UEI-ACME",
        ),
        SbirAward(
            award_id="B-001",
            firm="Other Corp",
            agency="DOD",
            phase="Phase I",
            program="STTR",
            award_title="Test 3",
            award_amount=150_000,
            award_date=date(2024, 3, 1),
            state="TX",
            city="Austin",
            uei="UEI-OTHER",
        ),
    ]

    prospects = _group_by_firm(awards)
    assert len(prospects) == 2

    acme = [p for p in prospects if "Acme" in p.company_name]
    assert len(acme) == 1
    assert len(acme[0].sbir_awards) == 2
    assert acme[0].uei == "UEI-ACME"

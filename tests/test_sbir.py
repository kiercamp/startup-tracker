"""Tests for SBIR source module."""

from datetime import date

from prospect_engine.models.prospect import SbirAward
from prospect_engine.sources.sbir import (
    _parse_award,
    _parse_csv_row,
    _resolve_csv_columns,
    _sbir_award_to_dict,
    _dict_to_sbir_award,
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


# ---------------------------------------------------------------------------
# Bulk CSV parser tests
# ---------------------------------------------------------------------------

# Simulated CSV header matching real sbir.gov bulk export
CSV_HEADER = [
    "Company", "Award Title", "Agency", "Phase", "Program",
    "Award Amount", "Award Year", "Proposal Award Date",
    "City", "Company State", "Abstract", "UEI", "Contract",
]

SAMPLE_CSV_ROW = {
    "Company": "Desert Propulsion LLC",
    "Award Title": "Advanced Solid Rocket Motor Design",
    "Agency": "Department of Defense",
    "Phase": "Phase II",
    "Program": "SBIR",
    "Award Amount": "749,500",
    "Award Year": "2024",
    "Proposal Award Date": "2024-06-15",
    "City": "Tucson",
    "Company State": "AZ",
    "Abstract": "Novel propulsion system for tactical missiles.",
    "UEI": "DP123456",
    "Contract": "FA8650-24-C-1234",
}


def test_resolve_csv_columns():
    col_map = _resolve_csv_columns(CSV_HEADER)
    assert col_map["firm"] == "Company"
    assert col_map["state"] == "Company State"
    assert col_map["award_amount"] == "Award Amount"
    assert col_map["award_start_date"] == "Proposal Award Date"
    assert col_map["uei"] == "UEI"


def test_resolve_csv_columns_alternate_names():
    """Headers may use 'State' instead of 'Company State'."""
    header = list(CSV_HEADER)
    header[header.index("Company State")] = "State"
    col_map = _resolve_csv_columns(header)
    assert col_map["state"] == "State"


def test_resolve_csv_columns_proposal_award_date():
    """Real CSV uses 'Proposal Award Date' — must be resolved."""
    col_map = _resolve_csv_columns(CSV_HEADER)
    assert col_map["award_start_date"] == "Proposal Award Date"


def test_parse_csv_row_valid():
    col_map = _resolve_csv_columns(CSV_HEADER)
    award = _parse_csv_row(
        SAMPLE_CSV_ROW, col_map,
        agencies_upper={"DOD", "NASA"},
        states_upper={"AZ", "TX"},
        start_year=2020,
    )
    assert award is not None
    assert award.firm == "Desert Propulsion LLC"
    assert award.agency == "DOD"  # Normalized from "Department of Defense"
    assert award.phase == "Phase II"
    assert award.award_amount == 749500.0
    assert award.award_date == date(2024, 6, 15)
    assert award.state == "AZ"
    assert award.uei == "DP123456"
    assert award.award_id == "FA8650-24-C-1234"
    assert award.city == "Tucson"


def test_parse_csv_row_filters_agency():
    col_map = _resolve_csv_columns(CSV_HEADER)
    row = dict(SAMPLE_CSV_ROW, Agency="Department of Health and Human Services")
    award = _parse_csv_row(
        row, col_map,
        agencies_upper={"DOD", "NASA"},
        states_upper={"AZ"},
        start_year=2020,
    )
    assert award is None


def test_parse_csv_row_filters_state():
    col_map = _resolve_csv_columns(CSV_HEADER)
    row = dict(SAMPLE_CSV_ROW, **{"Company State": "CA"})
    award = _parse_csv_row(
        row, col_map,
        agencies_upper={"DOD", "NASA"},
        states_upper={"AZ", "TX"},
        start_year=2020,
    )
    assert award is None


def test_parse_csv_row_filters_year():
    col_map = _resolve_csv_columns(CSV_HEADER)
    row = dict(SAMPLE_CSV_ROW, **{"Award Year": "2018"})
    award = _parse_csv_row(
        row, col_map,
        agencies_upper={"DOD", "NASA"},
        states_upper={"AZ"},
        start_year=2020,
    )
    assert award is None


def test_parse_csv_row_missing_firm():
    col_map = _resolve_csv_columns(CSV_HEADER)
    row = dict(SAMPLE_CSV_ROW, Company="")
    award = _parse_csv_row(
        row, col_map,
        agencies_upper={"DOD", "NASA"},
        states_upper={"AZ"},
        start_year=2020,
    )
    assert award is None


def test_parse_csv_row_dollar_sign_amount():
    col_map = _resolve_csv_columns(CSV_HEADER)
    row = dict(SAMPLE_CSV_ROW, **{"Award Amount": "$1,250,000"})
    award = _parse_csv_row(
        row, col_map,
        agencies_upper={"DOD", "NASA"},
        states_upper={"AZ"},
        start_year=2020,
    )
    assert award is not None
    assert award.award_amount == 1250000.0


def test_parse_csv_row_mm_dd_yyyy_date():
    col_map = _resolve_csv_columns(CSV_HEADER)
    row = dict(SAMPLE_CSV_ROW, **{"Proposal Award Date": "06/15/2024"})
    award = _parse_csv_row(
        row, col_map,
        agencies_upper={"DOD", "NASA"},
        states_upper={"AZ"},
        start_year=2020,
    )
    assert award is not None
    assert award.award_date == date(2024, 6, 15)


def test_parse_csv_row_phase_normalization():
    col_map = _resolve_csv_columns(CSV_HEADER)
    row = dict(SAMPLE_CSV_ROW, Phase="1")
    award = _parse_csv_row(
        row, col_map,
        agencies_upper={"DOD", "NASA"},
        states_upper={"AZ"},
        start_year=2020,
    )
    assert award is not None
    assert award.phase == "Phase I"


def test_parse_csv_row_nasa_agency():
    """NASA as full name or abbreviation should both work."""
    col_map = _resolve_csv_columns(CSV_HEADER)
    row = dict(SAMPLE_CSV_ROW, Agency="National Aeronautics and Space Administration")
    award = _parse_csv_row(
        row, col_map,
        agencies_upper={"DOD", "NASA"},
        states_upper={"AZ"},
        start_year=2020,
    )
    assert award is not None
    assert award.agency == "NASA"  # Normalized to code


# ---------------------------------------------------------------------------
# Cache serialization roundtrip
# ---------------------------------------------------------------------------


def test_sbir_award_serialization_roundtrip():
    original = SbirAward(
        award_id="RT-001",
        firm="Roundtrip Corp",
        agency="DOD",
        phase="Phase II",
        program="SBIR",
        award_title="Test roundtrip",
        award_amount=500_000,
        award_date=date(2024, 3, 15),
        state="AZ",
        city="Tucson",
        abstract="Testing serialization.",
        uei="RT123",
        source_url="https://sbir.gov/test",
    )
    d = _sbir_award_to_dict(original)
    restored = _dict_to_sbir_award(d)
    assert restored.award_id == original.award_id
    assert restored.firm == original.firm
    assert restored.award_amount == original.award_amount
    assert restored.award_date == original.award_date
    assert restored.state == original.state
    assert restored.uei == original.uei


def test_sbir_award_serialization_no_date():
    original = SbirAward(
        award_id="RT-002",
        firm="No Date Corp",
        agency="NASA",
        phase="Phase I",
        program="STTR",
        award_title="No date test",
        award_amount=100_000,
        award_date=None,
        state="TX",
        city="Houston",
    )
    d = _sbir_award_to_dict(original)
    assert d["award_date"] is None
    restored = _dict_to_sbir_award(d)
    assert restored.award_date is None
    assert restored.firm == "No Date Corp"

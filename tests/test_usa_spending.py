"""Tests for USASpending source module."""

from datetime import date

from prospect_engine.models.prospect import ContractAward
from prospect_engine.sources.usa_spending import (
    _build_request_body,
    _parse_result,
    _group_by_recipient,
)


def test_build_request_body():
    body = _build_request_body(
        states=["AZ", "TX"],
        naics_codes=["336414", "541715"],
        start_date="2020-01-01",
        end_date="2024-12-31",
        page=1,
        limit=100,
    )

    assert body["filters"]["award_type_codes"] == ["A", "B", "C", "D"]
    # naics_codes removed from server query (API bug), filtered client-side
    assert "naics_codes" not in body["filters"]
    assert len(body["filters"]["recipient_locations"]) == 2
    assert body["filters"]["recipient_locations"][0] == {
        "country": "USA",
        "state": "AZ",
    }
    assert body["filters"]["time_period"][0]["start_date"] == "2020-01-01"
    assert body["page"] == 1
    assert body["limit"] == 100
    assert body["sort"] == "Award Amount"


def test_parse_result_valid():
    raw = {
        "Award ID": "W911NF-24-C-0001",
        "Recipient Name": "Desert Defense Inc",
        "Start Date": "2024-03-01",
        "End Date": "2025-03-01",
        "Award Amount": 2500000.0,
        "Awarding Agency": "Department of Defense",
        "Awarding Sub Agency": "Army",
        "NAICS Code": "336414",
        "NAICS Description": "Guided Missile Manufacturing",
    }

    award = _parse_result(raw)
    assert award is not None
    assert award.award_id == "W911NF-24-C-0001"
    assert award.source == "usa_spending"
    assert award.recipient_name == "Desert Defense Inc"
    assert award.signed_date == date(2024, 3, 1)
    assert award.obligation_amount == 2500000.0
    assert award.naics_code == "336414"


def test_parse_result_missing_name():
    raw = {
        "Award ID": "X-001",
        "Recipient Name": "",
        "Award Amount": 100,
    }
    assert _parse_result(raw) is None


def test_parse_result_bad_date():
    raw = {
        "Award ID": "X-002",
        "Recipient Name": "Test Corp",
        "Start Date": "not-a-date",
        "Award Amount": 500,
        "Awarding Agency": "NASA",
        "NAICS Code": "541715",
        "NAICS Description": "R&D",
    }
    award = _parse_result(raw)
    assert award is not None
    assert award.signed_date is None


def test_parse_result_null_amount():
    raw = {
        "Award ID": "X-003",
        "Recipient Name": "Test Corp",
        "Start Date": "2024-01-01",
        "Award Amount": None,
        "Awarding Agency": "DOD",
        "NAICS Code": "334511",
    }
    award = _parse_result(raw)
    assert award is not None
    assert award.obligation_amount == 0.0


def test_group_by_recipient():
    awards = [
        ContractAward(
            award_id="A-001",
            source="usa_spending",
            recipient_name="Desert Defense Inc",
            awarding_agency="DOD",
            naics_code="336414",
            signed_date=date(2024, 1, 1),
            obligation_amount=100_000,
        ),
        ContractAward(
            award_id="A-002",
            source="usa_spending",
            recipient_name="DESERT DEFENSE INC",
            awarding_agency="NASA",
            naics_code="541715",
            signed_date=date(2024, 6, 1),
            obligation_amount=200_000,
        ),
    ]

    prospects = _group_by_recipient(awards, [])
    assert len(prospects) == 1
    assert len(prospects[0].contract_awards) == 2
    assert "usa_spending" in prospects[0].data_sources

"""Tests for USASpending source module."""

from datetime import date

from prospect_engine.models.prospect import ContractAward
from prospect_engine.sources.usa_spending import (
    _build_request_body,
    _parse_result,
    _group_by_recipient,
    _filter_by_keywords,
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
    assert body["sort"] == "Start Date"
    assert body["order"] == "desc"
    # No award_amounts cap — primes filter handles large companies
    assert "award_amounts" not in body["filters"]
    # internal_id causes HTTP 500 (API bug), must not be in fields
    assert "internal_id" not in body["fields"]
    assert "generated_internal_id" in body["fields"]


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


def test_build_request_body_with_agencies():
    agencies = [
        {"type": "awarding", "tier": "toptier", "name": "Department of Defense"},
        {"type": "awarding", "tier": "toptier", "name": "National Aeronautics and Space Administration"},
    ]
    body = _build_request_body(
        states=["AZ"],
        naics_codes=["336414"],
        start_date="2020-01-01",
        end_date="2024-12-31",
        page=1,
        limit=100,
        agencies=agencies,
    )
    assert "agencies" in body["filters"]
    assert len(body["filters"]["agencies"]) == 2
    assert body["filters"]["agencies"][0]["name"] == "Department of Defense"


def test_build_request_body_no_agencies():
    body = _build_request_body(
        states=["AZ"],
        naics_codes=["336414"],
        start_date="2020-01-01",
        end_date="2024-12-31",
        page=1,
        limit=100,
        agencies=None,
    )
    # When agencies=None, no agencies key should appear in filters
    assert "agencies" not in body["filters"]


def test_filter_by_keywords_matches():
    awards = [
        ContractAward(
            award_id="K-001",
            source="usa_spending",
            recipient_name="Rocket Corp",
            awarding_agency="DOD",
            naics_code="336414",
            signed_date=date(2024, 1, 1),
            obligation_amount=500_000,
            description="Design and manufacture of satellite antenna systems",
        ),
    ]
    filtered = _filter_by_keywords(awards)
    assert len(filtered) == 1


def test_filter_by_keywords_no_match_removed():
    awards = [
        ContractAward(
            award_id="K-002",
            source="usa_spending",
            recipient_name="Catering Inc",
            awarding_agency="DOD",
            naics_code="722310",
            signed_date=date(2024, 1, 1),
            obligation_amount=100_000,
            description="Food service and cafeteria management for base facilities",
        ),
    ]
    filtered = _filter_by_keywords(awards)
    assert len(filtered) == 0


def test_filter_by_keywords_empty_description_retained():
    awards = [
        ContractAward(
            award_id="K-003",
            source="usa_spending",
            recipient_name="Mystery Corp",
            awarding_agency="NASA",
            naics_code="336414",
            signed_date=date(2024, 1, 1),
            obligation_amount=200_000,
            description="",
        ),
        ContractAward(
            award_id="K-004",
            source="usa_spending",
            recipient_name="Null Desc Corp",
            awarding_agency="DOD",
            naics_code="336414",
            signed_date=date(2024, 3, 1),
            obligation_amount=300_000,
            description=None,
        ),
    ]
    filtered = _filter_by_keywords(awards)
    assert len(filtered) == 2  # Both retained — benefit of the doubt


def test_filter_by_keywords_case_insensitive():
    awards = [
        ContractAward(
            award_id="K-005",
            source="usa_spending",
            recipient_name="Radar Inc",
            awarding_agency="DOD",
            naics_code="334511",
            signed_date=date(2024, 1, 1),
            obligation_amount=750_000,
            description="ADVANCED RADAR DETECTION SYSTEM PROTOTYPE",
        ),
    ]
    filtered = _filter_by_keywords(awards)
    assert len(filtered) == 1


def test_filter_by_keywords_custom_keywords():
    awards = [
        ContractAward(
            award_id="K-006",
            source="usa_spending",
            recipient_name="Widget Corp",
            awarding_agency="DOD",
            naics_code="336414",
            signed_date=date(2024, 1, 1),
            obligation_amount=100_000,
            description="Widget assembly for test purposes",
        ),
    ]
    # Should match with custom keywords
    filtered = _filter_by_keywords(awards, keywords=["widget"])
    assert len(filtered) == 1

    # Should not match with unrelated keywords
    filtered = _filter_by_keywords(awards, keywords=["satellite", "rocket"])
    assert len(filtered) == 0


def test_filter_by_keywords_empty_list_returns_all():
    awards = [
        ContractAward(
            award_id="K-007",
            source="usa_spending",
            recipient_name="Any Corp",
            awarding_agency="DOD",
            naics_code="336414",
            signed_date=date(2024, 1, 1),
            obligation_amount=100_000,
            description="Totally unrelated work",
        ),
    ]
    # Empty keywords list means no filtering
    filtered = _filter_by_keywords(awards, keywords=[])
    assert len(filtered) == 1

"""Tests for SAM.gov source module."""

from datetime import date

import pytest

from prospect_engine.models.prospect import ContractAward
from prospect_engine.sources.sam_gov import _parse_award, _group_by_recipient


SAMPLE_RAW_AWARD = {
    "contractId": {"piid": "FA8650-23-C-9999"},
    "awardDetails": {
        "awardeeData": {
            "recipientName": "Acme Aerospace LLC",
            "ueiSAM": "UEI123456",
        },
        "dollars": {"actionObligation": 1500000.0},
        "dates": {"signedDate": "03/15/2024"},
        "contractData": {
            "naicsCode": "336414",
            "descriptionOfRequirement": "Missile guidance system",
        },
        "fundingAgency": {"name": "Department of Defense"},
    },
}


def test_parse_award_valid():
    award = _parse_award(SAMPLE_RAW_AWARD)
    assert award is not None
    assert award.award_id == "FA8650-23-C-9999"
    assert award.source == "sam_gov"
    assert award.recipient_name == "Acme Aerospace LLC"
    assert award.awarding_agency == "Department of Defense"
    assert award.naics_code == "336414"
    assert award.signed_date == date(2024, 3, 15)
    assert award.obligation_amount == 1500000.0
    assert award.description == "Missile guidance system"


def test_parse_award_missing_recipient():
    raw = {
        "contractId": {"piid": "X-001"},
        "awardDetails": {
            "awardeeData": {"recipientName": ""},
            "dollars": {"actionObligation": 100},
            "dates": {"signedDate": "01/01/2024"},
            "contractData": {"naicsCode": "336414"},
            "fundingAgency": {"name": "NASA"},
        },
    }
    assert _parse_award(raw) is None


def test_parse_award_bad_date():
    raw = {
        "contractId": {"piid": "X-002"},
        "awardDetails": {
            "awardeeData": {"recipientName": "Test Corp"},
            "dollars": {"actionObligation": 200},
            "dates": {"signedDate": "bad-date"},
            "contractData": {"naicsCode": "541715"},
            "fundingAgency": {"name": "DOD"},
        },
    }
    award = _parse_award(raw)
    assert award is not None
    assert award.signed_date is None


def test_parse_award_missing_obligation():
    raw = {
        "contractId": {"piid": "X-003"},
        "awardDetails": {
            "awardeeData": {"recipientName": "Test Corp"},
            "dollars": {},
            "dates": {"signedDate": "06/01/2024"},
            "contractData": {"naicsCode": "541330"},
            "fundingAgency": {"name": "NASA"},
        },
    }
    award = _parse_award(raw)
    assert award is not None
    assert award.obligation_amount == 0.0


def test_group_by_recipient():
    awards = [
        ContractAward(
            award_id="A-001",
            source="sam_gov",
            recipient_name="Acme Aerospace LLC",
            awarding_agency="DOD",
            naics_code="336414",
            signed_date=date(2024, 1, 1),
            obligation_amount=100_000,
        ),
        ContractAward(
            award_id="A-002",
            source="sam_gov",
            recipient_name="ACME AEROSPACE LLC",  # same company, different case
            awarding_agency="NASA",
            naics_code="541715",
            signed_date=date(2024, 6, 1),
            obligation_amount=200_000,
        ),
        ContractAward(
            award_id="B-001",
            source="sam_gov",
            recipient_name="Other Corp",
            awarding_agency="DOD",
            naics_code="334511",
            signed_date=date(2024, 3, 1),
            obligation_amount=50_000,
        ),
    ]

    prospects = _group_by_recipient(awards)
    assert len(prospects) == 2

    # Find the Acme prospect
    acme = [
        p for p in prospects if "Acme" in p.company_name or "ACME" in p.company_name
    ]
    assert len(acme) == 1
    assert len(acme[0].contract_awards) == 2
    assert "sam_gov" in acme[0].data_sources


def test_fetch_raises_without_api_key():
    from prospect_engine.sources.sam_gov import fetch

    # SAM_GOV_API_KEY is empty by default in test environment
    with pytest.raises(ValueError, match="SAM_GOV_API_KEY"):
        fetch()

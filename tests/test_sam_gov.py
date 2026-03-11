"""Tests for SAM.gov source module."""

from datetime import date

import pytest

from prospect_engine.models.prospect import ContractAward
from prospect_engine.sources.sam_gov import (
    _parse_award,
    _filter_by_amount,
    _group_by_recipient,
)


# Sample matching the real SAM.gov Contract Awards v1 API structure.
SAMPLE_RAW_AWARD = {
    "contractId": {
        "subtier": {"code": "9700", "name": "DEPT OF DEFENSE"},
        "piid": "FA8650-23-C-9999",
        "modificationNumber": "P00001",
    },
    "coreData": {
        "federalOrganization": {
            "contractingInformation": {
                "contractingDepartment": {
                    "code": "9700",
                    "name": "DEPT OF DEFENSE",
                },
                "contractingSubtier": {
                    "code": "5700",
                    "name": "DEPT OF THE AIR FORCE",
                },
                "contractingOffice": {
                    "code": "FA8650",
                    "name": "AFLCMC/XP",
                },
            },
        },
        "productOrServiceInformation": {
            "principalNaics": [
                {
                    "code": "336414",
                    "name": "GUIDED MISSILE AND SPACE VEHICLE MANUFACTURING",
                }
            ],
        },
    },
    "awardDetails": {
        "dates": {
            "dateSigned": "2024-03-15T00:00:00Z",
            "fiscalYear": "2024",
        },
        "dollars": {
            "actionObligation": "1500000",
            "baseDollarsObligated": "1500000",
        },
        "productOrServiceInformation": {
            "descriptionOfContractRequirement": "Missile guidance system",
        },
        "awardeeData": {
            "awardeeHeader": {
                "awardeeName": "Acme Aerospace LLC",
                "legalBusinessName": "ACME AEROSPACE LLC",
            },
            "awardeeUEIInformation": {
                "uniqueEntityId": "UEI123456",
            },
            "awardeeLocation": {
                "city": "Tucson",
                "state": {"code": "AZ", "name": "ARIZONA"},
                "zip": "857060001",
                "country": {"code": "USA", "name": "UNITED STATES"},
            },
        },
    },
}


def test_parse_award_valid():
    award = _parse_award(SAMPLE_RAW_AWARD)
    assert award is not None
    assert award.award_id == "FA8650-23-C-9999"
    assert award.source == "sam_gov"
    assert award.recipient_name == "Acme Aerospace LLC"
    assert award.awarding_agency == "DEPT OF THE AIR FORCE"
    assert award.naics_code == "336414"
    assert award.signed_date == date(2024, 3, 15)
    assert award.obligation_amount == 1500000.0
    assert award.description == "Missile guidance system"
    assert award.piid == "FA8650-23-C-9999"
    assert "FA8650-23-C-9999" in award.source_url


def test_parse_award_missing_recipient():
    raw = {
        "contractId": {"piid": "X-001"},
        "coreData": {},
        "awardDetails": {
            "awardeeData": {
                "awardeeHeader": {"awardeeName": ""},
                "awardeeUEIInformation": {},
            },
            "dollars": {"actionObligation": "100"},
            "dates": {"dateSigned": "2024-01-01T00:00:00Z"},
            "productOrServiceInformation": {},
        },
    }
    assert _parse_award(raw) is None


def test_parse_award_iso_date():
    raw = {
        "contractId": {"piid": "X-002"},
        "coreData": {},
        "awardDetails": {
            "awardeeData": {
                "awardeeHeader": {"awardeeName": "Test Corp"},
                "awardeeUEIInformation": {},
            },
            "dollars": {"actionObligation": "200"},
            "dates": {"dateSigned": "2024-06-01T12:00:00Z"},
            "productOrServiceInformation": {},
        },
    }
    award = _parse_award(raw)
    assert award is not None
    assert award.signed_date == date(2024, 6, 1)


def test_parse_award_bad_date():
    raw = {
        "contractId": {"piid": "X-003"},
        "coreData": {},
        "awardDetails": {
            "awardeeData": {
                "awardeeHeader": {"awardeeName": "Test Corp"},
                "awardeeUEIInformation": {},
            },
            "dollars": {"actionObligation": "200"},
            "dates": {"dateSigned": "bad-date"},
            "productOrServiceInformation": {},
        },
    }
    award = _parse_award(raw)
    assert award is not None
    assert award.signed_date is None


def test_parse_award_missing_obligation():
    raw = {
        "contractId": {"piid": "X-004"},
        "coreData": {},
        "awardDetails": {
            "awardeeData": {
                "awardeeHeader": {"awardeeName": "Test Corp"},
                "awardeeUEIInformation": {},
            },
            "dollars": {},
            "dates": {"dateSigned": "2024-06-01T00:00:00Z"},
            "productOrServiceInformation": {},
        },
    }
    award = _parse_award(raw)
    assert award is not None
    assert award.obligation_amount == 0.0


def test_parse_award_string_obligation():
    """API returns actionObligation as a string, not a float."""
    raw = {
        "contractId": {"piid": "X-005"},
        "coreData": {},
        "awardDetails": {
            "awardeeData": {
                "awardeeHeader": {"awardeeName": "Test Corp"},
                "awardeeUEIInformation": {},
            },
            "dollars": {"actionObligation": "2500000"},
            "dates": {"dateSigned": "2024-01-01T00:00:00Z"},
            "productOrServiceInformation": {},
        },
    }
    award = _parse_award(raw)
    assert award is not None
    assert award.obligation_amount == 2500000.0


def test_parse_award_falls_back_to_department():
    """When contractingSubtier is absent, fall back to contractingDepartment."""
    raw = {
        "contractId": {"piid": "X-006"},
        "coreData": {
            "federalOrganization": {
                "contractingInformation": {
                    "contractingDepartment": {
                        "code": "8000",
                        "name": "NASA",
                    },
                },
            },
        },
        "awardDetails": {
            "awardeeData": {
                "awardeeHeader": {"awardeeName": "Space Corp"},
                "awardeeUEIInformation": {},
            },
            "dollars": {"actionObligation": "500000"},
            "dates": {"dateSigned": "2024-02-01T00:00:00Z"},
            "productOrServiceInformation": {},
        },
    }
    award = _parse_award(raw)
    assert award is not None
    assert award.awarding_agency == "NASA"


def test_filter_by_amount():
    awards = [
        ContractAward(
            award_id="A-001",
            source="sam_gov",
            recipient_name="Small Co",
            awarding_agency="DOD",
            naics_code="336414",
            signed_date=date(2024, 1, 1),
            obligation_amount=500_000,
        ),
        ContractAward(
            award_id="A-002",
            source="sam_gov",
            recipient_name="Big Co",
            awarding_agency="NASA",
            naics_code="336414",
            signed_date=date(2024, 6, 1),
            obligation_amount=2_000_000,
        ),
        ContractAward(
            award_id="A-003",
            source="sam_gov",
            recipient_name="Exact Co",
            awarding_agency="DOD",
            naics_code="336414",
            signed_date=date(2024, 3, 1),
            obligation_amount=1_000_000,
        ),
    ]
    filtered = _filter_by_amount(awards, 1_000_000)
    assert len(filtered) == 2
    amounts = {a.obligation_amount for a in filtered}
    assert amounts == {2_000_000, 1_000_000}


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


def test_fetch_raises_without_api_key(monkeypatch):
    import prospect_engine.sources.sam_gov as sam_mod

    monkeypatch.setattr(sam_mod, "SAM_GOV_API_KEY", "")
    with pytest.raises(ValueError, match="SAM_GOV_API_KEY"):
        sam_mod.fetch()

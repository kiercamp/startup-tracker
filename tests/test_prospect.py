"""Tests for the Prospect data models."""

from datetime import date

from prospect_engine.models.prospect import (
    ContractAward,
    SbirAward,
    VcRound,
    Prospect,
)


def test_contract_award_creation():
    award = ContractAward(
        award_id="FA8650-23-C-1234",
        source="sam_gov",
        recipient_name="Acme Aerospace LLC",
        awarding_agency="Department of Defense",
        naics_code="336414",
        signed_date=date(2024, 3, 15),
        obligation_amount=1_500_000.0,
    )
    assert award.award_id == "FA8650-23-C-1234"
    assert award.source == "sam_gov"
    assert award.obligation_amount == 1_500_000.0
    assert award.description == ""
    assert award.piid == ""


def test_sbir_award_creation():
    award = SbirAward(
        award_id="SBIR-2024-001",
        firm="Test Space Inc",
        agency="DOD",
        phase="Phase II",
        program="SBIR",
        award_title="Advanced Propulsion System",
        award_amount=750_000.0,
        award_date=date(2024, 6, 1),
        state="AZ",
        city="Tucson",
    )
    assert award.phase == "Phase II"
    assert award.program == "SBIR"
    assert award.abstract == ""


def test_vc_round_creation():
    vc = VcRound(
        round_id="vc-001",
        company_name="NewSpace Corp",
        round_type="Series A",
        amount_usd=10_000_000.0,
        announced_date=date(2024, 1, 15),
        lead_investor="Lockheed Ventures",
    )
    assert vc.round_type == "Series A"
    assert vc.source == "stub"


def test_prospect_creation_defaults():
    p = Prospect(company_name="Test Corp")
    assert p.company_name == "Test Corp"
    assert p.uei == ""
    assert p.contract_awards == []
    assert p.sbir_awards == []
    assert p.vc_rounds == []
    assert p.contract_count == 0
    assert p.total_funding == 0.0
    assert p.outreach_flags == []
    assert p.founded_year is None
    assert p.data_sources == []


def test_prospect_with_signals():
    award = ContractAward(
        award_id="C-001",
        source="usa_spending",
        recipient_name="Test Corp",
        awarding_agency="NASA",
        naics_code="541715",
        signed_date=date(2024, 5, 1),
        obligation_amount=500_000.0,
    )
    p = Prospect(
        company_name="Test Corp",
        uei="ABC123",
        state="TX",
        contract_awards=[award],
        data_sources=["usa_spending"],
    )
    assert len(p.contract_awards) == 1
    assert p.contract_awards[0].obligation_amount == 500_000.0
    assert p.state == "TX"

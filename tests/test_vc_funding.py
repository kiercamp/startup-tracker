"""Tests for VC funding source module."""

from prospect_engine.sources.vc_funding import (
    StubFundingSource,
    FundingSource,
    fetch,
    _group_by_company,
)
from prospect_engine.models.prospect import VcRound
from datetime import date


def test_stub_implements_protocol():
    stub = StubFundingSource()
    assert isinstance(stub, FundingSource)


def test_stub_returns_data():
    stub = StubFundingSource()
    rounds = stub.get_rounds(states=["AZ", "TX"], lookback_days=365)
    assert len(rounds) > 0
    for r in rounds:
        assert isinstance(r, VcRound)
        assert r.company_name
        assert r.round_type
        assert r.source == "stub"


def test_stub_has_known_companies():
    stub = StubFundingSource()
    rounds = stub.get_rounds(states=["AZ"], lookback_days=365)
    names = {r.company_name for r in rounds}
    assert "Loft Orbital" in names
    assert "Optisys" in names


def test_fetch_returns_prospects():
    prospects = fetch()
    assert len(prospects) > 0
    for p in prospects:
        assert len(p.vc_rounds) > 0
        assert "vc_funding" in p.data_sources


def test_group_by_company():
    rounds = [
        VcRound(
            round_id="vc-001",
            company_name="Acme Space",
            round_type="Seed",
            amount_usd=2_000_000,
            announced_date=date(2024, 1, 1),
        ),
        VcRound(
            round_id="vc-002",
            company_name="Acme Space",
            round_type="Series A",
            amount_usd=10_000_000,
            announced_date=date(2024, 6, 1),
        ),
        VcRound(
            round_id="vc-003",
            company_name="Other Corp",
            round_type="Seed",
            amount_usd=1_000_000,
            announced_date=date(2024, 3, 1),
        ),
    ]

    prospects = _group_by_company(rounds)
    assert len(prospects) == 2

    acme = [p for p in prospects if "Acme" in p.company_name]
    assert len(acme) == 1
    assert len(acme[0].vc_rounds) == 2

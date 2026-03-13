"""Tests for enrichment/company_profile module."""

from datetime import date

from prospect_engine.models.prospect import (
    ContractAward,
    SbirAward,
    VcRound,
    Prospect,
)
from prospect_engine.enrichment.company_profile import (
    normalize_company_name,
    merge_sources,
    enrich_prospect,
    build_outreach_flags,
    filter_by_founded_year,
    filter_known_primes,
    filter_excluded_companies,
)


# --- normalize_company_name ---


def test_normalize_basic():
    assert normalize_company_name("Acme Corp.") == "acme"


def test_normalize_llc():
    assert normalize_company_name("Acme Aerospace LLC") == "acme aerospace"


def test_normalize_case_and_whitespace():
    assert normalize_company_name("  ACME   AEROSPACE  INC  ") == "acme aerospace"


def test_normalize_punctuation():
    assert normalize_company_name("Acme, Inc.") == "acme"


# --- merge_sources ---


def test_merge_by_name():
    """Two prospects with same company name from different sources merge."""
    p1 = Prospect(
        company_name="Acme Aerospace LLC",
        contract_awards=[
            ContractAward(
                award_id="C-001",
                source="sam_gov",
                recipient_name="Acme Aerospace LLC",
                awarding_agency="DOD",
                naics_code="336414",
                signed_date=date(2024, 1, 1),
                obligation_amount=100_000,
            )
        ],
        data_sources=["sam_gov"],
    )
    p2 = Prospect(
        company_name="Acme Aerospace",
        sbir_awards=[
            SbirAward(
                award_id="S-001",
                firm="Acme Aerospace",
                agency="DOD",
                phase="Phase I",
                program="SBIR",
                award_title="Test",
                award_amount=50_000,
                award_date=date(2024, 6, 1),
                state="AZ",
                city="Tucson",
            )
        ],
        state="AZ",
        data_sources=["sbir"],
    )

    merged = merge_sources([[p1], [p2]])
    assert len(merged) == 1
    assert len(merged[0].contract_awards) == 1
    assert len(merged[0].sbir_awards) == 1
    assert "sam_gov" in merged[0].data_sources
    assert "sbir" in merged[0].data_sources
    assert merged[0].state == "AZ"


def test_merge_by_uei():
    """Two prospects with same UEI from different sources merge."""
    p1 = Prospect(
        company_name="Acme LLC",
        uei="UEI123",
        contract_awards=[
            ContractAward(
                award_id="C-001",
                source="usa_spending",
                recipient_name="Acme LLC",
                awarding_agency="NASA",
                naics_code="541715",
                signed_date=None,
                obligation_amount=50_000,
            )
        ],
        data_sources=["usa_spending"],
    )
    p2 = Prospect(
        company_name="Acme Corp",
        uei="UEI123",
        sbir_awards=[
            SbirAward(
                award_id="S-001",
                firm="Acme Corp",
                agency="DOD",
                phase="Phase II",
                program="SBIR",
                award_title="Test",
                award_amount=500_000,
                award_date=date(2024, 3, 1),
                state="TX",
                city="Austin",
                uei="UEI123",
            )
        ],
        state="TX",
        data_sources=["sbir"],
    )

    merged = merge_sources([[p1], [p2]])
    assert len(merged) == 1
    assert len(merged[0].contract_awards) == 1
    assert len(merged[0].sbir_awards) == 1


def test_merge_different_companies_stay_separate():
    p1 = Prospect(company_name="Alpha Corp", data_sources=["sam_gov"])
    p2 = Prospect(company_name="Beta Inc", data_sources=["sbir"])

    merged = merge_sources([[p1], [p2]])
    assert len(merged) == 2


# --- enrich_prospect ---


def test_enrich_computes_totals():
    p = Prospect(
        company_name="Test Corp",
        contract_awards=[
            ContractAward(
                award_id="C-001",
                source="sam_gov",
                recipient_name="Test Corp",
                awarding_agency="DOD",
                naics_code="336414",
                signed_date=date(2024, 1, 1),
                obligation_amount=100_000,
            ),
            ContractAward(
                award_id="C-002",
                source="usa_spending",
                recipient_name="Test Corp",
                awarding_agency="NASA",
                naics_code="541715",
                signed_date=date(2024, 6, 1),
                obligation_amount=200_000,
            ),
        ],
        sbir_awards=[
            SbirAward(
                award_id="S-001",
                firm="Test Corp",
                agency="DOD",
                phase="Phase I",
                program="SBIR",
                award_title="Test",
                award_amount=50_000,
                award_date=date(2024, 3, 1),
                state="AZ",
                city="Tucson",
            ),
            SbirAward(
                award_id="S-002",
                firm="Test Corp",
                agency="DOD",
                phase="Phase II",
                program="SBIR",
                award_title="Test 2",
                award_amount=500_000,
                award_date=date(2024, 9, 1),
                state="AZ",
                city="Tucson",
            ),
        ],
        vc_rounds=[
            VcRound(
                round_id="vc-001",
                company_name="Test Corp",
                round_type="Series A",
                amount_usd=10_000_000,
                announced_date=date(2024, 5, 1),
            ),
        ],
    )

    enriched = enrich_prospect(p)
    assert enriched.contract_count == 2
    assert enriched.total_contract_obligation == 300_000
    assert enriched.sbir_phase_i_count == 1
    assert enriched.sbir_phase_ii_count == 1
    assert enriched.total_sbir_amount == 550_000
    assert enriched.total_vc_raised == 10_000_000
    assert enriched.total_funding == 300_000 + 550_000 + 10_000_000
    assert enriched.latest_contract_date == date(2024, 6, 1)
    assert enriched.latest_sbir_date == date(2024, 9, 1)
    assert enriched.latest_vc_date == date(2024, 5, 1)


# --- build_outreach_flags ---


def test_outreach_flags_sbir():
    p = Prospect(
        company_name="Test Corp",
        sbir_awards=[
            SbirAward(
                award_id="S-001",
                firm="Test Corp",
                agency="DOD",
                phase="Phase I",
                program="SBIR",
                award_title="Antenna Array",
                award_amount=100_000,
                award_date=date(2024, 10, 1),
                state="AZ",
                city="Tucson",
            ),
        ],
    )

    flagged = build_outreach_flags(p, reference_date=date(2024, 12, 1))
    assert len(flagged.outreach_flags) >= 1
    assert any("SBIR Phase I" in f for f in flagged.outreach_flags)


def test_outreach_flags_contract():
    p = Prospect(
        company_name="Test Corp",
        contract_awards=[
            ContractAward(
                award_id="C-001",
                source="sam_gov",
                recipient_name="Test Corp",
                awarding_agency="Department of Defense",
                naics_code="336414",
                signed_date=date(2024, 11, 15),
                obligation_amount=500_000,
            ),
        ],
    )

    flagged = build_outreach_flags(p, reference_date=date(2024, 12, 1))
    assert len(flagged.outreach_flags) >= 1
    assert any("SAM.gov contract" in f for f in flagged.outreach_flags)


def test_outreach_flags_vc():
    p = Prospect(
        company_name="Test Corp",
        vc_rounds=[
            VcRound(
                round_id="vc-001",
                company_name="Test Corp",
                round_type="Series A",
                amount_usd=10_000_000,
                announced_date=date(2024, 11, 1),
            ),
        ],
    )

    flagged = build_outreach_flags(p, reference_date=date(2024, 12, 1))
    assert len(flagged.outreach_flags) >= 1
    assert any("VC Series A" in f for f in flagged.outreach_flags)


def test_outreach_flags_seed_not_flagged():
    """Seed rounds should NOT trigger VC outreach flags."""
    p = Prospect(
        company_name="Test Corp",
        vc_rounds=[
            VcRound(
                round_id="vc-001",
                company_name="Test Corp",
                round_type="Seed",
                amount_usd=2_000_000,
                announced_date=date(2024, 11, 1),
            ),
        ],
    )

    flagged = build_outreach_flags(p, reference_date=date(2024, 12, 1))
    assert len(flagged.outreach_flags) == 0


def test_outreach_flags_old_events_not_flagged():
    """Events outside lookback window should not be flagged."""
    p = Prospect(
        company_name="Test Corp",
        sbir_awards=[
            SbirAward(
                award_id="S-001",
                firm="Test Corp",
                agency="DOD",
                phase="Phase II",
                program="SBIR",
                award_title="Old Award",
                award_amount=500_000,
                award_date=date(2023, 1, 1),
                state="AZ",
                city="Tucson",
            ),
        ],
    )

    flagged = build_outreach_flags(p, reference_date=date(2024, 12, 1))
    assert len(flagged.outreach_flags) == 0


def test_outreach_flags_dod_innovation():
    """AFWERX and other DoD innovation programs should trigger a flag."""
    p = Prospect(
        company_name="Test Corp",
        contract_awards=[
            ContractAward(
                award_id="C-001",
                source="sam_gov",
                recipient_name="Test Corp",
                awarding_agency="AFWERX",
                naics_code="336414",
                signed_date=date(2024, 11, 1),
                obligation_amount=200_000,
                description="AFWERX SBIR Phase I Challenge",
            ),
        ],
    )

    flagged = build_outreach_flags(p, reference_date=date(2024, 12, 1))
    innovation_flags = [f for f in flagged.outreach_flags if "DoD Innovation" in f]
    assert len(innovation_flags) >= 1
    assert "AFWERX" in innovation_flags[0]


# --- filter_by_founded_year ---


def test_filter_keeps_recent():
    prospects = [
        Prospect(company_name="New Corp", founded_year=2020),
        Prospect(company_name="Old Corp", founded_year=2005),
        Prospect(company_name="Unknown Corp"),  # no founded_year
    ]

    filtered = filter_by_founded_year(prospects, max_age_years=10, reference_year=2025)
    names = {p.company_name for p in filtered}
    assert "New Corp" in names
    assert "Unknown Corp" in names  # retained (no data to disqualify)
    assert "Old Corp" not in names


# --- filter_known_primes ---


def test_filter_known_primes_removes_lockheed():
    """Known primes are filtered out by substring match."""
    prospects = [
        Prospect(company_name="LOCKHEED MARTIN CORPORATION"),
        Prospect(company_name="Desert Defense Inc"),
    ]
    filtered = filter_known_primes(prospects)
    names = {p.company_name for p in filtered}
    assert "LOCKHEED MARTIN CORPORATION" not in names
    assert "Desert Defense Inc" in names


def test_filter_known_primes_keeps_startups():
    """Startups not in primes list are retained."""
    prospects = [
        Prospect(company_name="SpaceX Innovations LLC"),
        Prospect(company_name="Rocket Propulsion Inc"),
    ]
    filtered = filter_known_primes(prospects)
    assert len(filtered) == 2


def test_filter_known_primes_case_insensitive():
    """Matching works regardless of case."""
    prospects = [
        Prospect(company_name="The Boeing Company"),
        Prospect(company_name="RAYTHEON COMPANY"),
    ]
    filtered = filter_known_primes(prospects)
    assert len(filtered) == 0


def test_filter_known_primes_custom_list():
    """Custom primes list can be provided."""
    prospects = [
        Prospect(company_name="Custom Prime Corp"),
        Prospect(company_name="Good Startup LLC"),
    ]
    filtered = filter_known_primes(prospects, primes_list=["custom prime"])
    names = {p.company_name for p in filtered}
    assert "Custom Prime Corp" not in names
    assert "Good Startup LLC" in names


# --- filter_excluded_companies ---


def test_filter_excluded_removes_university():
    prospects = [
        Prospect(company_name="University of Arizona Research Foundation"),
        Prospect(company_name="Desert Antenna Systems LLC"),
    ]
    filtered = filter_excluded_companies(prospects)
    names = {p.company_name for p in filtered}
    assert "Desert Antenna Systems LLC" in names
    assert "University of Arizona Research Foundation" not in names


def test_filter_excluded_removes_construction():
    prospects = [
        Prospect(company_name="Cerris Builders Construction Corp"),
        Prospect(company_name="Acme Aerospace Inc"),
    ]
    filtered = filter_excluded_companies(prospects)
    assert len(filtered) == 1
    assert filtered[0].company_name == "Acme Aerospace Inc"


def test_filter_excluded_removes_telecom():
    prospects = [
        Prospect(company_name="Lumen Technologies Services Group"),
        Prospect(company_name="Level 3 Communications LLC"),
        Prospect(company_name="Rocket Propulsion LLC"),
    ]
    filtered = filter_excluded_companies(prospects)
    assert len(filtered) == 1
    assert filtered[0].company_name == "Rocket Propulsion LLC"


def test_filter_excluded_keeps_aerospace():
    prospects = [
        Prospect(company_name="Tucson Antenna Systems"),
        Prospect(company_name="Desert Propulsion LLC"),
        Prospect(company_name="Mesa Satellite Corp"),
    ]
    filtered = filter_excluded_companies(prospects)
    assert len(filtered) == 3  # All kept — none match exclusion patterns


def test_filter_excluded_custom_patterns():
    prospects = [
        Prospect(company_name="Widget Factory Inc"),
        Prospect(company_name="Missile Systems Corp"),
    ]
    filtered = filter_excluded_companies(prospects, patterns=["widget"])
    assert len(filtered) == 1
    assert filtered[0].company_name == "Missile Systems Corp"


def test_filter_excluded_empty_patterns_keeps_all():
    prospects = [
        Prospect(company_name="Anything Corp"),
    ]
    filtered = filter_excluded_companies(prospects, patterns=[])
    assert len(filtered) == 1

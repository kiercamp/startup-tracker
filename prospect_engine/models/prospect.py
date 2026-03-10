"""Core data models for the A&D Prospect Engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional


@dataclass
class ContractAward:
    """A single DoD/NASA contract award from SAM.gov or USASpending."""

    award_id: str
    source: str  # "sam_gov" | "usa_spending"
    recipient_name: str
    awarding_agency: str
    naics_code: str
    signed_date: Optional[date]
    obligation_amount: float
    description: str = ""
    piid: str = ""


@dataclass
class SbirAward:
    """A single SBIR/STTR award from sbir.gov."""

    award_id: str
    firm: str
    agency: str
    phase: str  # "Phase I" | "Phase II" | "Phase III"
    program: str  # "SBIR" | "STTR"
    award_title: str
    award_amount: float
    award_date: Optional[date]
    state: str
    city: str
    abstract: str = ""
    uei: str = ""


@dataclass
class VcRound:
    """A single VC or private funding round."""

    round_id: str
    company_name: str
    round_type: str  # "Seed" | "Series A" | etc.
    amount_usd: Optional[float]
    announced_date: Optional[date]
    lead_investor: str = ""
    source: str = "stub"


@dataclass
class Prospect:
    """Unified record representing a single A&D company prospect."""

    # Identity
    company_name: str
    uei: str = ""
    state: str = ""
    city: str = ""
    naics_codes: List[str] = field(default_factory=list)

    # Raw signals
    contract_awards: List[ContractAward] = field(default_factory=list)
    sbir_awards: List[SbirAward] = field(default_factory=list)
    vc_rounds: List[VcRound] = field(default_factory=list)

    # Derived counts (populated by enrichment)
    contract_count: int = 0
    sbir_phase_i_count: int = 0
    sbir_phase_ii_count: int = 0
    sbir_phase_iii_count: int = 0
    total_contract_obligation: float = 0.0
    total_sbir_amount: float = 0.0
    total_vc_raised: float = 0.0
    total_funding: float = 0.0  # sum of all three — sort key
    latest_contract_date: Optional[date] = None
    latest_sbir_date: Optional[date] = None
    latest_vc_date: Optional[date] = None

    # Outreach flags
    outreach_flags: List[str] = field(default_factory=list)

    # Metadata
    founded_year: Optional[int] = None
    data_sources: List[str] = field(default_factory=list)

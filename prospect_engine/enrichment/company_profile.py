"""Company profile enrichment — merge, dedup, derive, and flag.

After all source fetches are complete, this module:
1. Merges Prospect lists from multiple sources into a deduplicated master list
2. Computes derived summary counts and totals
3. Builds outreach flags for recent funding events
"""

from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from typing import Dict, List, Optional

from prospect_engine.config import (
    FOUNDED_WITHIN_YEARS,
    OUTREACH_LOOKBACK_DAYS,
    DOD_INNOVATION_PROGRAMS,
)

try:
    from prospect_engine.config import KNOWN_DEFENSE_PRIMES
except ImportError:
    KNOWN_DEFENSE_PRIMES: list = []
from prospect_engine.models.prospect import Prospect

logger = logging.getLogger(__name__)

# Suffixes to strip during name normalization
_COMPANY_SUFFIXES = re.compile(
    r"\b(inc|llc|corp|ltd|co|company|incorporated|limited|lp|lc)\b",
    re.IGNORECASE,
)


def normalize_company_name(name: str) -> str:
    """Produce a canonical lowercase key for company name deduplication.

    Strips punctuation, lowercases, collapses whitespace, and removes
    common suffixes (Inc, LLC, Corp, Ltd).

    Args:
        name: Raw company name string.

    Returns:
        Normalized string suitable for use as a dict key.
    """
    name = name.lower()
    name = re.sub(r"[^\w\s]", "", name)
    name = _COMPANY_SUFFIXES.sub("", name)
    return " ".join(name.split())


def merge_sources(source_results: List[List[Prospect]]) -> List[Prospect]:
    """Merge Prospect lists from multiple data sources into a deduplicated master list.

    Merge key priority: UEI > normalized company name.
    For list fields (contract_awards, sbir_awards, vc_rounds), concatenate.
    For scalar fields, prefer the first non-empty/non-None value found.

    Args:
        source_results: List of Prospect lists, one per source module.

    Returns:
        Deduplicated, merged list of Prospect objects.
    """
    # Map merge keys to Prospect objects
    uei_map: Dict[str, Prospect] = {}
    name_map: Dict[str, Prospect] = {}

    for prospect_list in source_results:
        for prospect in prospect_list:
            merged = _find_existing(prospect, uei_map, name_map)
            if merged is not None:
                _merge_into(merged, prospect)
            else:
                # New prospect
                if prospect.uei:
                    uei_map[prospect.uei.upper()] = prospect
                norm_name = normalize_company_name(prospect.company_name)
                if norm_name:
                    name_map[norm_name] = prospect

    # Collect unique prospects (uei_map values may overlap with name_map)
    seen_ids: set = set()
    result: List[Prospect] = []
    for p in list(uei_map.values()) + list(name_map.values()):
        pid = id(p)
        if pid not in seen_ids:
            seen_ids.add(pid)
            result.append(p)

    return result


def _find_existing(
    prospect: Prospect,
    uei_map: Dict[str, Prospect],
    name_map: Dict[str, Prospect],
) -> Optional[Prospect]:
    """Find an existing Prospect that matches the given one by UEI or name.

    Args:
        prospect: The prospect to look up.
        uei_map: UEI -> Prospect mapping.
        name_map: Normalized name -> Prospect mapping.

    Returns:
        The matching existing Prospect, or None.
    """
    if prospect.uei:
        existing = uei_map.get(prospect.uei.upper())
        if existing is not None:
            return existing

    norm_name = normalize_company_name(prospect.company_name)
    if norm_name:
        return name_map.get(norm_name)

    return None


def _merge_into(target: Prospect, source: Prospect) -> None:
    """Merge source prospect data into target prospect in-place.

    Concatenates list fields, prefers non-empty scalar fields from source.

    Args:
        target: The existing prospect to merge into.
        source: The new prospect data to merge from.
    """
    # Concatenate signal lists
    target.contract_awards.extend(source.contract_awards)
    target.sbir_awards.extend(source.sbir_awards)
    target.vc_rounds.extend(source.vc_rounds)

    # Merge scalar fields (prefer non-empty)
    if not target.uei and source.uei:
        target.uei = source.uei
    if not target.state and source.state:
        target.state = source.state
    if not target.city and source.city:
        target.city = source.city
    if target.founded_year is None and source.founded_year is not None:
        target.founded_year = source.founded_year

    # Merge NAICS codes
    existing_naics = set(target.naics_codes)
    for code in source.naics_codes:
        if code not in existing_naics:
            target.naics_codes.append(code)
            existing_naics.add(code)

    # Merge data sources
    existing_sources = set(target.data_sources)
    for src in source.data_sources:
        if src not in existing_sources:
            target.data_sources.append(src)
            existing_sources.add(src)


def enrich_prospect(prospect: Prospect) -> Prospect:
    """Compute derived fields on a merged Prospect.

    Populates: contract_count, sbir_phase_i/ii/iii_count,
    total_contract_obligation, total_sbir_amount, total_vc_raised,
    total_funding, latest dates.

    Args:
        prospect: A merged Prospect with raw signal lists populated.

    Returns:
        The same Prospect with derived fields computed in-place.
    """
    # Contract stats
    prospect.contract_count = len(prospect.contract_awards)
    prospect.total_contract_obligation = sum(
        a.obligation_amount for a in prospect.contract_awards
    )
    contract_dates = [a.signed_date for a in prospect.contract_awards if a.signed_date]
    prospect.latest_contract_date = max(contract_dates) if contract_dates else None

    # SBIR stats
    prospect.sbir_phase_i_count = sum(
        1 for a in prospect.sbir_awards if a.phase == "Phase I"
    )
    prospect.sbir_phase_ii_count = sum(
        1 for a in prospect.sbir_awards if a.phase == "Phase II"
    )
    prospect.sbir_phase_iii_count = sum(
        1 for a in prospect.sbir_awards if a.phase == "Phase III"
    )
    prospect.total_sbir_amount = sum(a.award_amount for a in prospect.sbir_awards)
    sbir_dates = [a.award_date for a in prospect.sbir_awards if a.award_date]
    prospect.latest_sbir_date = max(sbir_dates) if sbir_dates else None

    # VC stats
    prospect.total_vc_raised = sum(
        r.amount_usd for r in prospect.vc_rounds if r.amount_usd
    )
    vc_dates = [r.announced_date for r in prospect.vc_rounds if r.announced_date]
    prospect.latest_vc_date = max(vc_dates) if vc_dates else None

    # Total funding (sort key)
    prospect.total_funding = (
        prospect.total_contract_obligation
        + prospect.total_sbir_amount
        + prospect.total_vc_raised
    )

    # Collect NAICS from awards if not already populated
    naics_set = set(prospect.naics_codes)
    for a in prospect.contract_awards:
        if a.naics_code and a.naics_code not in naics_set:
            prospect.naics_codes.append(a.naics_code)
            naics_set.add(a.naics_code)

    return prospect


def build_outreach_flags(
    prospect: Prospect,
    reference_date: Optional[date] = None,
    lookback_days: int = OUTREACH_LOOKBACK_DAYS,
) -> Prospect:
    """Determine outreach trigger flags for a prospect.

    Flags are generated for recent events:
    - New SBIR award (any phase)
    - New SAM.gov contract award
    - VC round (Series A or later)
    - New USASpending obligation
    - DoD innovation program awards (AFWERX, SpaceWERX, etc.)

    Args:
        prospect: The enriched Prospect.
        reference_date: The reference date for recency checks. Defaults to today.
        lookback_days: Number of days to consider "recent".

    Returns:
        The same Prospect with outreach_flags populated.
    """
    ref = reference_date or date.today()
    cutoff = ref - timedelta(days=lookback_days)
    flags: List[str] = []

    # SBIR awards (any phase)
    for award in prospect.sbir_awards:
        if award.award_date and award.award_date >= cutoff:
            flags.append(
                "SBIR {} award: {} ({})".format(
                    award.phase, award.award_title[:60], award.award_date.isoformat()
                )
            )
            # Check for DoD innovation program
            _check_innovation_program(
                flags,
                agency=award.agency,
                description=award.award_title,
                date_str=award.award_date.isoformat(),
            )

    # Contract awards
    for award in prospect.contract_awards:
        if award.signed_date and award.signed_date >= cutoff:
            source_label = "SAM.gov" if award.source == "sam_gov" else "USASpending"
            flags.append(
                "{} contract: ${:,.0f} from {} ({})".format(
                    source_label,
                    award.obligation_amount,
                    award.awarding_agency[:40],
                    award.signed_date.isoformat(),
                )
            )
            # Check for DoD innovation program
            _check_innovation_program(
                flags,
                agency=award.awarding_agency,
                description=award.description,
                date_str=award.signed_date.isoformat(),
            )

    # VC rounds (Series A or later)
    series_levels = {"Series A", "Series B", "Series C", "Series D", "Series E"}
    for r in prospect.vc_rounds:
        if r.announced_date and r.announced_date >= cutoff:
            if r.round_type in series_levels:
                amount_str = (
                    "${:,.0f}".format(r.amount_usd) if r.amount_usd else "undisclosed"
                )
                flags.append(
                    "VC {}: {} ({})".format(
                        r.round_type, amount_str, r.announced_date.isoformat()
                    )
                )

    prospect.outreach_flags = flags
    return prospect


def _check_innovation_program(
    flags: List[str],
    agency: str,
    description: str,
    date_str: str,
) -> None:
    """Check if an award is from a DoD innovation program and add a flag.

    Args:
        flags: List to append flags to.
        agency: Award agency/sub-agency string.
        description: Award description/title string.
        date_str: Date string for the flag message.
    """
    combined = "{} {}".format(agency, description).upper()
    for program in DOD_INNOVATION_PROGRAMS:
        if program.upper() in combined:
            flags.append(
                "DoD Innovation: {} program detected ({})".format(program, date_str)
            )
            break  # One flag per award


def filter_known_primes(
    prospects: List[Prospect],
    primes_list: Optional[List[str]] = None,
) -> List[Prospect]:
    """Remove prospects matching known large defense prime company names.

    Uses substring matching against normalized company names to catch
    name variations (e.g. "LOCKHEED MARTIN CORPORATION" and
    "LOCKHEED MARTIN CORP" both match the fragment "lockheed martin").

    Args:
        prospects: List of Prospect objects.
        primes_list: List of lowercase name fragments to exclude.
            Defaults to KNOWN_DEFENSE_PRIMES from config.

    Returns:
        Filtered list of Prospect objects with primes removed.
    """
    primes = primes_list or KNOWN_DEFENSE_PRIMES

    def _is_prime(prospect: Prospect) -> bool:
        norm = normalize_company_name(prospect.company_name)
        return any(fragment in norm for fragment in primes)

    before = len(prospects)
    result = [p for p in prospects if not _is_prime(p)]
    removed = before - len(result)
    if removed:
        logger.info(
            "filter_known_primes: removed %d known defense primes, %d remaining",
            removed,
            len(result),
        )
    return result


def filter_by_founded_year(
    prospects: List[Prospect],
    max_age_years: int = FOUNDED_WITHIN_YEARS,
    reference_year: Optional[int] = None,
) -> List[Prospect]:
    """Filter prospects to those founded within max_age_years.

    Prospects with no founded_year are retained (cannot be disqualified).

    Args:
        prospects: List of enriched Prospect objects.
        max_age_years: Maximum company age in years.
        reference_year: The year to measure from. Defaults to current year.

    Returns:
        Filtered list of Prospect objects.
    """
    ref_year = reference_year or date.today().year
    cutoff_year = ref_year - max_age_years

    return [
        p for p in prospects if p.founded_year is None or p.founded_year >= cutoff_year
    ]

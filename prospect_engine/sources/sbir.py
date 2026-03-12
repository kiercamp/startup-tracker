"""SBIR/STTR award lookup from sbir.gov.

Fetches SBIR/STTR awards from the public sbir.gov API.
State and phase filtering is client-side only.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional

import httpx

from prospect_engine.config import (
    TARGET_STATES,
    LOOKBACK_YEARS,
    SBIR_PAGE_SIZE,
    SBIR_REQUEST_DELAY,
    MIN_AWARD_AMOUNT,
)
from prospect_engine.models.prospect import SbirAward, Prospect
from prospect_engine.utils.http import get_with_retry

BASE_URL = "https://api.www.sbir.gov/public/api/awards"
logger = logging.getLogger(__name__)

# Agencies relevant to A&D
TARGET_AGENCIES: List[str] = ["DOD", "NASA"]


def fetch(
    agencies: Optional[List[str]] = None,
    states: Optional[List[str]] = None,
    lookback_years: Optional[int] = None,
    min_amount: Optional[float] = None,
) -> List[Prospect]:
    """Fetch SBIR/STTR awards from sbir.gov for target agencies and territory.

    State and amount filtering is client-side only (not supported by the API).

    Args:
        agencies: Agencies to query. Defaults to TARGET_AGENCIES.
        states: States to filter client-side. Defaults to TARGET_STATES.
        lookback_years: Years back to include. Defaults to LOOKBACK_YEARS.
        min_amount: Minimum award amount in USD. Defaults to MIN_AWARD_AMOUNT.

    Returns:
        List of Prospect objects with sbir_awards populated.
    """
    agencies = agencies or TARGET_AGENCIES
    states = states or TARGET_STATES
    years_back = lookback_years or min(LOOKBACK_YEARS, 3)
    floor = min_amount if min_amount is not None else MIN_AWARD_AMOUNT

    current_year = date.today().year
    start_year = current_year - years_back

    all_awards: List[SbirAward] = []
    fetch_errors: List[str] = []
    for agency in agencies:
        try:
            raw_awards = _fetch_agency_awards(agency, start_year, current_year)
            for raw in raw_awards:
                award = _parse_award(raw)
                if award is not None:
                    all_awards.append(award)
        except Exception as exc:
            logger.exception("SBIR fetch failed for agency=%s", agency)
            fetch_errors.append("{}: {}".format(agency, str(exc)[:100]))

    # If we got zero awards and had errors, surface the failure
    if not all_awards and fetch_errors:
        raise RuntimeError(
            "All SBIR requests failed: {}".format("; ".join(fetch_errors))
        )

    filtered = _filter_by_territory(all_awards, states)
    filtered = _filter_by_amount(filtered, floor)
    logger.info(
        "SBIR: fetched %d awards, %d after territory + amount filter",
        len(all_awards),
        len(filtered),
    )
    return _group_by_firm(filtered)


def _fetch_agency_awards(
    agency: str,
    start_year: int,
    end_year: int,
) -> List[Dict[str, Any]]:
    """Paginate through all SBIR awards for a single agency across years.

    Args:
        agency: Agency abbreviation (e.g. "DOD", "NASA").
        start_year: Earliest award year to include.
        end_year: Latest award year to include.

    Returns:
        List of raw award dicts.
    """
    all_results: List[Dict[str, Any]] = []
    year_errors: List[str] = []
    ip_banned = False

    years = list(range(start_year, end_year + 1))
    for year_idx, year in enumerate(years):
        if ip_banned:
            year_errors.append("{}/{}".format(agency, year))
            continue

        offset = 0
        while True:
            params = {
                "agency": agency,
                "year": str(year),
                "rows": SBIR_PAGE_SIZE,
                "start": offset,
            }

            try:
                # Use max_retries=1 to avoid burning rate limit budget on retries.
                # SBIR allows only 10 requests per 10 minutes.
                response = get_with_retry(
                    BASE_URL, params=params, timeout=30.0, max_retries=1,
                )
                data = response.json()
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status == 403:
                    # IP banned — stop ALL requests for this agency immediately
                    logger.warning(
                        "SBIR API returned 403 Forbidden for agency=%s — IP banned, "
                        "stopping all requests for this agency",
                        agency,
                    )
                    ip_banned = True
                    year_errors.append("{}/{}".format(agency, year))
                    break
                logger.warning(
                    "SBIR API request failed for agency=%s year=%d offset=%d: %s",
                    agency,
                    year,
                    offset,
                    str(exc)[:80],
                )
                year_errors.append("{}/{}".format(agency, year))
                break
            except Exception as exc:
                logger.warning(
                    "SBIR API request failed for agency=%s year=%d offset=%d: %s",
                    agency,
                    year,
                    offset,
                    str(exc)[:80],
                )
                year_errors.append("{}/{}".format(agency, year))
                break

            # Polite delay between requests — 65s ensures <10 requests per 10 minutes
            time.sleep(SBIR_REQUEST_DELAY)

            # API may return a list directly or a dict with results
            if isinstance(data, list):
                results = data
            elif isinstance(data, dict):
                results = data.get("results", data.get("data", []))
            else:
                break

            if not results:
                break

            all_results.extend(results)
            offset += len(results)

            if len(results) < SBIR_PAGE_SIZE:
                break

        # Cool-down between years to spread requests
        if year_idx < len(years) - 1:
            time.sleep(SBIR_REQUEST_DELAY)

    # If every year failed and we got nothing, raise so outer fetch() sees it
    if not all_results and year_errors:
        raise RuntimeError(
            "SBIR {} all years failed ({})".format(agency, ", ".join(year_errors))
        )

    logger.debug("SBIR agency=%s: %d awards fetched", agency, len(all_results))
    return all_results


def _parse_award(raw: Dict[str, Any]) -> Optional[SbirAward]:
    """Parse a raw SBIR award dict into an SbirAward.

    Args:
        raw: Raw award dict from sbir.gov response.

    Returns:
        SbirAward or None if required fields are missing.
    """
    try:
        firm = raw.get("firm", "")
        if not firm:
            return None

        # Parse award date
        award_date = None
        date_str = raw.get("proposal_award_date", "")
        if date_str:
            try:
                award_date = date.fromisoformat(date_str)
            except (ValueError, TypeError):
                # Try MM/DD/YYYY format
                try:
                    parts = date_str.split("/")
                    if len(parts) == 3:
                        award_date = date(int(parts[2]), int(parts[0]), int(parts[1]))
                except (ValueError, IndexError):
                    pass

        phase = raw.get("phase", "")
        # Normalize phase naming
        phase_map = {
            "1": "Phase I",
            "I": "Phase I",
            "Phase 1": "Phase I",
            "2": "Phase II",
            "II": "Phase II",
            "Phase 2": "Phase II",
            "3": "Phase III",
            "III": "Phase III",
            "Phase 3": "Phase III",
        }
        phase = phase_map.get(phase, phase)

        award_amount = float(raw.get("award_amount", 0) or 0)
        uei = raw.get("uei", "") or ""
        contract = raw.get("contract", "") or ""
        award_id = contract or "{}-{}-{}".format(
            uei or firm[:10], raw.get("award_year", ""), phase
        )

        # Build source URL — link to sbir.gov search for this firm
        source_url = "https://www.sbir.gov/sbirsearch/detail/{}".format(
            raw.get("award_id", award_id)
        )

        return SbirAward(
            award_id=award_id,
            firm=firm,
            agency=raw.get("agency", ""),
            phase=phase,
            program=raw.get("program", "SBIR"),
            award_title=raw.get("award_title", ""),
            award_amount=award_amount,
            award_date=award_date,
            state=raw.get("state", ""),
            city=raw.get("city", ""),
            abstract=raw.get("abstract", ""),
            uei=uei,
            source_url=source_url,
        )
    except Exception:
        logger.exception("Failed to parse SBIR award")
        return None


def _filter_by_territory(
    awards: List[SbirAward],
    states: List[str],
) -> List[SbirAward]:
    """Filter SBIR awards to the target territory.

    Args:
        awards: All parsed SbirAward objects.
        states: Target state codes.

    Returns:
        Filtered list of SbirAward objects.
    """
    states_upper = {s.upper() for s in states}
    return [a for a in awards if a.state.upper() in states_upper]


def _filter_by_amount(
    awards: List[SbirAward],
    min_amount: float,
) -> List[SbirAward]:
    """Filter SBIR awards to those at or above a minimum dollar amount.

    Args:
        awards: Parsed SbirAward objects.
        min_amount: Minimum award amount in USD (inclusive).

    Returns:
        Filtered list of SbirAward objects.
    """
    return [a for a in awards if a.award_amount >= min_amount]


def _group_by_firm(awards: List[SbirAward]) -> List[Prospect]:
    """Group SbirAward objects by UEI (falling back to firm name) into Prospect objects.

    Args:
        awards: Filtered SbirAward objects.

    Returns:
        List of Prospect objects with sbir_awards lists populated.
    """
    groups: Dict[str, List[SbirAward]] = defaultdict(list)
    for award in awards:
        key = (award.uei or award.firm.strip()).upper()
        groups[key].append(award)

    prospects = []
    for _key, group_awards in groups.items():
        first = group_awards[0]
        prospects.append(
            Prospect(
                company_name=first.firm,
                uei=first.uei,
                state=first.state,
                city=first.city,
                sbir_awards=group_awards,
                data_sources=["sbir"],
            )
        )
    return prospects

"""USASpending.gov obligation history fetcher.

Fetches contract obligation history from USASpending.gov,
filtered by target states, NAICS codes, and date range.
No authentication required.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from prospect_engine.config import (
    TARGET_STATES,
    TARGET_NAICS,
    LOOKBACK_YEARS,
    USASPENDING_PAGE_SIZE,
    MIN_AWARD_AMOUNT,
    USASPENDING_AWARD_UPPER_BOUND,
    TARGET_AGENCIES_USASPENDING,
    AEROSPACE_DEFENSE_KEYWORDS,
)

# NAICS codes that are inherently A&D-relevant — awards with these codes
# are retained even when the description field is empty.
_AD_NAICS_SET = set(TARGET_NAICS)
from prospect_engine.models.prospect import ContractAward, Prospect
from prospect_engine.utils.cache import get_cache
from prospect_engine.utils.http import post_with_retry

BASE_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
logger = logging.getLogger(__name__)


def fetch(
    states: Optional[List[str]] = None,
    naics_codes: Optional[List[str]] = None,
    lookback_days: Optional[int] = None,
    min_amount: Optional[float] = None,
    agencies: Optional[List[Dict[str, str]]] = None,
) -> List[Prospect]:
    """Fetch contract obligation history from USASpending.gov.

    Uses POST with JSON body to filter by NAICS, state, and award type codes.
    Paginates through all results automatically.

    Args:
        states: State codes to filter. Defaults to TARGET_STATES.
        naics_codes: NAICS codes to filter. Defaults to TARGET_NAICS.
        lookback_days: Days back from today to include. Defaults to LOOKBACK_YEARS * 365.
        min_amount: Minimum award amount in USD. Defaults to MIN_AWARD_AMOUNT.
        agencies: USASpending agency filter dicts. Defaults to TARGET_AGENCIES_USASPENDING.

    Returns:
        List of Prospect objects with contract_awards populated.
    """
    states = states or TARGET_STATES
    naics_codes = naics_codes or TARGET_NAICS
    days = lookback_days or (LOOKBACK_YEARS * 365)
    floor = min_amount if min_amount is not None else MIN_AWARD_AMOUNT
    agencies = agencies if agencies is not None else TARGET_AGENCIES_USASPENDING

    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    all_awards: List[ContractAward] = []
    all_raw: List[Dict[str, Any]] = []
    page = 1
    fetch_errors: List[str] = []
    cache = get_cache()

    while True:
        body = _build_request_body(
            states=states,
            naics_codes=naics_codes,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            page=page,
            limit=USASPENDING_PAGE_SIZE,
            agencies=agencies,
        )

        # Check cache first
        agency_names = sorted(a.get("name", "") for a in agencies) if agencies else []
        cache_key = {"endpoint": "usa_spending", "states": sorted(states),
                     "start": start_date.isoformat(), "end": end_date.isoformat(),
                     "page": page, "agencies": agency_names}
        cached = cache.get("usa_spending", cache_key)
        if cached is not None:
            data = json.loads(cached)
        else:
            try:
                response = post_with_retry(
                    BASE_URL, json=body, timeout=60.0, endpoint="usa_spending",
                )
                data = response.json()
                cache.put("usa_spending", cache_key, json.dumps(data))
            except Exception as exc:
                logger.exception("USASpending fetch failed on page %d", page)
                fetch_errors.append(str(exc)[:120])
                break

        results = data.get("results", [])
        if not results:
            break

        for raw in results:
            award = _parse_result(raw)
            if award is not None:
                all_awards.append(award)
                all_raw.append(raw)

        page_meta = data.get("page_metadata", {})
        total = page_meta.get("total") or 0
        if total > 0:
            total_pages = (total + USASPENDING_PAGE_SIZE - 1) // USASPENDING_PAGE_SIZE
        else:
            # total not provided — keep paginating until empty results
            total_pages = 999

        # Cap pagination to avoid excessive requests.
        # 10 pages × 100 results = 1000 awards (top by amount).
        max_pages = min(total_pages, 10)
        if page >= max_pages:
            break
        page += 1

    # If we got zero awards and had errors, surface the failure
    if not all_awards and fetch_errors:
        raise RuntimeError(
            "All USASpending requests failed: {}".format(fetch_errors[0])
        )

    # NOTE: Client-side NAICS filtering removed — the spending_by_award endpoint
    # does not return NAICS Code data (always None as of March 2026).
    # Results are already scoped by award_type_codes (contracts) and state,
    # so all results are government contracts in our target territory.

    amount_filtered = _filter_by_amount(all_awards, floor)
    filtered = _filter_by_keywords(amount_filtered)
    logger.info(
        "USASpending: %d awards, %d after amount, %d after keywords, %d pages",
        len(all_awards),
        len(amount_filtered),
        len(filtered),
        page,
    )
    return _group_by_recipient(filtered, all_raw)


def _build_request_body(
    states: List[str],
    naics_codes: List[str],
    start_date: str,
    end_date: str,
    page: int,
    limit: int,
    agencies: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """Build the POST body for the USASpending spending_by_award endpoint.

    Args:
        states: List of state codes for recipient_locations filter.
        naics_codes: List of NAICS codes for naics_codes filter.
        start_date: ISO date string "YYYY-MM-DD".
        end_date: ISO date string "YYYY-MM-DD".
        page: Page number (1-indexed).
        limit: Results per page.
        agencies: Agency filter dicts for the USASpending "agencies" filter.

    Returns:
        Dict ready to serialize as JSON request body.
    """
    # NOTE: naics_codes filter omitted — USASpending API returns HTTP 500
    # when naics_codes is included (server-side bug as of March 2026).
    # NOTE: "internal_id" field also omitted — causes HTTP 500 as of March 2026.
    # Using "generated_internal_id" instead for award URLs.
    filters: Dict[str, Any] = {
        "award_type_codes": ["A", "B", "C", "D"],
        "recipient_locations": [{"country": "USA", "state": s} for s in states],
        "time_period": [{"start_date": start_date, "end_date": end_date}],
    }

    # Cap award amounts to surface startup-sized contracts, not mega-prime deals.
    # If this causes a 500 (like naics_codes), we fall back to client-side filtering.
    if USASPENDING_AWARD_UPPER_BOUND > 0:
        filters["award_amounts"] = [
            {"lower_bound": 0, "upper_bound": USASPENDING_AWARD_UPPER_BOUND}
        ]

    # Filter to target awarding agencies (DoD, NASA)
    if agencies:
        filters["agencies"] = agencies

    return {
        "filters": filters,
        "fields": [
            "Award ID",
            "Recipient Name",
            "Start Date",
            "End Date",
            "Award Amount",
            "Awarding Agency",
            "Awarding Sub Agency",
            "Description",
            "NAICS Code",
            "NAICS Description",
            "generated_internal_id",
        ],
        "page": page,
        "limit": limit,
        "sort": "Start Date",
        "order": "desc",
        "subawards": False,
    }


def _parse_result(raw: Dict[str, Any]) -> Optional[ContractAward]:
    """Parse a single USASpending result dict into a ContractAward.

    Args:
        raw: Single result dict from the USASpending response results array.

    Returns:
        ContractAward or None if required fields are absent.
    """
    try:
        recipient_name = raw.get("Recipient Name", "")
        if not recipient_name:
            return None

        award_id = raw.get("Award ID", "") or ""
        obligation = float(raw.get("Award Amount", 0) or 0)

        signed_date = None
        date_str = raw.get("Start Date")
        if date_str:
            try:
                signed_date = date.fromisoformat(date_str)
            except (ValueError, TypeError):
                pass

        # Build source URL from generated internal award ID
        # (internal_id field causes HTTP 500, so only generated_internal_id is fetched)
        internal_id = raw.get("generated_internal_id", "")
        source_url = ""
        if internal_id:
            source_url = "https://www.usaspending.gov/award/{}".format(internal_id)
        elif award_id:
            source_url = "https://www.usaspending.gov/search/?keyword={}".format(
                award_id
            )

        return ContractAward(
            award_id=award_id,
            source="usa_spending",
            recipient_name=recipient_name,
            awarding_agency=raw.get("Awarding Agency", ""),
            naics_code=str(raw.get("NAICS Code") or ""),
            signed_date=signed_date,
            obligation_amount=obligation,
            description=raw.get("Description", "") or raw.get("NAICS Description", ""),
            source_url=source_url,
        )
    except Exception:
        logger.exception("Failed to parse USASpending result")
        return None


def _filter_by_amount(
    awards: List[ContractAward],
    min_amount: float,
) -> List[ContractAward]:
    """Filter contract awards to those at or above a minimum dollar amount.

    Args:
        awards: Parsed ContractAward objects.
        min_amount: Minimum obligation amount in USD (inclusive).

    Returns:
        Filtered list of ContractAward objects.
    """
    return [a for a in awards if a.obligation_amount >= min_amount]


def _filter_by_keywords(
    awards: List[ContractAward],
    keywords: Optional[List[str]] = None,
) -> List[ContractAward]:
    """Filter awards to those with aerospace/defense-relevant descriptions.

    Awards with empty or missing descriptions are retained (benefit of the
    doubt — the agency filter already scopes to DoD/NASA).

    Args:
        awards: Parsed ContractAward objects.
        keywords: Lowercase keyword strings for substring matching.
            Defaults to AEROSPACE_DEFENSE_KEYWORDS from config.

    Returns:
        Filtered list of ContractAward objects.
    """
    if keywords is None:
        keywords = AEROSPACE_DEFENSE_KEYWORDS
    if not keywords:
        return awards

    def _matches(award: ContractAward) -> bool:
        text = (award.description or "").lower()
        if not text:
            # No description — retain only if NAICS code is A&D-relevant
            # (or if NAICS code is also missing/empty, give benefit of the doubt)
            naics = (award.naics_code or "").strip()
            return (not naics) or (naics in _AD_NAICS_SET)
        return any(kw in text for kw in keywords)

    return [a for a in awards if _matches(a)]


def _group_by_recipient(
    awards: List[ContractAward],
    raw_results: List[Dict[str, Any]],
) -> List[Prospect]:
    """Group ContractAward objects by recipient name into Prospect objects.

    Args:
        awards: Parsed ContractAward objects.
        raw_results: Original raw dicts (parallel to awards list).

    Returns:
        List of Prospect objects.
    """
    groups: Dict[str, List[ContractAward]] = defaultdict(list)
    for award in awards:
        key = award.recipient_name.strip().upper()
        groups[key].append(award)

    prospects = []
    for _key, group_awards in groups.items():
        first = group_awards[0]
        prospects.append(
            Prospect(
                company_name=first.recipient_name,
                contract_awards=group_awards,
                data_sources=["usa_spending"],
            )
        )
    return prospects

"""USASpending.gov obligation history fetcher.

Fetches contract obligation history from USASpending.gov,
filtered by target states, NAICS codes, and date range.
No authentication required.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from prospect_engine.config import (
    TARGET_STATES,
    TARGET_NAICS,
    LOOKBACK_YEARS,
    USASPENDING_PAGE_SIZE,
)
from prospect_engine.models.prospect import ContractAward, Prospect
from prospect_engine.utils.http import post_with_retry

BASE_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
logger = logging.getLogger(__name__)


def fetch(
    states: Optional[List[str]] = None,
    naics_codes: Optional[List[str]] = None,
    lookback_days: Optional[int] = None,
) -> List[Prospect]:
    """Fetch contract obligation history from USASpending.gov.

    Uses POST with JSON body to filter by NAICS, state, and award type codes.
    Paginates through all results automatically.

    Args:
        states: State codes to filter. Defaults to TARGET_STATES.
        naics_codes: NAICS codes to filter. Defaults to TARGET_NAICS.
        lookback_days: Days back from today to include. Defaults to LOOKBACK_YEARS * 365.

    Returns:
        List of Prospect objects with contract_awards populated.
    """
    states = states or TARGET_STATES
    naics_codes = naics_codes or TARGET_NAICS
    days = lookback_days or (LOOKBACK_YEARS * 365)

    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    all_awards: List[ContractAward] = []
    all_raw: List[Dict[str, Any]] = []
    page = 1

    while True:
        body = _build_request_body(
            states=states,
            naics_codes=naics_codes,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            page=page,
            limit=USASPENDING_PAGE_SIZE,
        )

        try:
            response = post_with_retry(BASE_URL, json=body, timeout=60.0)
            data = response.json()
        except Exception:
            logger.exception("USASpending fetch failed on page %d", page)
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
        total_pages = (
            page_meta.get("total", 0) + USASPENDING_PAGE_SIZE - 1
        ) // USASPENDING_PAGE_SIZE
        if page >= total_pages:
            break
        page += 1

    logger.info("USASpending: fetched %d awards across %d pages", len(all_awards), page)
    return _group_by_recipient(all_awards, all_raw)


def _build_request_body(
    states: List[str],
    naics_codes: List[str],
    start_date: str,
    end_date: str,
    page: int,
    limit: int,
) -> Dict[str, Any]:
    """Build the POST body for the USASpending spending_by_award endpoint.

    Args:
        states: List of state codes for recipient_locations filter.
        naics_codes: List of NAICS codes for naics_codes.require filter.
        start_date: ISO date string "YYYY-MM-DD".
        end_date: ISO date string "YYYY-MM-DD".
        page: Page number (1-indexed).
        limit: Results per page.

    Returns:
        Dict ready to serialize as JSON request body.
    """
    return {
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "naics_codes": {"require": naics_codes},
            "recipient_locations": [{"country": "USA", "state": s} for s in states],
            "time_period": [{"start_date": start_date, "end_date": end_date}],
        },
        "fields": [
            "Award ID",
            "Recipient Name",
            "Start Date",
            "End Date",
            "Award Amount",
            "Awarding Agency",
            "Awarding Sub Agency",
            "NAICS Code",
            "NAICS Description",
            "internal_id",
            "generated_internal_id",
        ],
        "page": page,
        "limit": limit,
        "sort": "Award Amount",
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

        # Build source URL from internal award ID
        internal_id = raw.get("internal_id", "") or raw.get("generated_internal_id", "")
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
            naics_code=str(raw.get("NAICS Code", "")),
            signed_date=signed_date,
            obligation_amount=obligation,
            description=raw.get("NAICS Description", ""),
            source_url=source_url,
        )
    except Exception:
        logger.exception("Failed to parse USASpending result")
        return None


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

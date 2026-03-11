"""SAM.gov contract awards fetcher.

Fetches DoD/NASA contract awards from the SAM.gov Contract Awards API,
filtered by target states and NAICS codes.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from prospect_engine.config import (
    SAM_GOV_API_KEY,
    TARGET_STATES,
    TARGET_NAICS,
    LOOKBACK_YEARS,
    SAM_GOV_PAGE_SIZE,
    MIN_AWARD_AMOUNT,
)
from prospect_engine.models.prospect import ContractAward, Prospect
from prospect_engine.utils.http import get_with_retry

BASE_URL = "https://api.sam.gov/contract-awards/v1/search"
logger = logging.getLogger(__name__)


def fetch(
    states: Optional[List[str]] = None,
    naics_codes: Optional[List[str]] = None,
    lookback_days: Optional[int] = None,
    min_amount: Optional[float] = None,
) -> List[Prospect]:
    """Fetch DoD/NASA contract awards from SAM.gov for target territory and NAICS codes.

    Loops over each state to work around the single-state filter constraint.
    Paginates until all results are retrieved.

    Args:
        states: State codes to query. Defaults to TARGET_STATES from config.
        naics_codes: NAICS codes to filter. Defaults to TARGET_NAICS from config.
        lookback_days: Number of days back to search. Defaults to LOOKBACK_YEARS * 365.
        min_amount: Minimum obligation amount in USD. Defaults to MIN_AWARD_AMOUNT.

    Returns:
        List of Prospect objects, one per unique recipient, with contract_awards populated.

    Raises:
        ValueError: If SAM_GOV_API_KEY is not set.
    """
    if not SAM_GOV_API_KEY:
        raise ValueError(
            "SAM_GOV_API_KEY is not set. Register at SAM.gov and add your "
            "API key to .env"
        )

    states = states or TARGET_STATES
    naics_codes = naics_codes or TARGET_NAICS
    days = lookback_days or (LOOKBACK_YEARS * 365)
    floor = min_amount if min_amount is not None else MIN_AWARD_AMOUNT

    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    date_range = "[{},{}]".format(
        start_date.strftime("%m/%d/%Y"),
        end_date.strftime("%m/%d/%Y"),
    )

    naics_tilde = "~".join(naics_codes)

    all_awards: List[ContractAward] = []
    for state in states:
        try:
            raw_awards = _fetch_for_state(
                state, naics_tilde, date_range, SAM_GOV_API_KEY
            )
            for raw in raw_awards:
                award = _parse_award(raw)
                if award is not None:
                    all_awards.append(award)
        except Exception:
            logger.exception("SAM.gov fetch failed for state=%s", state)

    filtered = _filter_by_amount(all_awards, floor)
    logger.info(
        "SAM.gov: fetched %d awards, %d after amount filter, across %d states",
        len(all_awards),
        len(filtered),
        len(states),
    )
    return _group_by_recipient(filtered)


def _fetch_for_state(
    state: str,
    naics_tilde: str,
    date_range: str,
    api_key: str,
) -> List[Dict[str, Any]]:
    """Paginate through SAM.gov results for a single state.

    Args:
        state: Two-letter state code.
        naics_tilde: Tilde-separated NAICS code string.
        date_range: Date range string in SAM.gov format.
        api_key: SAM.gov API key.

    Returns:
        List of raw award dicts from the API response.
    """
    all_results: List[Dict[str, Any]] = []
    offset = 0

    while True:
        params = {
            "api_key": api_key,
            "awardeeStateCode": state,
            "naicsCode": naics_tilde,
            "dateSigned": date_range,
            "limit": SAM_GOV_PAGE_SIZE,
            "offset": offset,
        }
        response = get_with_retry(BASE_URL, params=params, timeout=30.0)
        data = response.json()

        results = data.get("data", [])
        if not results:
            break

        all_results.extend(results)
        offset += len(results)

        if len(results) < SAM_GOV_PAGE_SIZE:
            break

    logger.debug("SAM.gov state=%s: %d awards fetched", state, len(all_results))
    return all_results


def _parse_award(raw: Dict[str, Any]) -> Optional[ContractAward]:
    """Parse a single SAM.gov award dict into a ContractAward.

    Args:
        raw: Raw award dict from SAM.gov response.

    Returns:
        ContractAward or None if required fields are missing.
    """
    try:
        award_details = raw.get("awardDetails", {})
        awardee_data = award_details.get("awardeeData", {})
        dollars = award_details.get("dollars", {})
        dates = award_details.get("dates", {})
        contract_data = award_details.get("contractData", {})

        recipient_name = awardee_data.get("recipientName", "")
        if not recipient_name:
            return None

        # Parse signed date
        signed_date_str = dates.get("signedDate", "")
        signed_date = None
        if signed_date_str:
            try:
                parts = signed_date_str.split("/")
                if len(parts) == 3:
                    signed_date = date(int(parts[2]), int(parts[0]), int(parts[1]))
            except (ValueError, IndexError):
                pass

        obligation = float(dollars.get("actionObligation", 0) or 0)
        naics_code = str(contract_data.get("naicsCode", ""))
        award_id = raw.get("contractId", {}).get("piid", "") or str(id(raw))

        piid = raw.get("contractId", {}).get("piid", "")
        source_url = ""
        if piid:
            source_url = "https://sam.gov/opp/{}/view".format(piid)

        return ContractAward(
            award_id=award_id,
            source="sam_gov",
            recipient_name=recipient_name,
            awarding_agency=award_details.get("fundingAgency", {}).get("name", ""),
            naics_code=naics_code,
            signed_date=signed_date,
            obligation_amount=obligation,
            description=contract_data.get("descriptionOfRequirement", ""),
            piid=piid,
            source_url=source_url,
        )
    except Exception:
        logger.exception("Failed to parse SAM.gov award")
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


def _group_by_recipient(awards: List[ContractAward]) -> List[Prospect]:
    """Group ContractAward objects by recipient name into Prospect objects.

    Args:
        awards: List of parsed ContractAward objects.

    Returns:
        List of Prospect objects with contract_awards lists populated.
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
                data_sources=["sam_gov"],
            )
        )
    return prospects

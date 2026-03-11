"""SAM.gov contract awards fetcher.

Fetches DoD/NASA contract awards from the SAM.gov Contract Awards API,
filtered by target states and NAICS codes.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from prospect_engine.config import (
    _get_secret,
    TARGET_STATES,
    TARGET_NAICS,
    LOOKBACK_YEARS,
    SAM_GOV_PAGE_SIZE,
    SAM_GOV_REQUEST_DELAY,
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
    # Read API key at call time — not at import time — so Streamlit Cloud
    # secrets are available even if they weren't during module init.
    api_key = _get_secret("SAM_GOV_API_KEY")
    if not api_key:
        raise ValueError(
            "SAM_GOV_API_KEY is not set. Register at SAM.gov and add your "
            "API key to .env or Streamlit Cloud secrets"
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
            raw_awards = _fetch_for_state(state, naics_tilde, date_range, api_key)
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

        # Polite delay between requests to avoid 429s
        time.sleep(SAM_GOV_REQUEST_DELAY)

        results = data.get("awardSummary", [])
        if not results:
            break

        all_results.extend(results)
        offset += len(results)

        total_records = int(data.get("totalRecords", 0) or 0)
        if offset >= total_records:
            break

    logger.debug("SAM.gov state=%s: %d awards fetched", state, len(all_results))
    return all_results


def _parse_award(raw: Dict[str, Any]) -> Optional[ContractAward]:
    """Parse a single SAM.gov award dict into a ContractAward.

    The SAM.gov Contract Awards v1 API returns deeply nested objects.
    Key paths:
        Recipient name:  awardDetails.awardeeData.awardeeHeader.awardeeName
        UEI:             awardDetails.awardeeData.awardeeUEIInformation.uniqueEntityId
        Date signed:     awardDetails.dates.dateSigned  (ISO: "2024-03-15T00:00:00Z")
        Obligation:      awardDetails.dollars.actionObligation  (string)
        NAICS:           coreData.productOrServiceInformation.principalNaics[0].code
        Agency:          coreData.federalOrganization.contractingInformation
                             .contractingSubtier.name
        Description:     awardDetails.productOrServiceInformation
                             .descriptionOfContractRequirement
        PIID:            contractId.piid

    Args:
        raw: Raw award dict from SAM.gov awardSummary response.

    Returns:
        ContractAward or None if required fields are missing.
    """
    try:
        award_details = raw.get("awardDetails", {})
        core_data = raw.get("coreData", {})
        awardee_data = award_details.get("awardeeData", {})
        dollars = award_details.get("dollars", {})
        dates = award_details.get("dates", {})

        # Recipient name — nested under awardeeHeader
        awardee_header = awardee_data.get("awardeeHeader", {})
        recipient_name = awardee_header.get("awardeeName", "")
        if not recipient_name:
            return None

        # Parse signed date — ISO format "2024-03-15T00:00:00Z"
        signed_date_str = dates.get("dateSigned", "")
        signed_date = None
        if signed_date_str:
            try:
                # Strip time portion and parse ISO date
                date_part = signed_date_str.split("T")[0]
                signed_date = date.fromisoformat(date_part)
            except (ValueError, TypeError):
                pass

        # Obligation amount — API returns as string
        obligation = float(dollars.get("actionObligation", 0) or 0)

        # NAICS code — nested under coreData.productOrServiceInformation
        naics_code = ""
        product_info = core_data.get("productOrServiceInformation", {})
        principal_naics = product_info.get("principalNaics", [])
        if principal_naics and isinstance(principal_naics, list):
            naics_code = str(principal_naics[0].get("code", ""))

        # Award ID from PIID
        contract_id = raw.get("contractId", {})
        piid = contract_id.get("piid", "")
        award_id = piid or str(id(raw))

        # Awarding agency — contracting subtier
        fed_org = core_data.get("federalOrganization", {})
        contracting_info = fed_org.get("contractingInformation", {})
        awarding_agency = contracting_info.get("contractingSubtier", {}).get("name", "")
        # Fall back to department level if subtier is empty
        if not awarding_agency:
            awarding_agency = contracting_info.get("contractingDepartment", {}).get(
                "name", ""
            )

        # Description
        award_product_info = award_details.get("productOrServiceInformation", {})
        description = award_product_info.get("descriptionOfContractRequirement", "")

        # Source URL
        source_url = ""
        if piid:
            source_url = "https://sam.gov/opp/{}/view".format(piid)

        return ContractAward(
            award_id=award_id,
            source="sam_gov",
            recipient_name=recipient_name,
            awarding_agency=awarding_agency,
            naics_code=naics_code,
            signed_date=signed_date,
            obligation_amount=obligation,
            description=description,
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

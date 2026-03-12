"""SAM.gov Entity API enrichment — look up founding dates, UEI, and state.

Queries the SAM.gov Entity Information API v3 to populate:
- founded_year (from entityStartDate)
- uei (from ueiSAM)
- state (from physicalAddress.stateOrProvinceCode)
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from prospect_engine.config import (
    SAM_GOV_API_KEY,
    SAM_ENTITY_DAILY_BUDGET,
)
from prospect_engine.models.prospect import Prospect
from prospect_engine.utils.cache import get_cache
from prospect_engine.utils.http import get_with_retry

SAM_ENTITY_URL = "https://api.sam.gov/entity-information/v3/entities"
logger = logging.getLogger(__name__)


def enrich_with_entity_data(
    prospects: List[Prospect],
    api_key: Optional[str] = None,
    daily_budget: Optional[int] = None,
) -> List[Prospect]:
    """Enrich prospects with SAM.gov Entity API data.

    For each prospect without a founded_year, queries the Entity API to
    look up entityStartDate, UEI, and state. Skips prospects that already
    have founded_year populated.

    Args:
        prospects: List of Prospect objects to enrich (modified in-place).
        api_key: SAM.gov API key. Defaults to SAM_GOV_API_KEY from config.
        daily_budget: Max API requests to make. Defaults to SAM_ENTITY_DAILY_BUDGET.

    Returns:
        The same list of Prospect objects (enriched in-place).
    """
    key = api_key or SAM_GOV_API_KEY
    budget = daily_budget if daily_budget is not None else SAM_ENTITY_DAILY_BUDGET

    if not key:
        logger.warning("No SAM.gov API key — skipping entity enrichment")
        return prospects

    enriched_count = 0
    api_calls = 0

    for prospect in prospects:
        if prospect.founded_year is not None:
            continue  # Already enriched

        # Rate limiter enforces daily cap; manual budget is a soft limit
        if api_calls >= budget:
            logger.warning(
                "Entity enrichment budget exhausted (%d/%d) — %d prospects not enriched",
                api_calls,
                budget,
                sum(1 for p in prospects if p.founded_year is None),
            )
            break

        # Look up by UEI (exact) or company name (fuzzy)
        entity = None
        if prospect.uei:
            entity = _lookup_entity_by_uei(prospect.uei, key)
        else:
            entity = _lookup_entity_by_name(prospect.company_name, key)
        api_calls += 1

        if entity is not None:
            uei, state, founded_year = _extract_entity_fields(entity)
            if founded_year is not None:
                prospect.founded_year = founded_year
                enriched_count += 1
            if uei and not prospect.uei:
                prospect.uei = uei
            if state and not prospect.state:
                prospect.state = state

        # Rate limiter handles pacing — no manual sleep needed

    logger.info(
        "Entity enrichment: %d/%d prospects enriched with founding year (%d API calls)",
        enriched_count,
        len(prospects),
        api_calls,
    )
    return prospects


def _lookup_entity_by_uei(
    uei: str,
    api_key: str,
) -> Optional[Dict[str, Any]]:
    """Query SAM.gov Entity API by UEI for an exact match.

    Args:
        uei: The 12-character Unique Entity Identifier.
        api_key: SAM.gov API key.

    Returns:
        Entity dict from the response, or None if not found.
    """
    cache = get_cache()
    cache_key = {"endpoint": "sam_entity", "uei": uei}
    cached = cache.get("sam_entity", cache_key)
    if cached is not None:
        data = json.loads(cached)
        entities = data.get("entityData", [])
        if entities:
            return entities[0]
        return None

    try:
        response = get_with_retry(
            SAM_ENTITY_URL,
            params={
                "api_key": api_key,
                "ueiSAM": uei,
                "registrationStatus": "A",
                "samRegistered": "Yes",
            },
            timeout=30.0,
            max_retries=1,
            endpoint="sam_entity",
        )
        data = response.json()
        cache.put("sam_entity", cache_key, json.dumps(data))
        entities = data.get("entityData", [])
        if entities:
            return entities[0]
    except Exception:
        logger.debug("Entity lookup by UEI=%s failed", uei, exc_info=True)
    return None


def _lookup_entity_by_name(
    company_name: str,
    api_key: str,
) -> Optional[Dict[str, Any]]:
    """Query SAM.gov Entity API by legal business name.

    Returns the best matching entity, or None if no good match found.

    Args:
        company_name: The company name to search for.
        api_key: SAM.gov API key.

    Returns:
        Best-matching entity dict, or None.
    """
    cache = get_cache()
    cache_key = {"endpoint": "sam_entity", "name": company_name}
    cached = cache.get("sam_entity", cache_key)
    if cached is not None:
        data = json.loads(cached)
        entities = data.get("entityData", [])
        if not entities:
            return None
        return _select_best_match(entities, company_name)

    try:
        response = get_with_retry(
            SAM_ENTITY_URL,
            params={
                "api_key": api_key,
                "legalBusinessName": company_name,
                "registrationStatus": "A",
                "samRegistered": "Yes",
            },
            timeout=30.0,
            max_retries=1,
            endpoint="sam_entity",
        )
        data = response.json()
        cache.put("sam_entity", cache_key, json.dumps(data))
        entities = data.get("entityData", [])
        if not entities:
            return None
        return _select_best_match(entities, company_name)
    except Exception:
        logger.debug(
            "Entity lookup by name=%s failed", company_name[:40], exc_info=True,
        )
    return None


def _select_best_match(
    entities: List[Dict[str, Any]],
    target_name: str,
) -> Optional[Dict[str, Any]]:
    """Select the entity whose name best matches the target.

    Uses normalize_company_name() for comparison. Returns None if no
    entity has a normalized name matching the target.

    Args:
        entities: List of entity dicts from the API response.
        target_name: The company name to match against.

    Returns:
        Best-matching entity dict, or None if no close match.
    """
    from prospect_engine.enrichment.company_profile import normalize_company_name

    target_norm = normalize_company_name(target_name)
    if not target_norm:
        return None

    for entity in entities:
        reg = entity.get("entityRegistration", {})
        entity_name = reg.get("legalBusinessName", "")
        entity_norm = normalize_company_name(entity_name)
        # Accept if normalized names match exactly
        if entity_norm == target_norm:
            return entity

    # Fall back: check if target is a substring of entity name or vice versa
    for entity in entities:
        reg = entity.get("entityRegistration", {})
        entity_name = reg.get("legalBusinessName", "")
        entity_norm = normalize_company_name(entity_name)
        if target_norm in entity_norm or entity_norm in target_norm:
            return entity

    return None


def _extract_entity_fields(
    entity: Dict[str, Any],
) -> Tuple[str, str, Optional[int]]:
    """Extract UEI, state, and founding year from an entity response dict.

    Args:
        entity: A single entity dict from the SAM.gov response.

    Returns:
        Tuple of (uei, state, founded_year). Any value may be empty/None.
    """
    reg = entity.get("entityRegistration", {})
    core = entity.get("coreData", {})
    info = core.get("entityInformation", {})
    addr = core.get("physicalAddress", {})

    uei = reg.get("ueiSAM", "") or ""
    state = addr.get("stateOrProvinceCode", "") or ""

    founded_year = None
    start_date = info.get("entityStartDate", "")
    if start_date:
        founded_year = _parse_year(start_date)

    return uei, state, founded_year


def _parse_year(date_str: str) -> Optional[int]:
    """Parse a year from various date formats.

    Handles "YYYY-MM-DD", "MM/DD/YYYY", and "YYYY" formats.

    Args:
        date_str: Date string from the API.

    Returns:
        Integer year, or None if parsing fails.
    """
    if not date_str:
        return None
    try:
        # Try ISO format first (YYYY-MM-DD)
        if "-" in date_str and len(date_str) >= 4:
            return int(date_str[:4])
        # Try MM/DD/YYYY
        if "/" in date_str:
            parts = date_str.split("/")
            if len(parts) == 3:
                return int(parts[2])
        # Try bare year
        if date_str.isdigit() and len(date_str) == 4:
            return int(date_str)
    except (ValueError, IndexError):
        pass
    return None

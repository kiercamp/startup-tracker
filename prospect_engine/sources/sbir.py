"""SBIR/STTR award lookup from sbir.gov.

Fetches SBIR/STTR awards from the sbir.gov bulk CSV export (preferred)
or the public sbir.gov paginated API (fallback).

The bulk CSV at https://data.www.sbir.gov/awarddatapublic/award_data.csv
contains all historical SBIR/STTR awards and has no rate limits, unlike
the API which enforces a strict 10-requests-per-10-minutes limit and
frequently issues 403 IP bans.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import tempfile
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Set

import httpx

from prospect_engine.config import (
    TARGET_STATES,
    LOOKBACK_YEARS,
    SBIR_PAGE_SIZE,
    MIN_AWARD_AMOUNT,
)
from prospect_engine.models.prospect import SbirAward, Prospect
from prospect_engine.utils.cache import get_cache
from prospect_engine.utils.http import get_with_retry

BASE_URL = "https://api.www.sbir.gov/public/api/awards"
BULK_CSV_URL = "https://data.www.sbir.gov/awarddatapublic/award_data.csv"
logger = logging.getLogger(__name__)

# Agencies relevant to A&D
TARGET_AGENCIES: List[str] = ["DOD", "NASA"]

# Mapping from full agency names (used in bulk CSV) to abbreviated codes
# (used by the API and in SbirAward.agency).
_AGENCY_FULL_TO_CODE = {
    "department of defense": "DOD",
    "national aeronautics and space administration": "NASA",
    "department of energy": "DOE",
    "department of homeland security": "DHS",
    "department of health and human services": "HHS",
    "national science foundation": "NSF",
    "department of commerce": "DOC",
    "department of transportation": "DOT",
    "department of agriculture": "USDA",
    "department of education": "ED",
    "department of interior": "DOI",
    "environmental protection agency": "EPA",
    "nuclear regulatory commission": "NRC",
}

# Phase normalization map (shared by API and CSV parsers)
_PHASE_MAP = {
    "1": "Phase I",
    "I": "Phase I",
    "Phase 1": "Phase I",
    "Phase I": "Phase I",
    "2": "Phase II",
    "II": "Phase II",
    "Phase 2": "Phase II",
    "Phase II": "Phase II",
    "3": "Phase III",
    "III": "Phase III",
    "Phase 3": "Phase III",
    "Phase III": "Phase III",
}

# CSV column name variants → internal key.
# The bulk CSV may use different header names across releases.
_CSV_COLUMNS = {
    "firm": ["Company", "Company Name", "Firm"],
    "award_title": ["Award Title", "Title"],
    "agency": ["Agency"],
    "phase": ["Phase"],
    "program": ["Program"],
    "award_amount": ["Award Amount", "Amount"],
    "award_year": ["Award Year", "Year"],
    "award_start_date": ["Proposal Award Date", "Award Start Date", "Award Date", "Date"],
    "city": ["City", "Company City"],
    "state": ["State", "Company State"],
    "abstract": ["Abstract"],
    "uei": ["UEI", "SAM UEI"],
    "contract": ["Contract", "Contract Number", "Award Number"],
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch(
    agencies: Optional[List[str]] = None,
    states: Optional[List[str]] = None,
    lookback_years: Optional[int] = None,
    min_amount: Optional[float] = None,
) -> List[Prospect]:
    """Fetch SBIR/STTR awards, preferring the bulk CSV over the API.

    Tries the bulk CSV download first (no rate limits, complete data).
    Falls back to the paginated API only if the CSV download fails.

    Args:
        agencies: Agencies to query. Defaults to TARGET_AGENCIES.
        states: States to filter client-side. Defaults to TARGET_STATES.
        lookback_years: Years back to include. Defaults to LOOKBACK_YEARS.
        min_amount: Minimum award amount in USD. Defaults to MIN_AWARD_AMOUNT.

    Returns:
        List of Prospect objects with sbir_awards populated.
    """
    try:
        return fetch_bulk(
            agencies=agencies,
            states=states,
            lookback_years=lookback_years,
            min_amount=min_amount,
        )
    except Exception as exc:
        logger.warning(
            "SBIR bulk CSV fetch failed (%s), falling back to API",
            str(exc)[:120],
        )
        return _fetch_via_api(
            agencies=agencies,
            states=states,
            lookback_years=lookback_years,
            min_amount=min_amount,
        )


# ---------------------------------------------------------------------------
# Bulk CSV fetcher (preferred)
# ---------------------------------------------------------------------------


def fetch_bulk(
    agencies: Optional[List[str]] = None,
    states: Optional[List[str]] = None,
    lookback_years: Optional[int] = None,
    min_amount: Optional[float] = None,
) -> List[Prospect]:
    """Fetch SBIR/STTR awards from the sbir.gov bulk CSV export.

    Downloads the complete SBIR award CSV (~150k+ rows), stream-parses
    it line by line, and filters to target agencies, states, and year
    range.  Results are cached in SQLite with a 7-day TTL so the large
    download only happens once per week.

    Args:
        agencies: Agency abbreviations (e.g. ["DOD", "NASA"]).
        states: State codes (e.g. ["AZ", "TX"]).
        lookback_years: Years back to include. Defaults to LOOKBACK_YEARS.
        min_amount: Minimum award amount. Defaults to MIN_AWARD_AMOUNT.

    Returns:
        List of Prospect objects with sbir_awards populated.
    """
    agencies = agencies or TARGET_AGENCIES
    states = states or TARGET_STATES
    years_back = lookback_years or LOOKBACK_YEARS
    floor = min_amount if min_amount is not None else MIN_AWARD_AMOUNT

    current_year = date.today().year
    start_year = current_year - years_back

    cache = get_cache()
    cache_key = {
        "endpoint": "sbir_bulk",
        "agencies": sorted(a.upper() for a in agencies),
        "states": sorted(s.upper() for s in states),
        "start_year": start_year,
    }

    cached = cache.get("sbir", cache_key)
    if cached is not None:
        awards_data = json.loads(cached)
        awards = [_dict_to_sbir_award(d) for d in awards_data]
        logger.info("SBIR bulk: loaded %d awards from cache", len(awards))
    else:
        # Download and parse the CSV
        awards = _download_and_parse_bulk_csv(
            agencies_upper={a.upper() for a in agencies},
            states_upper={s.upper() for s in states},
            start_year=start_year,
        )
        # Cache the filtered results (much smaller than raw CSV)
        awards_data = [_sbir_award_to_dict(a) for a in awards]
        cache.put(
            "sbir",
            cache_key,
            json.dumps(awards_data),
            ttl=timedelta(days=7),
        )
        logger.info("SBIR bulk: downloaded and cached %d awards", len(awards))

    filtered = _filter_by_amount(awards, floor)
    logger.info(
        "SBIR bulk: %d awards total, %d after amount filter",
        len(awards),
        len(filtered),
    )
    return _group_by_firm(filtered)


def _download_and_parse_bulk_csv(
    agencies_upper: Set[str],
    states_upper: Set[str],
    start_year: int,
) -> List[SbirAward]:
    """Download the bulk CSV to a temp file, then stream-parse matching rows.

    Args:
        agencies_upper: Uppercased agency codes to include (e.g. {"DOD", "NASA"}).
        states_upper: Uppercased state codes to include (e.g. {"AZ", "TX"}).
        start_year: Earliest award year to include.

    Returns:
        List of filtered SbirAward objects.
    """
    logger.info("SBIR bulk: downloading CSV from %s", BULK_CSV_URL)

    tmp_path = None
    try:
        # Stream-download to temp file to avoid holding ~100MB in memory
        with httpx.Client(timeout=300.0, follow_redirects=True) as client:
            with client.stream("GET", BULK_CSV_URL) as response:
                response.raise_for_status()
                with tempfile.NamedTemporaryFile(
                    mode="wb", suffix=".csv", delete=False,
                ) as tmp:
                    tmp_path = tmp.name
                    for chunk in response.iter_bytes(chunk_size=65536):
                        tmp.write(chunk)

        logger.info("SBIR bulk: CSV downloaded (%s)", tmp_path)

        # Parse the CSV file line by line
        awards: List[SbirAward] = []
        col_map: Optional[Dict[str, str]] = None

        with open(tmp_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames:
                col_map = _resolve_csv_columns(list(reader.fieldnames))
                logger.debug("SBIR bulk CSV columns resolved: %s", col_map)

            if not col_map:
                raise ValueError("Could not read CSV header or no columns matched")

            rows_read = 0
            for row in reader:
                rows_read += 1
                award = _parse_csv_row(
                    row, col_map, agencies_upper, states_upper, start_year,
                )
                if award is not None:
                    awards.append(award)

        logger.info(
            "SBIR bulk: parsed %d rows, %d matched filters",
            rows_read,
            len(awards),
        )
        return awards
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


def _resolve_csv_columns(header: List[str]) -> Dict[str, str]:
    """Map CSV column names to internal keys.

    Args:
        header: List of column header strings from the CSV.

    Returns:
        Dict mapping internal key → actual CSV column name found in header.
    """
    header_lower: Dict[str, str] = {}
    for h in header:
        header_lower[h.lower().strip()] = h

    mapping: Dict[str, str] = {}
    for internal_key, variants in _CSV_COLUMNS.items():
        for variant in variants:
            if variant.lower() in header_lower:
                mapping[internal_key] = header_lower[variant.lower()]
                break

    return mapping


def _parse_csv_row(
    row: Dict[str, str],
    col_map: Dict[str, str],
    agencies_upper: Set[str],
    states_upper: Set[str],
    start_year: int,
) -> Optional[SbirAward]:
    """Parse and filter a single CSV row into an SbirAward.

    Returns None if the row doesn't match filters or has missing data.

    Args:
        row: Dict from csv.DictReader.
        col_map: Mapping from internal key → CSV column name.
        agencies_upper: Uppercased agency codes to include.
        states_upper: Uppercased state codes to include.
        start_year: Earliest award year to include.

    Returns:
        SbirAward or None.
    """
    def _get(key: str) -> str:
        col = col_map.get(key)
        return (row.get(col, "") or "").strip() if col else ""

    # --- Quick filters before full parse ---
    agency_raw = _get("agency")
    # Normalize full agency names (CSV) to codes (API format)
    agency_code = _AGENCY_FULL_TO_CODE.get(agency_raw.lower(), agency_raw.upper())
    if agency_code not in agencies_upper:
        return None

    state_raw = _get("state")
    if state_raw.upper() not in states_upper:
        return None

    # Year filter
    year_str = _get("award_year")
    try:
        award_year = int(year_str)
    except (ValueError, TypeError):
        award_year = 0
    if award_year < start_year:
        return None

    firm = _get("firm")
    if not firm:
        return None

    # --- Full parse ---
    phase = _PHASE_MAP.get(_get("phase"), _get("phase"))

    try:
        amount_str = _get("award_amount").replace(",", "").replace("$", "")
        award_amount = float(amount_str) if amount_str else 0.0
    except (ValueError, TypeError):
        award_amount = 0.0

    # Parse date
    award_date = None
    date_str = _get("award_start_date")
    if date_str:
        try:
            award_date = date.fromisoformat(date_str[:10])
        except (ValueError, TypeError):
            # Try MM/DD/YYYY format
            try:
                parts = date_str.split("/")
                if len(parts) == 3:
                    award_date = date(
                        int(parts[2][:4]), int(parts[0]), int(parts[1]),
                    )
            except (ValueError, IndexError):
                pass

    uei = _get("uei")
    contract = _get("contract")
    award_id = contract or "{}-{}-{}".format(
        uei or firm[:10], award_year, phase,
    )

    program = _get("program") or "SBIR"
    city = _get("city")
    abstract = _get("abstract")

    return SbirAward(
        award_id=award_id,
        firm=firm,
        agency=agency_code,
        phase=phase,
        program=program,
        award_title=_get("award_title"),
        award_amount=award_amount,
        award_date=award_date,
        state=state_raw.upper(),
        city=city,
        abstract=abstract,
        uei=uei,
        source_url="https://www.sbir.gov/sbirsearch/detail/{}".format(award_id),
    )


# ---------------------------------------------------------------------------
# Cache serialization helpers
# ---------------------------------------------------------------------------


def _sbir_award_to_dict(award: SbirAward) -> Dict[str, Any]:
    """Serialize an SbirAward to a JSON-safe dict for caching."""
    return {
        "award_id": award.award_id,
        "firm": award.firm,
        "agency": award.agency,
        "phase": award.phase,
        "program": award.program,
        "award_title": award.award_title,
        "award_amount": award.award_amount,
        "award_date": award.award_date.isoformat() if award.award_date else None,
        "state": award.state,
        "city": award.city,
        "abstract": award.abstract,
        "uei": award.uei,
        "source_url": award.source_url,
    }


def _dict_to_sbir_award(d: Dict[str, Any]) -> SbirAward:
    """Deserialize a cached dict back to an SbirAward."""
    award_date = None
    if d.get("award_date"):
        try:
            award_date = date.fromisoformat(d["award_date"])
        except (ValueError, TypeError):
            pass

    return SbirAward(
        award_id=d.get("award_id", ""),
        firm=d.get("firm", ""),
        agency=d.get("agency", ""),
        phase=d.get("phase", ""),
        program=d.get("program", "SBIR"),
        award_title=d.get("award_title", ""),
        award_amount=float(d.get("award_amount", 0) or 0),
        award_date=award_date,
        state=d.get("state", ""),
        city=d.get("city", ""),
        abstract=d.get("abstract", ""),
        uei=d.get("uei", ""),
        source_url=d.get("source_url", ""),
    )


# ---------------------------------------------------------------------------
# API fetcher (fallback)
# ---------------------------------------------------------------------------


def _fetch_via_api(
    agencies: Optional[List[str]] = None,
    states: Optional[List[str]] = None,
    lookback_years: Optional[int] = None,
    min_amount: Optional[float] = None,
) -> List[Prospect]:
    """Fetch SBIR/STTR awards from the sbir.gov paginated API.

    This is the fallback path when the bulk CSV download fails.
    The API is heavily rate-limited (10 req/10 min) and may issue 403
    IP bans, so the bulk CSV is always preferred.

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
        "SBIR API: fetched %d awards, %d after territory + amount filter",
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
    cache = get_cache()

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

            # Check cache first
            cache_key = {"endpoint": "sbir", "agency": agency, "year": year, "offset": offset}
            cached = cache.get("sbir", cache_key)
            if cached is not None:
                data = json.loads(cached)
            else:
                try:
                    # Use max_retries=1 to avoid burning rate limit budget on retries.
                    # Rate limiter enforces 1 req/6s (10 per 10 minutes).
                    response = get_with_retry(
                        BASE_URL, params=params, timeout=30.0, max_retries=1,
                        endpoint="sbir",
                    )
                    data = response.json()
                    cache.put("sbir", cache_key, json.dumps(data))
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

        # Rate limiter handles inter-request pacing automatically

    # If every year failed and we got nothing, raise so outer fetch() sees it
    if not all_results and year_errors:
        raise RuntimeError(
            "SBIR {} all years failed ({})".format(agency, ", ".join(year_errors))
        )

    logger.debug("SBIR agency=%s: %d awards fetched", agency, len(all_results))
    return all_results


# ---------------------------------------------------------------------------
# Shared parsers and filters
# ---------------------------------------------------------------------------


def _parse_award(raw: Dict[str, Any]) -> Optional[SbirAward]:
    """Parse a raw SBIR award dict (API format) into an SbirAward.

    Args:
        raw: Raw award dict from sbir.gov API response.

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
        phase = _PHASE_MAP.get(phase, phase)

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

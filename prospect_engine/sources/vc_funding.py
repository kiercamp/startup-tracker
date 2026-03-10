"""VC/private funding tracker.

Provides a pluggable interface for VC funding data sources.
Ships with a StubFundingSource for development/testing and a
placeholder CrunchbaseFundingSource for future implementation.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date
from typing import Dict, List, Optional, Protocol, runtime_checkable

from prospect_engine.config import TARGET_STATES, CRUNCHBASE_API_KEY, LOOKBACK_YEARS
from prospect_engine.models.prospect import VcRound, Prospect

logger = logging.getLogger(__name__)


@runtime_checkable
class FundingSource(Protocol):
    """Protocol for VC/private funding data sources."""

    def get_rounds(
        self,
        states: List[str],
        lookback_days: int,
    ) -> List[VcRound]:
        """Retrieve VC/private funding rounds for target territory."""
        ...


class StubFundingSource:
    """Mock VC funding source for development and testing.

    Returns deterministic sample data that exercises the full pipeline.
    """

    def get_rounds(self, states: List[str], lookback_days: int) -> List[VcRound]:
        """Return a fixed set of mock VC rounds for Southwest A&D companies."""
        return [
            VcRound(
                round_id="vc-stub-001",
                company_name="Loft Orbital",
                round_type="Series B",
                amount_usd=140_000_000.0,
                announced_date=date(2024, 9, 15),
                lead_investor="BlackRock",
                source="stub",
            ),
            VcRound(
                round_id="vc-stub-002",
                company_name="Optisys",
                round_type="Series A",
                amount_usd=12_000_000.0,
                announced_date=date(2024, 3, 1),
                lead_investor="Lockheed Ventures",
                source="stub",
            ),
            VcRound(
                round_id="vc-stub-003",
                company_name="LEAP Space",
                round_type="Seed",
                amount_usd=5_000_000.0,
                announced_date=date(2024, 7, 20),
                lead_investor="Techstars",
                source="stub",
            ),
            VcRound(
                round_id="vc-stub-004",
                company_name="Phantom Space",
                round_type="Series A",
                amount_usd=16_000_000.0,
                announced_date=date(2024, 1, 10),
                lead_investor="Prime Movers Lab",
                source="stub",
            ),
            VcRound(
                round_id="vc-stub-005",
                company_name="Ursa Major Technologies",
                round_type="Series C",
                amount_usd=100_000_000.0,
                announced_date=date(2024, 5, 22),
                lead_investor="Baillie Gifford",
                source="stub",
            ),
        ]


class CrunchbaseFundingSource:
    """Crunchbase Enterprise API funding source (requires paid API key).

    Not yet implemented.
    """

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def get_rounds(self, states: List[str], lookback_days: int) -> List[VcRound]:
        raise NotImplementedError(
            "CrunchbaseFundingSource is not yet implemented. "
            "Set CRUNCHBASE_API_KEY in .env and implement this class."
        )


def _get_active_source() -> FundingSource:
    """Return the appropriate FundingSource based on environment configuration."""
    if CRUNCHBASE_API_KEY:
        return CrunchbaseFundingSource(api_key=CRUNCHBASE_API_KEY)
    logger.warning("CRUNCHBASE_API_KEY not set — using StubFundingSource")
    return StubFundingSource()


def fetch(
    states: Optional[List[str]] = None,
    lookback_days: Optional[int] = None,
) -> List[Prospect]:
    """Fetch VC/private funding rounds and return as Prospect objects.

    Args:
        states: States to filter. Defaults to TARGET_STATES.
        lookback_days: Days back to include. Defaults to LOOKBACK_YEARS * 365.

    Returns:
        List of Prospect objects with vc_rounds populated.
    """
    states = states or TARGET_STATES
    days = lookback_days or (LOOKBACK_YEARS * 365)

    source = _get_active_source()
    try:
        rounds = source.get_rounds(states=states, lookback_days=days)
    except NotImplementedError:
        logger.warning("VC funding source not implemented, returning empty list")
        return []
    except Exception:
        logger.exception("VC funding fetch failed")
        return []

    logger.info("VC funding: fetched %d rounds", len(rounds))
    return _group_by_company(rounds)


def _group_by_company(rounds: List[VcRound]) -> List[Prospect]:
    """Group VcRound objects by company name into Prospect objects.

    Args:
        rounds: List of VcRound objects.

    Returns:
        List of Prospect objects with vc_rounds populated.
    """
    groups: Dict[str, List[VcRound]] = defaultdict(list)
    for r in rounds:
        key = r.company_name.strip().upper()
        groups[key].append(r)

    prospects = []
    for _key, group_rounds in groups.items():
        first = group_rounds[0]
        prospects.append(
            Prospect(
                company_name=first.company_name,
                vc_rounds=group_rounds,
                data_sources=["vc_funding"],
            )
        )
    return prospects

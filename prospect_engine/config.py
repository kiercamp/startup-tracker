"""Centralized configuration for the A&D Prospect Engine."""

from __future__ import annotations

import os
from pathlib import Path
from typing import List

from dotenv import load_dotenv

load_dotenv()


def _get_secret(key: str, default: str = "") -> str:
    """Load a secret from env vars (local .env) or Streamlit secrets (Cloud)."""
    val = os.environ.get(key, "")
    if val:
        return val
    try:
        import streamlit as st  # noqa: F811

        return st.secrets.get(key, default)
    except Exception:
        return default


# --- Paths ---
ROOT_DIR: Path = Path(__file__).parent.parent
LOGS_DIR: Path = ROOT_DIR / "logs"
OUTPUT_DIR: Path = ROOT_DIR / "output_data"
SEED_DATA_DIR: Path = ROOT_DIR / "seed_data"
SEED_SNAPSHOT_PATH: Path = SEED_DATA_DIR / "prospect_snapshot.json"
try:
    LOGS_DIR.mkdir(exist_ok=True)
except OSError:
    pass
try:
    OUTPUT_DIR.mkdir(exist_ok=True)
except OSError:
    pass

# --- API Keys ---
SAM_GOV_API_KEY: str = _get_secret("SAM_GOV_API_KEY")

# --- Territory ---
TARGET_STATES: List[str] = ["AZ", "NM", "CO", "UT", "TX"]

# --- NAICS Codes ---
TARGET_NAICS: List[str] = [
    "336414",  # Guided missile & space vehicle manufacturing
    "336415",  # Propulsion units & parts
    "334511",  # Search, detection, navigation instruments
    "541715",  # R&D in physical/engineering/life sciences
    "336413",  # Other aircraft parts & equipment
    "541330",  # Engineering services
]

# --- Date Range ---
LOOKBACK_YEARS: int = 5
FOUNDED_WITHIN_YEARS: int = 10

# --- API Rate Limits ---
SAM_GOV_DAILY_LIMIT: int = 1000
SAM_GOV_PAGE_SIZE: int = 100
SAM_GOV_REQUEST_DELAY: float = 4.0
USASPENDING_PAGE_SIZE: int = 100
USASPENDING_REQUEST_DELAY: float = 1.5
SBIR_PAGE_SIZE: int = 500
SBIR_REQUEST_DELAY: float = 4.0

# --- Award Amount Floor ---
MIN_AWARD_AMOUNT: float = 1_000_000

# --- Retry Settings ---
MAX_RETRIES: int = 5
INITIAL_BACKOFF_SECONDS: float = 5.0
BACKOFF_MULTIPLIER: float = 2.0
MAX_BACKOFF_SECONDS: float = 60.0

# --- Outreach Flag Settings ---
OUTREACH_LOOKBACK_DAYS: int = 90

# --- DoD Innovation Programs (keyword match for outreach flags) ---
DOD_INNOVATION_PROGRAMS: List[str] = [
    "AFWERX",
    "SpaceWERX",
    "SOFWERX",
    "NavalX",
    "xTechSearch",
    "xTech",
    "DIU",
    "Defense Innovation Unit",
    "DARPA",
    "AFRL",
    "Space Force",
]

# --- Known Pipeline Accounts (do not re-score as cold) ---
EXISTING_PIPELINE: List[str] = [
    "Loft Orbital",
    "Optisys",
    "ITI-RCS",
    "LEAP Space",
]

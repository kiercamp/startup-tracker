"""Centralized configuration for the A&D Prospect Engine."""

from __future__ import annotations

import os
from pathlib import Path
from typing import List

from dotenv import load_dotenv

load_dotenv()

# --- Paths ---
ROOT_DIR: Path = Path(__file__).parent.parent
LOGS_DIR: Path = ROOT_DIR / "logs"
OUTPUT_DIR: Path = ROOT_DIR / "output_data"
try:
    LOGS_DIR.mkdir(exist_ok=True)
except OSError:
    pass
try:
    OUTPUT_DIR.mkdir(exist_ok=True)
except OSError:
    pass

# --- API Keys ---
SAM_GOV_API_KEY: str = os.environ.get("SAM_GOV_API_KEY", "")
CRUNCHBASE_API_KEY: str = os.environ.get("CRUNCHBASE_API_KEY", "")

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
LOOKBACK_YEARS: int = 10
FOUNDED_WITHIN_YEARS: int = 10

# --- API Rate Limits ---
SAM_GOV_DAILY_LIMIT: int = 1000
SAM_GOV_PAGE_SIZE: int = 100
USASPENDING_PAGE_SIZE: int = 100
SBIR_PAGE_SIZE: int = 100

# --- Retry Settings ---
MAX_RETRIES: int = 2
INITIAL_BACKOFF_SECONDS: float = 1.0
BACKOFF_MULTIPLIER: float = 2.0
MAX_BACKOFF_SECONDS: float = 10.0

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

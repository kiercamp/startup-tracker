"""SAM.gov bulk entity data download and import.

Downloads the daily SAM.gov entity extract (~2-4 GB ZIP), stream-parses
it, filters by target state + NAICS codes, and loads matching rows into
the ``sam_entities`` SQLite table.  This counts as a single API request
and replaces hundreds of individual Entity API calls.

After import, :mod:`entity_lookup` queries ``sam_entities`` first and
falls back to the live API only for misses.

Usage::

    from prospect_engine.sources.sam_bulk import refresh_bulk_entities

    stats = refresh_bulk_entities()
    print(stats)  # {"rows_inserted": 12345, "rows_skipped": 500000, ...}
"""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from prospect_engine.config import (
    _get_secret,
    TARGET_STATES,
    TARGET_NAICS,
)
from prospect_engine.utils.db import get_connection

logger = logging.getLogger(__name__)

# SAM.gov bulk data extract endpoint (requires API key)
BULK_EXTRACT_URL = "https://api.sam.gov/entity-information/v3/download-entities"

# Column names in the bulk CSV that we care about
_COL_UEI = "UNIQUE ENTITY ID"
_COL_LEGAL_NAME = "LEGAL BUSINESS NAME"
_COL_DBA_NAME = "DBA NAME"
_COL_STATE = "PHYSICAL ADDRESS STATE"
_COL_CITY = "PHYSICAL ADDRESS CITY"
_COL_NAICS = "NAICS CODE"
_COL_START_DATE = "ENTITY START DATE"
_COL_REG_STATUS = "REGISTRATION STATUS"
_COL_CAGE = "CAGE CODE"

# Batch size for SQLite inserts
BATCH_SIZE = 1000


def refresh_bulk_entities(
    zip_path: Optional[Path] = None,
    states: Optional[List[str]] = None,
    naics_codes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Download (or load from disk) the SAM.gov bulk entity extract.

    Filters entities to target states and NAICS codes, then upserts into
    the ``sam_entities`` table.

    Args:
        zip_path: Path to a pre-downloaded ZIP file (skips the download step).
            If None, downloads from the SAM.gov API.
        states: State codes to filter.  Defaults to TARGET_STATES.
        naics_codes: NAICS codes to filter.  Defaults to TARGET_NAICS.

    Returns:
        Dict with stats: ``rows_inserted``, ``rows_skipped``, ``rows_total``,
        ``elapsed_seconds``.
    """
    states_set = {s.upper() for s in (states or TARGET_STATES)}
    naics_set = set(naics_codes or TARGET_NAICS)
    start_time = datetime.utcnow()

    if zip_path is not None:
        logger.info("Loading bulk entities from local file: %s", zip_path)
        stats = _process_zip_file(zip_path, states_set, naics_set)
    else:
        logger.info("Downloading SAM.gov bulk entity extract...")
        stats = _download_and_process(states_set, naics_set)

    elapsed = (datetime.utcnow() - start_time).total_seconds()
    stats["elapsed_seconds"] = elapsed
    logger.info(
        "Bulk entity refresh: %d inserted, %d skipped, %d total rows, %.1fs",
        stats["rows_inserted"],
        stats["rows_skipped"],
        stats["rows_total"],
        elapsed,
    )
    return stats


def _download_and_process(
    states_set: Set[str],
    naics_set: Set[str],
) -> Dict[str, int]:
    """Download the bulk ZIP and stream-process it.

    Returns:
        Stats dict with ``rows_inserted``, ``rows_skipped``, ``rows_total``.
    """
    import httpx

    api_key = _get_secret("SAM_GOV_API_KEY")
    if not api_key:
        raise ValueError(
            "SAM_GOV_API_KEY is required for bulk entity download. "
            "Register at SAM.gov and add your key to .env"
        )

    # Stream the ZIP into memory in chunks to avoid loading 2+ GB at once.
    # NOTE: SAM.gov bulk extract responses can be very large. For production
    # use, consider streaming to a temp file instead.
    logger.info("Requesting bulk extract from SAM.gov (this may take several minutes)...")
    with httpx.stream(
        "GET",
        BULK_EXTRACT_URL,
        params={"api_key": api_key, "fileType": "csv"},
        timeout=600.0,  # 10 minute timeout for large downloads
    ) as response:
        response.raise_for_status()

        # Stream to a temp file to avoid holding 2+ GB in memory
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp_path = Path(tmp.name)
            total_bytes = 0
            for chunk in response.iter_bytes(chunk_size=8192):
                tmp.write(chunk)
                total_bytes += len(chunk)
                if total_bytes % (100 * 1024 * 1024) == 0:
                    logger.info("Downloaded %.0f MB...", total_bytes / 1024 / 1024)

        logger.info("Download complete: %.0f MB", total_bytes / 1024 / 1024)

    try:
        return _process_zip_file(tmp_path, states_set, naics_set)
    finally:
        # Clean up temp file
        try:
            tmp_path.unlink()
        except OSError:
            pass


def _process_zip_file(
    zip_path: Path,
    states_set: Set[str],
    naics_set: Set[str],
) -> Dict[str, int]:
    """Open a SAM.gov bulk extract ZIP and process the CSV inside.

    Args:
        zip_path: Path to the downloaded ZIP file.
        states_set: Target state codes (uppercase).
        naics_set: Target NAICS code strings.

    Returns:
        Stats dict with ``rows_inserted``, ``rows_skipped``, ``rows_total``.
    """
    stats = {"rows_inserted": 0, "rows_skipped": 0, "rows_total": 0}

    with zipfile.ZipFile(str(zip_path), "r") as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            raise ValueError("No CSV files found in ZIP: {}".format(zip_path))

        csv_name = csv_names[0]
        logger.info("Processing CSV from ZIP: %s", csv_name)

        with zf.open(csv_name) as f:
            # Wrap in TextIOWrapper for csv.DictReader
            text_stream = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
            reader = csv.DictReader(text_stream)

            batch: List[Dict[str, str]] = []
            for row in reader:
                stats["rows_total"] += 1

                # Filter by state
                state = (row.get(_COL_STATE, "") or "").strip().upper()
                if state not in states_set:
                    stats["rows_skipped"] += 1
                    continue

                # Filter by NAICS (may be a comma-separated list)
                naics_raw = row.get(_COL_NAICS, "") or ""
                entity_naics = {n.strip() for n in naics_raw.split(",") if n.strip()}
                if naics_set and not entity_naics.intersection(naics_set):
                    stats["rows_skipped"] += 1
                    continue

                # This entity matches — add to batch
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    inserted = _insert_batch(batch)
                    stats["rows_inserted"] += inserted
                    batch.clear()

                # Progress logging
                if stats["rows_total"] % 100_000 == 0:
                    logger.info(
                        "Processed %d rows (%d inserted, %d skipped)",
                        stats["rows_total"],
                        stats["rows_inserted"],
                        stats["rows_skipped"],
                    )

            # Final partial batch
            if batch:
                inserted = _insert_batch(batch)
                stats["rows_inserted"] += inserted

    return stats


def _insert_batch(rows: List[Dict[str, str]]) -> int:
    """Insert or replace a batch of entity rows into the sam_entities table.

    Args:
        rows: List of CSV row dicts.

    Returns:
        Number of rows inserted.
    """
    conn = get_connection()
    now = datetime.utcnow().isoformat()
    inserted = 0

    for row in rows:
        uei = (row.get(_COL_UEI, "") or "").strip()
        if not uei:
            continue

        legal_name = (row.get(_COL_LEGAL_NAME, "") or "").strip()
        if not legal_name:
            continue

        conn.execute(
            "INSERT OR REPLACE INTO sam_entities "
            "(uei, legal_name, dba_name, state, city, naics_codes, "
            "entity_start_date, registration_status, cage_code, loaded_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                uei,
                legal_name,
                (row.get(_COL_DBA_NAME, "") or "").strip(),
                (row.get(_COL_STATE, "") or "").strip().upper(),
                (row.get(_COL_CITY, "") or "").strip(),
                (row.get(_COL_NAICS, "") or "").strip(),
                (row.get(_COL_START_DATE, "") or "").strip(),
                (row.get(_COL_REG_STATUS, "") or "").strip(),
                (row.get(_COL_CAGE, "") or "").strip(),
                now,
            ),
        )
        inserted += 1

    conn.commit()
    return inserted


def lookup_entity_from_bulk(
    uei: Optional[str] = None,
    company_name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Look up an entity from the bulk-imported sam_entities table.

    Tries UEI first (exact match), then falls back to legal_name LIKE search.

    Args:
        uei: Unique Entity Identifier (12-character string).
        company_name: Company name for fuzzy matching.

    Returns:
        Entity dict with keys matching sam_entities columns, or None.
    """
    conn = get_connection()

    if uei:
        row = conn.execute(
            "SELECT * FROM sam_entities WHERE uei = ?", (uei.strip().upper(),)
        ).fetchone()
        if row:
            return dict(row)

    if company_name:
        # Try exact match first
        row = conn.execute(
            "SELECT * FROM sam_entities WHERE UPPER(legal_name) = ?",
            (company_name.strip().upper(),)
        ).fetchone()
        if row:
            return dict(row)

        # Fall back to LIKE search
        row = conn.execute(
            "SELECT * FROM sam_entities WHERE UPPER(legal_name) LIKE ?",
            ("%{}%".format(company_name.strip().upper()),)
        ).fetchone()
        if row:
            return dict(row)

    return None


def bulk_entity_count() -> int:
    """Return the number of entities in the sam_entities table."""
    conn = get_connection()
    row = conn.execute("SELECT COUNT(*) AS cnt FROM sam_entities").fetchone()
    return row["cnt"] if row else 0

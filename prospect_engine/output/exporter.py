"""Export prospects to CSV, JSON, and SQLite formats."""

from __future__ import annotations

import csv
import json
import logging
import sqlite3
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, List, Optional

from prospect_engine.config import OUTPUT_DIR
from prospect_engine.models.prospect import Prospect

logger = logging.getLogger(__name__)

# CSV columns (flat summary)
CSV_COLUMNS = [
    "company_name",
    "uei",
    "state",
    "city",
    "contract_count",
    "total_contract_obligation",
    "sbir_phase_i_count",
    "sbir_phase_ii_count",
    "sbir_phase_iii_count",
    "total_sbir_amount",
    "total_vc_raised",
    "total_funding",
    "latest_contract_date",
    "latest_sbir_date",
    "latest_vc_date",
    "outreach_flags",
    "data_sources",
    "founded_year",
]


def export_csv(
    prospects: List[Prospect],
    output_path: Optional[Path] = None,
) -> Path:
    """Export prospects to a flat CSV file.

    Args:
        prospects: Sorted list of Prospect objects.
        output_path: Destination path. Defaults to OUTPUT_DIR/prospects_{date}.csv.

    Returns:
        Path to the written CSV file.
    """
    if output_path is None:
        output_path = OUTPUT_DIR / "prospects_{}.csv".format(date.today().isoformat())

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()

        for p in prospects:
            row = {
                "company_name": p.company_name,
                "uei": p.uei,
                "state": p.state,
                "city": p.city,
                "contract_count": p.contract_count,
                "total_contract_obligation": p.total_contract_obligation,
                "sbir_phase_i_count": p.sbir_phase_i_count,
                "sbir_phase_ii_count": p.sbir_phase_ii_count,
                "sbir_phase_iii_count": p.sbir_phase_iii_count,
                "total_sbir_amount": p.total_sbir_amount,
                "total_vc_raised": p.total_vc_raised,
                "total_funding": p.total_funding,
                "latest_contract_date": (
                    p.latest_contract_date.isoformat() if p.latest_contract_date else ""
                ),
                "latest_sbir_date": (
                    p.latest_sbir_date.isoformat() if p.latest_sbir_date else ""
                ),
                "latest_vc_date": (
                    p.latest_vc_date.isoformat() if p.latest_vc_date else ""
                ),
                "outreach_flags": "; ".join(p.outreach_flags),
                "data_sources": ", ".join(p.data_sources),
                "founded_year": p.founded_year or "",
            }
            writer.writerow(row)

    logger.info("Exported %d prospects to CSV: %s", len(prospects), output_path)
    return output_path


def export_json(
    prospects: List[Prospect],
    output_path: Optional[Path] = None,
) -> Path:
    """Export prospects to JSON with full signal detail.

    Args:
        prospects: Sorted list of Prospect objects.
        output_path: Destination path. Defaults to OUTPUT_DIR/prospects_{date}.json.

    Returns:
        Path to the written JSON file.
    """
    if output_path is None:
        output_path = OUTPUT_DIR / "prospects_{}.json".format(date.today().isoformat())

    output_path.parent.mkdir(parents=True, exist_ok=True)

    data = [asdict(p) for p in prospects]

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=_date_serializer)

    logger.info("Exported %d prospects to JSON: %s", len(prospects), output_path)
    return output_path


def export_sqlite(
    prospects: List[Prospect],
    db_path: Optional[Path] = None,
) -> Path:
    """Upsert prospects into a SQLite database for persistent storage.

    Args:
        prospects: Prospect objects to upsert.
        db_path: Path to SQLite database. Defaults to OUTPUT_DIR/prospects.db.

    Returns:
        Path to the SQLite database file.
    """
    if db_path is None:
        db_path = OUTPUT_DIR / "prospects.db"

    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    try:
        _create_tables(conn)
        for p in prospects:
            _upsert_prospect(conn, p)
        conn.commit()
    finally:
        conn.close()

    logger.info("Exported %d prospects to SQLite: %s", len(prospects), db_path)
    return db_path


def _create_tables(conn: sqlite3.Connection) -> None:
    """Create database tables if they don't exist."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS prospects (
            company_name TEXT PRIMARY KEY,
            uei TEXT,
            state TEXT,
            city TEXT,
            contract_count INTEGER,
            total_contract_obligation REAL,
            sbir_phase_i_count INTEGER,
            sbir_phase_ii_count INTEGER,
            sbir_phase_iii_count INTEGER,
            total_sbir_amount REAL,
            total_vc_raised REAL,
            total_funding REAL,
            latest_contract_date TEXT,
            latest_sbir_date TEXT,
            latest_vc_date TEXT,
            outreach_flags TEXT,
            data_sources TEXT,
            founded_year INTEGER
        );

        CREATE TABLE IF NOT EXISTS contract_awards (
            award_id TEXT,
            prospect_name TEXT,
            source TEXT,
            recipient_name TEXT,
            awarding_agency TEXT,
            naics_code TEXT,
            signed_date TEXT,
            obligation_amount REAL,
            description TEXT,
            piid TEXT,
            PRIMARY KEY (award_id, prospect_name),
            FOREIGN KEY (prospect_name) REFERENCES prospects(company_name)
        );

        CREATE TABLE IF NOT EXISTS sbir_awards (
            award_id TEXT,
            prospect_name TEXT,
            firm TEXT,
            agency TEXT,
            phase TEXT,
            program TEXT,
            award_title TEXT,
            award_amount REAL,
            award_date TEXT,
            state TEXT,
            city TEXT,
            abstract TEXT,
            uei TEXT,
            PRIMARY KEY (award_id, prospect_name),
            FOREIGN KEY (prospect_name) REFERENCES prospects(company_name)
        );

        CREATE TABLE IF NOT EXISTS vc_rounds (
            round_id TEXT,
            prospect_name TEXT,
            company_name TEXT,
            round_type TEXT,
            amount_usd REAL,
            announced_date TEXT,
            lead_investor TEXT,
            source TEXT,
            PRIMARY KEY (round_id, prospect_name),
            FOREIGN KEY (prospect_name) REFERENCES prospects(company_name)
        );
        """
    )


def _upsert_prospect(conn: sqlite3.Connection, p: Prospect) -> None:
    """Insert or replace a prospect and its related records."""
    conn.execute(
        """
        INSERT OR REPLACE INTO prospects VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            p.company_name,
            p.uei,
            p.state,
            p.city,
            p.contract_count,
            p.total_contract_obligation,
            p.sbir_phase_i_count,
            p.sbir_phase_ii_count,
            p.sbir_phase_iii_count,
            p.total_sbir_amount,
            p.total_vc_raised,
            p.total_funding,
            p.latest_contract_date.isoformat() if p.latest_contract_date else None,
            p.latest_sbir_date.isoformat() if p.latest_sbir_date else None,
            p.latest_vc_date.isoformat() if p.latest_vc_date else None,
            "; ".join(p.outreach_flags),
            ", ".join(p.data_sources),
            p.founded_year,
        ),
    )

    # Delete old related records and re-insert
    conn.execute(
        "DELETE FROM contract_awards WHERE prospect_name = ?", (p.company_name,)
    )
    for a in p.contract_awards:
        conn.execute(
            "INSERT OR REPLACE INTO contract_awards VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                a.award_id,
                p.company_name,
                a.source,
                a.recipient_name,
                a.awarding_agency,
                a.naics_code,
                a.signed_date.isoformat() if a.signed_date else None,
                a.obligation_amount,
                a.description,
                a.piid,
            ),
        )

    conn.execute("DELETE FROM sbir_awards WHERE prospect_name = ?", (p.company_name,))
    for a in p.sbir_awards:
        conn.execute(
            "INSERT OR REPLACE INTO sbir_awards VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                a.award_id,
                p.company_name,
                a.firm,
                a.agency,
                a.phase,
                a.program,
                a.award_title,
                a.award_amount,
                a.award_date.isoformat() if a.award_date else None,
                a.state,
                a.city,
                a.abstract,
                a.uei,
            ),
        )

    conn.execute("DELETE FROM vc_rounds WHERE prospect_name = ?", (p.company_name,))
    for r in p.vc_rounds:
        conn.execute(
            "INSERT OR REPLACE INTO vc_rounds VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                r.round_id,
                p.company_name,
                r.company_name,
                r.round_type,
                r.amount_usd,
                r.announced_date.isoformat() if r.announced_date else None,
                r.lead_investor,
                r.source,
            ),
        )


def _date_serializer(obj: Any) -> str:
    """JSON serializer for date objects."""
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError("Object of type {} is not JSON serializable".format(type(obj)))

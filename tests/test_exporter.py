"""Tests for output/exporter module."""

import csv
import json
import sqlite3
from datetime import date
from pathlib import Path

from prospect_engine.models.prospect import (
    ContractAward,
    SbirAward,
    VcRound,
    Prospect,
)
from prospect_engine.output.exporter import export_csv, export_json, export_sqlite


def _make_prospect() -> Prospect:
    return Prospect(
        company_name="Test Aerospace",
        uei="TEST123",
        state="AZ",
        city="Tucson",
        contract_awards=[
            ContractAward(
                award_id="C-001",
                source="sam_gov",
                recipient_name="Test Aerospace",
                awarding_agency="DOD",
                naics_code="336414",
                signed_date=date(2024, 6, 1),
                obligation_amount=500_000,
            ),
        ],
        sbir_awards=[
            SbirAward(
                award_id="S-001",
                firm="Test Aerospace",
                agency="DOD",
                phase="Phase II",
                program="SBIR",
                award_title="Antenna",
                award_amount=750_000,
                award_date=date(2024, 3, 1),
                state="AZ",
                city="Tucson",
                uei="TEST123",
            ),
        ],
        vc_rounds=[
            VcRound(
                round_id="vc-001",
                company_name="Test Aerospace",
                round_type="Series A",
                amount_usd=10_000_000,
                announced_date=date(2024, 1, 15),
            ),
        ],
        contract_count=1,
        total_contract_obligation=500_000,
        sbir_phase_ii_count=1,
        total_sbir_amount=750_000,
        total_vc_raised=10_000_000,
        total_funding=11_250_000,
        latest_contract_date=date(2024, 6, 1),
        latest_sbir_date=date(2024, 3, 1),
        latest_vc_date=date(2024, 1, 15),
        outreach_flags=["SBIR Phase II: Antenna"],
        data_sources=["sam_gov", "sbir", "vc_funding"],
    )


def test_export_csv(tmp_path: Path):
    p = _make_prospect()
    csv_path = tmp_path / "test.csv"

    result = export_csv([p], output_path=csv_path)
    assert result == csv_path
    assert csv_path.exists()

    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 1
    assert rows[0]["company_name"] == "Test Aerospace"
    assert rows[0]["state"] == "AZ"
    assert float(rows[0]["total_funding"]) == 11_250_000


def test_export_json(tmp_path: Path):
    p = _make_prospect()
    json_path = tmp_path / "test.json"

    result = export_json([p], output_path=json_path)
    assert result == json_path
    assert json_path.exists()

    with open(json_path, "r") as f:
        data = json.load(f)

    assert len(data) == 1
    assert data[0]["company_name"] == "Test Aerospace"
    assert len(data[0]["contract_awards"]) == 1
    assert len(data[0]["sbir_awards"]) == 1
    assert len(data[0]["vc_rounds"]) == 1
    # Dates serialized as ISO strings
    assert data[0]["contract_awards"][0]["signed_date"] == "2024-06-01"


def test_export_sqlite(tmp_path: Path):
    p = _make_prospect()
    db_path = tmp_path / "test.db"

    result = export_sqlite([p], db_path=db_path)
    assert result == db_path
    assert db_path.exists()

    conn = sqlite3.connect(str(db_path))
    try:
        # Check prospects table
        rows = conn.execute("SELECT * FROM prospects").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "Test Aerospace"

        # Check contract_awards table
        awards = conn.execute("SELECT * FROM contract_awards").fetchall()
        assert len(awards) == 1

        # Check sbir_awards table
        sbir = conn.execute("SELECT * FROM sbir_awards").fetchall()
        assert len(sbir) == 1

        # Check vc_rounds table
        vc = conn.execute("SELECT * FROM vc_rounds").fetchall()
        assert len(vc) == 1
    finally:
        conn.close()


def test_export_csv_multiple(tmp_path: Path):
    """Exporting multiple prospects produces correct number of rows."""
    p1 = _make_prospect()
    p2 = Prospect(
        company_name="Other Corp",
        state="TX",
        total_funding=100_000,
        data_sources=["usa_spending"],
    )
    csv_path = tmp_path / "multi.csv"

    export_csv([p1, p2], output_path=csv_path)

    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 2

# CLAUDE.md — A&D Prospect Engine

## Project Overview
A funding signal tracker and prospect scoring engine for aerospace and defense companies in the Southwest US territory (AZ, NM, CO, UT, TX). The tool identifies, enriches, and prioritizes **companies founded within the last 10 years** as potential buyers of Siemens engineering software (NX, Teamcenter, Simcenter, FEKO, Nastran, Femap) and Altair tools (HyperWorks, Flux, e-Motor Director, HyperMesh, OptiStruct, RADIOSS, SimSolid, SimLab).

The engine tracks all funding events (no minimum threshold) across government and private sources, scores prospects by funding activity and territory fit, and drives two actions: (1) timing outreach to funding events and (2) building and prioritizing the territory account list. Results are rendered as a visual dashboard.

---

## Stack & Environment
- Language: Python 3.11+
- Package manager: pip (use requirements.txt)
- Key libraries: requests, pandas, httpx, python-dotenv, rich (CLI output)
- Data sources: SAM.gov API, USASpending.gov API, SBIR.gov, VC/private funding (Crunchbase API or equivalent)
- Output formats: CSV, JSON, optional SQLite for persistence
- Style: snake_case variables, Google-style docstrings, type hints on all functions

---

## Commands
- Run engine: `python main.py`
- Run tests: `pytest tests/`
- Lint: `ruff check .`
- Format: `black .`
- Install deps: `pip install -r requirements.txt --break-system-packages`

---

## Architecture
```
prospect_engine/
├── main.py                  # Entry point / orchestrator
├── sources/
│   ├── sam_gov.py           # SAM.gov DoD/NASA contract award fetcher
│   ├── usa_spending.py      # USASpending.gov obligation history
│   ├── sbir.py              # SBIR/STTR award lookup
│   └── vc_funding.py        # VC/private round tracker (Crunchbase or equivalent)
├── scoring/
│   ├── scorer.py            # Weighted scoring logic
│   └── criteria.py          # Scoring rubric definitions
├── enrichment/
│   └── company_profile.py   # Merge signals into unified record
├── output/
│   └── exporter.py          # CSV/JSON/SQLite export
├── models/
│   └── prospect.py          # Prospect dataclass
└── config.py                # Territory, NAICS codes, thresholds
```

---

## Domain Context

### Target Territory
Southwest US: Arizona, New Mexico, Colorado, Utah, Texas

### ICP (Ideal Customer Profile)
- Aerospace & defense manufacturers, suppliers, and integrators
- Space systems / satellite / launch vehicle companies
- Defense electronics, radar, antenna, and RF companies
- **Founded within the last 10 years** (no revenue ceiling)
- Company stages: Seed / pre-revenue, SBIR Phase I–II, Series A–C
- Engineering-intensive firms actively receiving government or private funding

### Relevant NAICS Codes
- 336414 — Guided missile & space vehicle manufacturing
- 336415 — Propulsion units & parts
- 334511 — Search, detection, navigation instruments
- 541715 — R&D in physical/engineering/life sciences
- 336413 — Other aircraft parts & equipment
- 541330 — Engineering services

### Product-to-Use-Case Mapping
- **NX** → CAD/CAM, structural design, manufacturing
- **Simcenter** → CFD, FEA, thermal, acoustics, systems simulation (STAR-CCM+, Nastran, Amesim)
- **Teamcenter** → PLM/PDM, configuration management, MBSE, MIL-spec compliance
- **FEKO** → EM simulation, antenna design, radar cross-section, network planning, high frequency
- **HyperWorks** → FEA/optimization, composites

---

## Funding Signals Tracked
All funding events are tracked regardless of dollar amount.

| Source | Signal Type | Notes |
|---|---|---|
| SAM.gov | DoD/NASA contract awards | Active contracts = engineering tool need |
| USASpending.gov | Obligation history | Full spend history, all years |
| SBIR.gov | SBIR/STTR awards | Phase I, II, III — R&D intensity signal |
| Crunchbase / equivalent | VC & private rounds | Seed, Series A–C, strategic rounds |

---

## Scoring Rubric
Prospects are scored 0–100 across four weighted signals (job posting signal removed):

| Signal | Weight | Notes |
|---|---|---|
| Gov contract activity (SAM/USASpending) | 35% | Active DoD/NASA contracts = high fit |
| SBIR/STTR awards | 25% | R&D intensity, phase progression signals |
| VC/private funding activity | 25% | Recent rounds signal growth & budget |
| Territory match | 15% | Must be Southwest US HQ or facility |

Output tier labels: **Hot (75–100) / Warm (50–74) / Cold (<50)**

---

## Outreach Timing Logic
The engine should flag accounts for outreach when:
- A new SBIR Phase II award is detected (budget expansion signal)
- A new SAM.gov contract award is posted for a tracked company
- A VC funding round closes (Series A or later)
- USASpending shows a new obligation on an existing contract

Flagged accounts should surface in the dashboard with the triggering event and date.

---

## Conventions & Rules
- Never commit API keys — use `.env` and `python-dotenv`
- All API calls must handle rate limits with exponential backoff
- Log errors to `logs/errors.log`, don't silently swallow exceptions
- Each data source module must return a list of `Prospect` dataclass objects
- Scoring must be deterministic — same inputs always produce same score
- Don't hardcode company names or scores; everything flows from data
- Keep tasks small: one source module, one scorer, one exporter at a time
- No funding amount minimum — collect and score all awards regardless of size

---

## What NOT to Do
- Don't use BeautifulSoup for JavaScript-heavy sites — use Playwright
- Don't merge/enrich data before all source fetches are complete
- Don't push to any external service or API without confirming with user
- Don't delete output files without explicit instruction
- Never assume a test framework exists — check pytest config first
- Do not filter out funding events by dollar amount

---

## Current Status / TODO
- [ ] SAM.gov fetcher
- [ ] USASpending enrichment
- [ ] SBIR award lookup
- [ ] VC/private funding tracker
- [ ] Scoring engine (updated rubric, no job posting signal)
- [ ] Outreach timing flag logic
- [ ] CSV/SQLite export
- [ ] CLI dashboard with `rich`
- [ ] Visual render dashboard

---

## Key Contacts / Accounts (Reference Only)
Accounts already in pipeline — do not re-score as cold leads:
- Loft Orbital (PLM/Teamcenter priority)
- Optisys (FEKO/additive manufacturing angle)
- ITI-RCS (FEKO, antenna/radome)
- LEAP Space (HyperWorks/Simcenter, early stage)

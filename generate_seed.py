#!/usr/bin/env python3
"""Generate seed data from USASpending and SBIR (skip SAM.gov if rate-limited).

Usage:
    python generate_seed.py              # Try all sources
    python generate_seed.py --skip-sam   # Skip SAM.gov (rate-limited)
    python generate_seed.py --skip-enrich  # Skip entity enrichment (fast mode)
"""
from __future__ import annotations

import argparse
import sys
from prospect_engine.utils.logging_setup import configure_logging
from prospect_engine.sources import sam_gov, usa_spending, sbir
from prospect_engine.enrichment.company_profile import (
    merge_sources,
    enrich_prospect,
    build_outreach_flags,
    filter_by_founded_year,
    filter_known_primes,
)
from prospect_engine.enrichment.entity_lookup import enrich_with_entity_data
from prospect_engine.output.exporter import export_seed_snapshot
from prospect_engine.config import SAM_GOV_API_KEY

configure_logging()


def main():
    parser = argparse.ArgumentParser(
        description="Generate seed data for Streamlit Cloud"
    )
    parser.add_argument(
        "--skip-sam", action="store_true", help="Skip SAM.gov (e.g. when rate-limited)"
    )
    parser.add_argument(
        "--skip-enrich",
        action="store_true",
        help="Skip SAM.gov entity enrichment (faster, no founding dates)",
    )
    parser.add_argument(
        "--skip-sbir",
        action="store_true",
        help="Skip SBIR (e.g. when IP is rate-limited/banned)",
    )
    args = parser.parse_args()

    source_results = []
    status_messages = []

    # SAM.gov
    if args.skip_sam:
        print("  SAM.gov: SKIPPED (--skip-sam)")
        source_results.append([])
        status_messages.append("SAM.gov: skipped (rate-limited)")
    else:
        print("  SAM.gov...", end=" ", flush=True)
        try:
            result = sam_gov.fetch()
            source_results.append(result)
            n = sum(len(p.contract_awards) for p in result)
            msg = "SAM.gov: {} companies, {} awards".format(len(result), n)
            print(msg)
            status_messages.append(msg)
        except Exception as exc:
            source_results.append([])
            msg = "SAM.gov: failed ({})".format(str(exc)[:80])
            print(msg)
            status_messages.append(msg)

    # USASpending
    print("  USASpending...", end=" ", flush=True)
    try:
        result = usa_spending.fetch()
        source_results.append(result)
        n = sum(len(p.contract_awards) for p in result)
        msg = "USASpending: {} companies, {} awards".format(len(result), n)
        print(msg)
        status_messages.append(msg)
    except Exception as exc:
        source_results.append([])
        msg = "USASpending: failed ({})".format(str(exc)[:80])
        print(msg)
        status_messages.append(msg)

    # SBIR
    if args.skip_sbir:
        print("  SBIR: SKIPPED (--skip-sbir)")
        source_results.append([])
        status_messages.append("SBIR: skipped (rate-limited)")
    else:
        print("  SBIR...", end=" ", flush=True)
        try:
            result = sbir.fetch()
            source_results.append(result)
            n = sum(len(p.sbir_awards) for p in result)
            msg = "SBIR: {} companies, {} awards".format(len(result), n)
            print(msg)
            status_messages.append(msg)
        except Exception as exc:
            source_results.append([])
            msg = "SBIR: failed ({})".format(str(exc)[:80])
            print(msg)
            status_messages.append(msg)

    # Merge
    prospects = merge_sources(source_results)
    print("\n  {} prospects after merge".format(len(prospects)))

    # Filter known defense primes
    before = len(prospects)
    prospects = filter_known_primes(prospects)
    print("  {} known primes removed, {} remaining".format(
        before - len(prospects), len(prospects),
    ))

    # Entity enrichment (founding dates, UEI, state)
    if not args.skip_enrich and SAM_GOV_API_KEY:
        print("  SAM.gov Entity enrichment...", flush=True)
        enrich_with_entity_data(prospects, api_key=SAM_GOV_API_KEY)
        enriched = sum(1 for p in prospects if p.founded_year is not None)
        print("  Enriched {}/{} with founding year".format(enriched, len(prospects)))
    elif args.skip_enrich:
        print("  Entity enrichment: SKIPPED (--skip-enrich)")
    else:
        print("  Entity enrichment: SKIPPED (no SAM.gov API key)")

    # Enrich derived fields
    for p in prospects:
        enrich_prospect(p)

    # Filter by founded year
    before = len(prospects)
    prospects = filter_by_founded_year(prospects)
    print("  {} filtered by founding year, {} remaining".format(
        before - len(prospects), len(prospects),
    ))

    # Outreach flags
    for p in prospects:
        build_outreach_flags(p)
    prospects.sort(key=lambda p: p.total_funding, reverse=True)

    print("\n  {} prospects after all filters".format(len(prospects)))

    if prospects:
        path = export_seed_snapshot(prospects, status_messages=status_messages)
        print("  Seed snapshot written to: {}".format(path))
        print("\n  Next steps:")
        print("    git add seed_data/prospect_snapshot.json")
        print('    git commit -m "Update seed data"')
        print("    git push origin main")
    else:
        print("  No prospects found — seed file NOT written.")
        sys.exit(1)


if __name__ == "__main__":
    main()

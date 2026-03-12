"""A&D Prospect Engine — orchestrator and CLI dashboard.

Entry point: python -m prospect_engine.main
"""

from __future__ import annotations

import argparse
import logging
from datetime import date
from typing import List, Optional

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

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
from prospect_engine.output.exporter import (
    export_csv,
    export_json,
    export_sqlite,
    export_seed_snapshot,
)
from prospect_engine.config import EXISTING_PIPELINE, SAM_GOV_API_KEY

console = Console()
logger = logging.getLogger(__name__)


def run_pipeline(
    *,
    export_formats: Optional[List[str]] = None,
    dry_run: bool = False,
    states: Optional[List[str]] = None,
) -> List:
    """Execute the full prospect engine pipeline.

    Steps:
    1. Fetch from all four data sources (failures are logged, not fatal)
    2. Merge and deduplicate into unified Prospect records
    3. Enrich with derived fields
    4. Filter by founded_year if available
    5. Build outreach flags
    6. Sort by total funding descending
    7. Export to requested formats
    8. Render CLI dashboard

    Args:
        export_formats: List of format strings: "csv", "json", "sqlite".
        dry_run: If True, skip export.
        states: Override target states.

    Returns:
        Sorted list of Prospect objects.
    """
    if export_formats is None:
        export_formats = ["csv", "json"]

    # --- Phase 1: Fetch from all sources ---
    console.print("\n[bold cyan]A&D Prospect Engine[/bold cyan]")
    console.print("[dim]Fetching funding signals...[/dim]\n")

    source_results = []
    status_messages = []

    # SAM.gov
    console.print("  [yellow]SAM.gov[/yellow] contract awards...", end=" ")
    try:
        sam_results = sam_gov.fetch(states=states)
        source_results.append(sam_results)
        n_awards = sum(len(p.contract_awards) for p in sam_results)
        msg = "SAM.gov: {} companies, {} awards".format(len(sam_results), n_awards)
        console.print("[green]{}[/green]".format(msg))
        status_messages.append(msg)
    except Exception as exc:
        source_results.append([])
        msg = "SAM.gov: failed ({})".format(str(exc)[:80])
        console.print("[red]{}[/red]".format(msg))
        status_messages.append(msg)
        logger.error("SAM.gov fetch failed: %s", exc)

    # USASpending
    console.print("  [yellow]USASpending[/yellow] obligations...", end=" ")
    try:
        usa_results = usa_spending.fetch(states=states)
        source_results.append(usa_results)
        n_awards = sum(len(p.contract_awards) for p in usa_results)
        msg = "USASpending: {} companies, {} awards".format(len(usa_results), n_awards)
        console.print("[green]{}[/green]".format(msg))
        status_messages.append(msg)
    except Exception as exc:
        source_results.append([])
        msg = "USASpending: failed ({})".format(str(exc)[:80])
        console.print("[red]{}[/red]".format(msg))
        status_messages.append(msg)
        logger.error("USASpending fetch failed: %s", exc)

    # SBIR
    console.print("  [yellow]SBIR/STTR[/yellow] awards...", end=" ")
    try:
        sbir_results = sbir.fetch(states=states)
        source_results.append(sbir_results)
        n_awards = sum(len(p.sbir_awards) for p in sbir_results)
        msg = "SBIR: {} companies, {} awards".format(len(sbir_results), n_awards)
        console.print("[green]{}[/green]".format(msg))
        status_messages.append(msg)
    except Exception as exc:
        source_results.append([])
        msg = "SBIR: failed ({})".format(str(exc)[:80])
        console.print("[red]{}[/red]".format(msg))
        status_messages.append(msg)
        logger.error("SBIR fetch failed: %s", exc)

    # --- Phase 2: Merge ---
    console.print("\n[dim]Merging and enriching...[/dim]")
    prospects = merge_sources(source_results)

    # --- Phase 3: Filter known defense primes ---
    before = len(prospects)
    console.print("  [yellow]Removing known defense primes...[/yellow]", end=" ")
    prospects = filter_known_primes(prospects)
    console.print(
        "[green]Removed {} primes, {} remaining[/green]".format(
            before - len(prospects), len(prospects),
        )
    )

    # --- Phase 3.5: SAM.gov Entity API enrichment ---
    if SAM_GOV_API_KEY:
        console.print("  [yellow]SAM.gov Entity API enrichment...[/yellow]")
        enrich_with_entity_data(prospects, api_key=SAM_GOV_API_KEY)
        enriched = sum(1 for p in prospects if p.founded_year is not None)
        console.print(
            "  [green]Enriched {}/{} with founding year[/green]".format(
                enriched, len(prospects),
            )
        )
    else:
        console.print("  [dim]Skipping entity enrichment (no SAM.gov API key)[/dim]")

    # --- Phase 4: Enrich ---
    for p in prospects:
        enrich_prospect(p)

    # --- Phase 5: Filter by founded year ---
    prospects = filter_by_founded_year(prospects)

    # --- Phase 5: Outreach flags ---
    for p in prospects:
        build_outreach_flags(p)

    # --- Phase 6: Sort by total funding descending ---
    prospects.sort(key=lambda p: p.total_funding, reverse=True)

    # --- Phase 7: Export ---
    if not dry_run:
        console.print("[dim]Exporting results...[/dim]")
        if "csv" in export_formats:
            path = export_csv(prospects)
            console.print("  CSV: [green]{}[/green]".format(path))
        if "json" in export_formats:
            path = export_json(prospects)
            console.print("  JSON: [green]{}[/green]".format(path))
        if "sqlite" in export_formats:
            path = export_sqlite(prospects)
            console.print("  SQLite: [green]{}[/green]".format(path))

        # Always write the seed snapshot for Streamlit Cloud fallback
        seed_path = export_seed_snapshot(prospects, status_messages=status_messages)
        console.print("  Seed snapshot: [green]{}[/green]".format(seed_path))

    # --- Phase 8: Dashboard ---
    render_dashboard(prospects)

    return prospects


def render_dashboard(
    prospects: List,
    reference_date: Optional[date] = None,
) -> None:
    """Render a rich CLI dashboard with prospect data and outreach flags.

    Args:
        prospects: Sorted list of Prospect objects.
        reference_date: Date to display in header. Defaults to today.
    """
    ref = reference_date or date.today()

    # Summary panel
    total = len(prospects)
    flagged = sum(1 for p in prospects if p.outreach_flags)
    pipeline = sum(
        1
        for p in prospects
        if any(name.lower() in p.company_name.lower() for name in EXISTING_PIPELINE)
    )
    total_funding = sum(p.total_funding for p in prospects)

    summary = (
        "[bold]Total Prospects:[/bold] {}\n"
        "[bold]Flagged for Outreach:[/bold] {}\n"
        "[bold]In Pipeline:[/bold] {}\n"
        "[bold]Total Funding Tracked:[/bold] ${:,.0f}"
    ).format(total, flagged, pipeline, total_funding)

    console.print()
    console.print(
        Panel(
            summary,
            title="A&D Prospect Engine — {}".format(ref.isoformat()),
            border_style="cyan",
        )
    )

    # Main table
    table = Table(
        title="Prospects by Total Funding",
        box=box.ROUNDED,
        show_lines=True,
    )
    table.add_column("#", style="dim", width=4)
    table.add_column("Company", style="bold", max_width=30)
    table.add_column("State", width=5)
    table.add_column("Contracts", justify="right", width=10)
    table.add_column("SBIR", justify="right", width=10)
    table.add_column("Total Funding", justify="right", width=16, style="bold green")
    table.add_column("Flags", width=6, justify="center")
    table.add_column("Sources", width=20)

    for i, p in enumerate(prospects, 1):
        flag_count = str(len(p.outreach_flags)) if p.outreach_flags else ""
        flag_style = "[bold red]{}[/bold red]".format(flag_count) if flag_count else ""

        # Highlight pipeline accounts
        name = p.company_name
        if any(n.lower() in name.lower() for n in EXISTING_PIPELINE):
            name = "[cyan]{}[/cyan] *".format(name)

        table.add_row(
            str(i),
            name,
            p.state,
            str(p.contract_count) if p.contract_count else "",
            (
                "I:{} II:{} III:{}".format(
                    p.sbir_phase_i_count, p.sbir_phase_ii_count, p.sbir_phase_iii_count
                )
                if any(
                    [
                        p.sbir_phase_i_count,
                        p.sbir_phase_ii_count,
                        p.sbir_phase_iii_count,
                    ]
                )
                else ""
            ),
            "${:,.0f}".format(p.total_funding),
            flag_style,
            ", ".join(p.data_sources),
        )

    console.print(table)

    # Outreach flags table
    flagged_prospects = [p for p in prospects if p.outreach_flags]
    if flagged_prospects:
        flags_table = Table(
            title="Outreach Flags — Action Required",
            box=box.ROUNDED,
            show_lines=True,
        )
        flags_table.add_column("Company", style="bold", max_width=25)
        flags_table.add_column("Flag", max_width=80)

        for p in flagged_prospects:
            for flag in p.outreach_flags:
                flags_table.add_row(p.company_name, flag)

        console.print(flags_table)

    console.print()


def _show_cache_stats() -> None:
    """Display cache hit/miss ratios and queue stats."""
    from prospect_engine.utils.cache import get_cache
    from prospect_engine.scheduler.sweep import queue_stats

    cache = get_cache()
    stats = cache.stats()

    console.print("\n[bold cyan]Cache Statistics[/bold cyan]")
    if not stats:
        console.print("  [dim]No cache activity recorded this session.[/dim]")
    else:
        table = Table(box=box.SIMPLE)
        table.add_column("Endpoint", style="bold")
        table.add_column("Hits", justify="right", style="green")
        table.add_column("Misses", justify="right", style="yellow")
        table.add_column("Ratio", justify="right")
        for ep, s in stats.items():
            ratio_pct = "{:.0f}%".format(s.get("ratio", 0) * 100)
            table.add_row(ep, str(s["hits"]), str(s["misses"]), ratio_pct)
        console.print(table)

    # Queue stats
    qstats = queue_stats()
    if qstats:
        console.print("\n[bold cyan]Task Queue[/bold cyan]")
        for status, cnt in sorted(qstats.items()):
            console.print("  {}: [bold]{}[/bold]".format(status, cnt))

    # Evict expired entries
    evicted = cache.evict_expired()
    if evicted:
        console.print("\n  [dim]Evicted {} expired cache entries[/dim]".format(evicted))
    console.print()


def _run_sweep_command(sweep_name: Optional[str] = None) -> None:
    """Run sweeps from the CLI.

    Args:
        sweep_name: Specific sweep to run (force), or None for all due sweeps.
    """
    from prospect_engine.scheduler.sweep import run_sweep, run_all_due_sweeps, SWEEP_PROFILES

    if sweep_name:
        if sweep_name not in SWEEP_PROFILES:
            console.print(
                "[red]Unknown sweep: {}[/red]\nAvailable: {}".format(
                    sweep_name, ", ".join(SWEEP_PROFILES.keys())
                )
            )
            return
        console.print("[cyan]Running sweep: {}[/cyan]".format(sweep_name))
        result = run_sweep(sweep_name, force=True)
        console.print("  Status: [bold]{}[/bold]".format(result["status"]))
        if result.get("stats"):
            s = result["stats"]
            console.print(
                "  Tasks: {} processed, {} succeeded, {} failed".format(
                    s["processed"], s["succeeded"], s["failed"]
                )
            )
    else:
        console.print("[cyan]Running all due sweeps...[/cyan]")
        results = run_all_due_sweeps()
        for name, result in results.items():
            status = result.get("status", "unknown")
            if status == "skipped":
                console.print("  {} — [dim]not due[/dim]".format(name))
            else:
                console.print("  {} — [bold]{}[/bold]".format(name, status))


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="A&D Prospect Engine — Funding signal tracker for Southwest US"
    )
    parser.add_argument(
        "--export",
        nargs="+",
        choices=["csv", "json", "sqlite"],
        default=["csv", "json"],
        help="Export formats (default: csv json)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run pipeline without writing output files",
    )
    parser.add_argument(
        "--states",
        nargs="+",
        help="Override target states (default: AZ NM CO UT TX)",
    )
    parser.add_argument(
        "--sweep",
        nargs="?",
        const="__all__",
        metavar="NAME",
        help="Run sweep(s) before the pipeline. No arg = all due sweeps. "
             "Name = run that sweep now (sbir_nightly, sam_gov_6h, usa_spending_4h)",
    )
    parser.add_argument(
        "--sweep-daemon",
        action="store_true",
        help="Start the sweep scheduler as a background daemon alongside the pipeline",
    )
    parser.add_argument(
        "--cache-stats",
        action="store_true",
        help="Show cache hit/miss ratios and task queue stats, then exit",
    )
    return parser.parse_args()


if __name__ == "__main__":
    configure_logging()
    args = _parse_args()

    # Cache stats mode — display and exit
    if args.cache_stats:
        _show_cache_stats()
        raise SystemExit(0)

    # Sweep mode — run sweeps before (or instead of) the pipeline
    if args.sweep:
        sweep_name = None if args.sweep == "__all__" else args.sweep
        _run_sweep_command(sweep_name)

    # Sweep daemon mode — start background scheduler
    if args.sweep_daemon:
        from prospect_engine.scheduler.sweep import start_daemon
        console.print("[cyan]Starting sweep daemon (background)...[/cyan]")
        start_daemon()

    # Normal pipeline
    run_pipeline(
        export_formats=args.export,
        dry_run=args.dry_run,
        states=args.states,
    )

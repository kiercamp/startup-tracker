"""Entry point for the A&D Prospect Engine.

Usage: python main.py [--export csv json sqlite] [--dry-run] [--states AZ TX ...]
                      [--skip sam sbir usaspending]
"""

from prospect_engine.main import _parse_args, run_pipeline
from prospect_engine.utils.logging_setup import configure_logging

if __name__ == "__main__":
    configure_logging()
    args = _parse_args()

    # Cache stats mode
    if getattr(args, "cache_stats", False):
        from prospect_engine.main import _show_cache_stats
        _show_cache_stats()
        raise SystemExit(0)

    # Sweep mode
    if getattr(args, "sweep", None):
        from prospect_engine.main import _run_sweep_command
        sweep_name = None if args.sweep == "__all__" else args.sweep
        _run_sweep_command(sweep_name)

    # Sweep daemon
    if getattr(args, "sweep_daemon", False):
        from prospect_engine.scheduler.sweep import start_daemon
        from rich.console import Console
        Console().print("[cyan]Starting sweep daemon (background)...[/cyan]")
        start_daemon()

    run_pipeline(
        export_formats=args.export,
        dry_run=args.dry_run,
        states=args.states,
        skip_sources=getattr(args, "skip", []),
    )

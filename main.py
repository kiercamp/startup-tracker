"""Entry point for the A&D Prospect Engine.

Usage: python main.py [--export csv json sqlite] [--dry-run] [--states AZ TX ...]
"""

from prospect_engine.main import _parse_args, run_pipeline
from prospect_engine.utils.logging_setup import configure_logging

if __name__ == "__main__":
    configure_logging()
    args = _parse_args()
    run_pipeline(
        export_formats=args.export,
        dry_run=args.dry_run,
        states=args.states,
    )

"""Logging configuration for the A&D Prospect Engine."""

from __future__ import annotations

import logging
import logging.handlers
import sys

from prospect_engine.config import LOGS_DIR


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logger with file handler for errors and stream handler for info.

    Args:
        level: The minimum log level for the stream (console) handler.
    """
    root_logger = logging.getLogger()

    # Avoid adding duplicate handlers on repeated calls
    if root_logger.handlers:
        return

    root_logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler: errors only, rotating 10MB / 3 backups
    error_log_path = LOGS_DIR / "errors.log"
    file_handler = logging.handlers.RotatingFileHandler(
        error_log_path,
        maxBytes=10 * 1024 * 1024,
        backupCount=3,
    )
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Stream handler: configurable level
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

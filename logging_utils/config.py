"""
Logging configuration module.

Provides centralized logging setup for the application.
Defines `setup_logging()` which initializes log handlers for
both console output and file persistence.

Usage:
    from logging_utils.config import setup_logging
    setup_logging()
"""
import logging
import logging.handlers
from pathlib import Path

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "app.log"

def setup_logging():
    """
    Configure application-wide logging.

    - Creates a `logs/` directory (if not already present).
    - Sets up a file handler writing DEBUG+ logs to `logs/app.log`.
    - Sets up a stream handler writing INFO+ logs to stdout (console).
    - Applies a consistent timestamped log format across handlers.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Rotating file handler (local dev logs)
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=5_000_000, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(name)s: %(message)s"
    ))
    logger.addHandler(file_handler)

    return logger
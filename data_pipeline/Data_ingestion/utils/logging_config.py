import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import os

DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "pipeline.log"


def get_logger(name: str):
    logger = logging.getLogger(name)

    # Core: set debugging level based on env variable
    logger.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)

    if logger.handlers:
        return logger

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        "[%(levelname)s] %(name)s: %(message)s"
    ))

    # Rotating file handler
    file_handler = TimedRotatingFileHandler(
        LOG_FILE,
        when="midnight",
        backupCount=7,
        encoding="utf-8"
    )
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    ))

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


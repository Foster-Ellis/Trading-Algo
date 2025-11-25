import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()  # ensure env is available before any loggers initialize


LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "pipeline.log"


def get_logger(name: str):
    DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

    logger = logging.getLogger(name)

    # Always reconfigure logger to match DEBUG_MODE
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)

    # Console output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        "[%(levelname)s] %(name)s: %(message)s"
    ))

    # File output
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


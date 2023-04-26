import logging
from pathlib import Path

from config import LOGGING_LEVEL


def get_logger(name):
    logging.basicConfig(
        level=LOGGING_LEVEL,
        format="%(name)s | %(levelname)-8s | %(message)s",
        datefmt="%m/%d/%Y %H:%M:%S",
    )
    logger = logging.getLogger(name)
    return logger

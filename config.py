import logging
from pathlib import Path


class Config:
    ROOT_DIR = Path(__file__).resolve().parent

    # Directories
    DATA_DIR = ROOT_DIR / "data"
    DATA_PATH = DATA_DIR / "books.jsonl"
    INDEX_DIR = ROOT_DIR / "index"

    # Number of pages to scrape from the website
    MAX_NUM_PAGES = 5

    # Logging level
    LOGGING_LEVEL = "DEBUG"

    # Search engine config
    MODEL_NAME = "all-MiniLM-L6-v2"
    VECTOR_SIZE = 384
    METRIC = "angular"

    logging.basicConfig(
        level=LOGGING_LEVEL,
        format="%(name)s | %(levelname)-8s | %(message)s",
        datefmt="%m/%d/%Y %H:%M:%S",
    )


# The only config is a debug config for now
config = Config()

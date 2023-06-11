import logging
from pathlib import Path


class Config:
    ROOT_DIR = Path(__file__).resolve().parent

    # Directories
    DATA_DIR = ROOT_DIR / "data"
    DATA_PATH = DATA_DIR / "books.jsonl"
    INDEX_DIR = ROOT_DIR / "index"

    # Scraping config
    MAX_NUM_PAGES = 5
    MAX_RETRIES = 3
    RATE_LIMIT = 20  # requests per second

    # Neural search engine config
    MODEL_NAME = "all-MiniLM-L6-v2"
    VECTOR_SIZE = 384
    METRIC = "angular"
    N_TREES = 5

    # Logging level
    LOGGING_LEVEL = "INFO"

    logging.basicConfig(
        level=LOGGING_LEVEL,
        format="%(name)s | %(levelname)-8s | %(message)s",
        datefmt="%m/%d/%Y %H:%M:%S",
    )

    def get(self, key: str):
        return getattr(self, key)


config = Config()

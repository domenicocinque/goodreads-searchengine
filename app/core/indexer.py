from abc import ABC, abstractmethod
from logging import getLogger
from pathlib import Path

import pandas as pd
from annoy import AnnoyIndex
from whoosh.analysis import StemmingAnalyzer
from whoosh.fields import ID, NUMERIC, TEXT, Schema
from whoosh.index import create_in, open_dir
from whoosh.qparser import QueryParser

logger = getLogger(__name__)


class BaseIndexer(ABC):
    def __init__(self, index_dir: Path):
        self.index = None
        self.index_dir = None

    @abstractmethod
    def setup(self, data_file: Path) -> None:
        pass


class Indexer(BaseIndexer):
    # TODO: Probably it is better to store only
    # the fields that are needed for the search
    # while the rest of the data can be stored in a separate file
    # and loaded when needed.
    schema = Schema(
        id=ID(stored=True),
        title=TEXT(stored=True),
        author=TEXT(stored=True),
        rating=NUMERIC(stored=True),
        rating_count=NUMERIC(stored=True),
        review_count=NUMERIC(stored=True),
        description=TEXT(stored=True, analyzer=StemmingAnalyzer()),
    )

    def __init__(self, index_dir: Path) -> None:
        super().__init__(index_dir)

    def create_index(self) -> None:
        """Creates a new index."""
        logger.info(f"Creating new index in {self.index_dir}")
        self.index = create_in(self.index_dir, self.schema)

    def clear_index(self) -> None:
        """Clears the index directory."""
        logger.info(f"Clearing index directory {self.index_dir}")
        for file in self.index_dir.iterdir():
            file.unlink()

    def index_dataframe(self, data_file: Path) -> None:
        """Indexes the data from the given file."""
        logger.info(f"Indexing data from {data_file}")
        df = pd.read_csv(data_file)
        with self.index.writer() as writer:
            for _, row in df.iterrows():
                if not pd.isna(row["description"]):
                    writer.add_document(
                        id=str(row["id"]),
                        title=row["title"],
                        author=row["author"],
                        rating=row["rating"],
                        rating_count=row["rating_count"],
                        review_count=row["review_count"],
                        description=row["description"],
                    )

    def setup(self, data_file: Path) -> None:
        self.clear_index()
        self.create_index()
        self.index_dataframe(data_file=data_file)


class AnnoyIndexer(BaseIndexer):
    def __init__(self, index_dir: Path) -> None:
        super().__init__(index_dir)
        self.index = None
        self.index_dir = index_dir

    def create_index(self, data_file: Path, n_trees: int = 100) -> None:
        logger.info(f"Creating new index in {self.index_dir}")
        df = pd.read_csv(data_file)
        self.index = AnnoyIndex(128, "angular")
        for _, row in df.iterrows():
            if not pd.isna(row["description"]):
                print(row["id"])
                self.index.add_item(row["id"], row["description"])
        self.index.build(n_trees)

    def setup(self, data_file: Path) -> None:
        self.create_index(data_file=data_file)


if __name__ == "__main__":
    indexer = AnnoyIndexer(Path("index"))
    indexer.setup(Path("data/books.csv"))

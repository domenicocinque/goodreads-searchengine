import shutil
from abc import ABC, abstractmethod
from logging import getLogger
from pathlib import Path

import pandas as pd
from annoy import AnnoyIndex  # type: ignore
from whoosh.analysis import StemmingAnalyzer  # type: ignore
from whoosh.fields import ID, NUMERIC, TEXT, Schema  # type: ignore
from whoosh.index import create_in, open_dir  # type: ignore
from whoosh.qparser import QueryParser  # type: ignore

logger = getLogger("Indexer")


class BaseIndexer(ABC):
    """Base class for indexers."""

    @abstractmethod
    def create_index(self, data_path: Path, index_dir: Path) -> None:
        pass

    @abstractmethod
    def clear_index(self, index_dir) -> None:
        pass


class WhooshIndexer(BaseIndexer):
    """Indexer using Whoosh."""

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

    def create_index(self, data_path: Path, index_dir: Path) -> None:
        """Creates a new index."""
        logger.info(f"Creating new index in {index_dir}")
        index = create_in(index_dir, self.schema)

        logger.info(f"Indexing data from {data_path}")
        df = pd.read_csv(data_path)
        with index.writer() as writer:
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

    def clear_index(self, index_dir: Path) -> None:
        """Clears the index directory."""
        logger.info(f"Clearing index directory {index_dir}")
        shutil.rmtree(index_dir)


class AnnoyIndexer(BaseIndexer):
    """Indexer using Annoy.

    params:
        n_trees: number of trees to build
        vector_size: size of the vectors
        metric: metric to use for the distance calculation
    """

    def __init__(
        self, n_trees: int = 100, vector_size: int = 128, metric: str = "angular"
    ) -> None:
        super().__init__()
        self.n_trees = n_trees
        self.vector_size = vector_size
        self.metric = metric

    def create_index(self, data_path: Path, index_dir: Path) -> None:
        logger.info(f"Creating new index in {index_dir}")
        df = pd.read_csv(data_path)
        index = AnnoyIndex(self.vector_size, self.metric)
        for _, row in df.iterrows():
            if not pd.isna(row["description"]):
                print(row["id"])
                index.add_item(row["id"], row["description"])
        index.build(self.n_trees)

    def clear_index(self, index_dir: Path) -> None:
        """Clears the index directory."""
        logger.info(f"Clearing index directory {index_dir}")
        shutil.rmtree(index_dir)


if __name__ == "__main__":
    indexer = AnnoyIndexer(Path("index"))
    indexer.create_index(Path("data/books.csv"), Path("index/books.ann"))

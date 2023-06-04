from abc import ABC, abstractmethod
from logging import getLogger
from pathlib import Path

import pandas as pd
from annoy import AnnoyIndex  # type: ignore
from whoosh.analysis import StemmingAnalyzer  # type: ignore
from whoosh.fields import ID, NUMERIC, TEXT, Schema  # type: ignore
from whoosh.index import create_in, open_dir  # type: ignore
from whoosh.qparser import QueryParser  # type: ignore

from app.core import Book

logger = getLogger(__name__)


class BaseSearcher(ABC):
    """Base class for searchers."""

    def __init__(self, index_dir: Path, *args, **kwargs):
        self.index = self.load_index(index_dir, *args, **kwargs)

    @abstractmethod
    def load_index(self, index_dir: Path) -> None:
        pass

    @abstractmethod
    def search(self, query: str, limit: int = 5) -> list:
        pass


class WhooshSearcher(BaseSearcher):
    """Searcher using Whoosh."""

    def load_index(self, index_dir: Path) -> None:
        """Loads an index from a directory."""
        logger.info(f"Loading index from {index_dir}")
        return open_dir(index_dir)

    def search(self, query: str, limit: int = 5) -> list:
        with self.index.searcher() as searcher:
            # TODO: Extend query parser to search in multiple fields
            query_parser = QueryParser("description", schema=self.index.schema)
            query = query_parser.parse(query)
            results = searcher.search(query, limit=limit)
            return [Book(**result) for result in results]


class AnnoySearcher(BaseSearcher):
    """Searcher using Annoy."""

    def __init__(self, index_dir: Path, vector_size: int = 128, metric: str = "angular"):
        super().__init__(index_dir, vector_size=vector_size, metric=metric)
        self.vector_size = vector_size
        self.metric = metric

    def load_index(self, index_dir: Path, vector_size: int = 128, metric: str = "angular") -> None:
        logger.info(f"Loading index from {index_dir}")
        # Load the Annoy index here with the required parameters
        print(vector_size, metric)
        return AnnoyIndex(f=vector_size, metric=metric).load(str(index_dir))

    def search(self, query: str, limit: int = 5) -> list:
        # Implement the search logic for Annoy index here
        results = self.index.get_nns_by_vector(query, limit)
        return results


def get_search_engine(index_dir: Path, searcher: str = "whoosh") -> BaseSearcher:
    if searcher == "whoosh":
        return WhooshSearcher(index_dir)
    elif searcher == "annoy":
        return AnnoySearcher(index_dir, vector_size=128, metric="angular")
    else:
        raise ValueError(f"Unknown searcher {searcher}")


if __name__ == "__main__":
    pass

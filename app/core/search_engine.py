from logging import getLogger
from pathlib import Path

import pandas as pd
from whoosh.analysis import StemmingAnalyzer  # type: ignore
from whoosh.fields import ID, NUMERIC, TEXT, Schema  # type: ignore
from whoosh.index import create_in, open_dir  # type: ignore
from whoosh.qparser import QueryParser  # type: ignore

from app.core import Book

logger = getLogger(__name__)


class Indexer:
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

    def __init__(self, index_dir) -> None:
        self.index = None
        self.index_dir = index_dir

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


class Searcher:
    """A wrapper class for the Whoosh searcher."""

    def __init__(self, indexer: Indexer):
        self.indexer = indexer

    def search(self, query: str, limit: int = 5) -> list:
        with self.indexer.index.searcher() as searcher:
            # TODO: Extend query parser to search in multiple fields
            query_parser = QueryParser("description", schema=self.indexer.index.schema)
            query = query_parser.parse(query)
            results = searcher.search(query, limit=limit)
            return [Book(**result) for result in results]


def get_indexer(index_dir: Path) -> Indexer:
    """Returns an indexer object if the index directory exists.

    Supposed to be run after the index is set up.
    """
    indexer = Indexer(index_dir)
    if index_dir.exists():
        indexer.index = open_dir(index_dir)
    else:
        raise FileNotFoundError(f"Index directory {index_dir} does not exist.")
    return indexer


def get_search_engine(index_dir: Path) -> Searcher:
    indexer = get_indexer(index_dir)
    searcher = Searcher(indexer)
    return searcher

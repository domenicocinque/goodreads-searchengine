import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd
from whoosh.analysis import StemmingAnalyzer
from whoosh.fields import ID, NUMERIC, TEXT, Schema
from whoosh.index import create_in, open_dir
from whoosh.qparser import QueryParser

from app.logger import get_logger
from app.service.dataset import Book
from config import INDEX_DIR

logger = get_logger(__name__)


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

    def __init__(self) -> None:
        self.index = None
        self.path = None

    def create_index(self, index_dir: Path) -> None:
        """Creates a new index."""
        logger.info(f"Creating index in {index_dir}")
        self.index = create_in(index_dir, self.schema)

    def index_dataframe(self, df: pd.DataFrame) -> None:
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

    def clear_index(self, index_dir: Path) -> None:
        """Clears the index directory."""
        for file in index_dir.iterdir():
            file.unlink()


class Searcher:
    def __init__(self, indexer: Indexer):
        self.indexer = indexer

    def search(self, query: str, limit: int = 5) -> List[Book]:
        with self.indexer.index.searcher() as searcher:
            # TODO: Extend query parser to search in multiple fields
            query_parser = QueryParser("description", schema=self.indexer.index.schema)
            query = query_parser.parse(query)
            results = searcher.search(query, limit=limit)
            return [Book(**result) for result in results]


class SearchEngine:
    def __init__(self, indexer: Indexer, searcher: Searcher):
        self.indexer = indexer
        self.searcher = searcher

    def search(self, query: str, limit: int = 5) -> List[Book]:
        return self.searcher.search(query, limit=limit)


def get_search_engine(index_dir: Path = INDEX_DIR) -> SearchEngine:
    indexer = Indexer()
    if index_dir.exists():
        indexer.index = open_dir(index_dir)
    else:
        indexer.create_index(index_dir)
    searcher = Searcher(indexer)
    return SearchEngine(indexer, searcher)


def get_indexer(index_dir: Path) -> Indexer:
    indexer = Indexer()
    if index_dir.exists():
        indexer.index = open_dir(index_dir)
    else:
        indexer.create_index(index_dir)
    return indexer

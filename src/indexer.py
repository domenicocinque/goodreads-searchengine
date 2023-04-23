import logging
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
from whoosh import index
from whoosh.analysis import StemmingAnalyzer
from whoosh.fields import ID, TEXT, Schema
from whoosh.qparser import QueryParser
from whoosh.searching import Results

from src.dataset import Book, BookDataset

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SearchEngine:
    schema = Schema(
        id=ID(stored=True),
        title=TEXT(stored=True),
        description=TEXT(analyzer=StemmingAnalyzer(), stored=True),
    )

    def __init__(self, data_dir: Path, dataset: BookDataset, index_name: str = "index"):
        self.index_dir = data_dir / index_name

        if not self.index_dir.exists():
            self.index_dir.mkdir()

        if index.exists_in(self.index_dir):
            logger.info("Loading existing index...")
            self.ix = index.open_dir(self.index_dir)
        else:
            logger.info("Creating new index...")
            self.ix = index.create_in(self.index_dir, self.schema)
            self.index(dataset.load_dataset())

    def index(self, data: pd.DataFrame) -> None:
        writer = self.ix.writer()
        for index, row in data.iterrows():
            # Remove books with no description
            if not pd.isna(row["description"]):
                writer.add_document(
                    id=str(row["id"]), title=row["title"], description=row["description"]
                )
        writer.commit()

    def search(self, query: str) -> List[Book]:
        with self.ix.searcher() as searcher:
            query_parser = QueryParser("description", schema=self.ix.schema)
            query = query_parser.parse(query)
            results = searcher.search(query, limit=5)
            return [Book(**result) for result in results]

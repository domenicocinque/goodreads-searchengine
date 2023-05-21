from pathlib import Path
from abc import ABC, abstractmethod
from logging import getLogger

import pandas as pd
from whoosh.analysis import StemmingAnalyzer
from whoosh.fields import ID, NUMERIC, TEXT, Schema
from whoosh.index import create_in, open_dir
from whoosh.qparser import QueryParser

from annoy import AnnoyIndex

logger = getLogger(__name__)

class BaseIndexer(ABC):
    def __init__(self):
        self.index = None
        self.path = None

    @abstractmethod
    def create_index(self, index_dir: Path) -> None:
        pass

    @abstractmethod
    def index_dataframe(self, df: pd.DataFrame) -> None:
        pass

    @abstractmethod
    def clear_index(self, index_dir: Path) -> None:
        pass


class WhooshIndexer(BaseIndexer):
    schema = Schema(
        id=ID(stored=True),
        title=TEXT(stored=True),
        author=TEXT(stored=True),
        rating=NUMERIC(stored=True),
        rating_count=NUMERIC(stored=True),
        review_count=NUMERIC(stored=True),
        description=TEXT(stored=True),
    )

    def __init__(self) -> None:
        super().__init__()

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



class AnnoyIndexer(BaseIndexer):
    def __init__(self):
        super().__init__()

    def create_index(self, index_dir: Path) -> None:
        """Creates a new index."""
        DIM = 768 # TODO: Fix this
        logger.info(f"Creating index in {index_dir}")
        self.index = AnnoyIndex(DIM, 'angular')
        
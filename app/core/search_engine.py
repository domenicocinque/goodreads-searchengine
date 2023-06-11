import json
from abc import ABC, abstractmethod
from logging import getLogger
from pathlib import Path
from typing import Literal

from annoy import AnnoyIndex  # type: ignore
from sentence_transformers import SentenceTransformer
from whoosh.analysis import StemmingAnalyzer  # type: ignore
from whoosh.fields import ID, NUMERIC, TEXT, Schema  # type: ignore
from whoosh.index import Index, create_in, open_dir  # type: ignore
from whoosh.qparser import QueryParser  # type: ignore

from config import Config

logger = getLogger(__name__)


class BaseSearcher(ABC):
    """Base class for searchers."""

    def __init__(self, index_dir: Path, data_path: Path, *args, **kwargs):
        self.index = self.load_index(index_dir, *args, **kwargs)
        self.data_path = data_path

    @abstractmethod
    def load_index(self, index_dir: Path) -> None:
        pass

    @abstractmethod
    def search(self, query: str, limit: int = 5) -> list:
        pass


class WhooshSearcher(BaseSearcher):
    """Searcher using Whoosh."""

    def __init__(self, index_dir: Path, data_path: Path):
        super().__init__(index_dir, data_path)
        # TODO: Extend query parser to search in multiple fields
        self.query_parser = QueryParser("description", schema=self.index.schema)

    def load_index(self, index_dir: Path) -> Index:
        """Loads an index from a directory."""
        logger.info(f"Loading index from {index_dir}")
        return open_dir(index_dir)

    def search(self, query: str, limit: int = 5) -> list:
        with self.index.searcher() as searcher:
            query = self.query_parser.parse(query)
            results = searcher.search(query, limit=limit)
            results_indices = {int(result["id"]) for result in results}

            results_data = []
            with open(self.data_path) as file:
                for line in file:
                    item = json.loads(line)
                    if item["id"] in results_indices:
                        results_data.append(item)
        return results_data


class AnnoySearcher(BaseSearcher):
    """Searcher using Annoy."""

    def __init__(
        self, index_dir: Path, data_path: Path, vector_size: int = 128, metric: str = "angular"
    ):
        super().__init__(index_dir, data_path, vector_size, metric)
        self.vector_size = vector_size
        self.metric = metric
        self.model = SentenceTransformer("all-MiniLM-L6-v2")

    def load_index(
        self,
        index_dir: Path,
        vector_size: int = 128,
        metric: Literal["angular", "euclidean"] = "angular",
    ) -> AnnoyIndex:
        logger.info(f"Loading index from {index_dir}")
        index = AnnoyIndex(f=vector_size, metric=metric)
        index.load(str(index_dir))
        return index

    def search(self, query: str, limit: int = 5) -> list:
        query = self.model.encode([query])[0]
        results_ixs = set(self.index.get_nns_by_vector(query, limit))
        results = []
        with open(self.data_path) as file:
            for i, line in enumerate(file):
                if i in results_ixs:
                    results.append(json.loads(line))
        return results


def get_search_engine(searcher: Literal["whoosh", "annoy"], config: Config) -> BaseSearcher:
    index_dir = Path(config.INDEX_DIR)
    data_path = Path(config.DATA_PATH)
    if searcher == "whoosh":
        return WhooshSearcher(index_dir=index_dir, data_path=data_path)
    elif searcher == "annoy":
        return AnnoySearcher(
            index_dir=index_dir / "index.ann",
            data_path=data_path,
            vector_size=config.VECTOR_SIZE,
            metric=config.METRIC,
        )
    else:
        raise ValueError(f"Unknown searcher {searcher}")

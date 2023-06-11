import json
import shutil
from abc import ABC, abstractmethod
from logging import getLogger
from pathlib import Path
from typing import Literal

from annoy import AnnoyIndex  # type: ignore
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from whoosh.analysis import StemmingAnalyzer  # type: ignore
from whoosh.fields import ID, NUMERIC, TEXT, Schema  # type: ignore
from whoosh.index import create_in, open_dir  # type: ignore
from whoosh.qparser import QueryParser  # type: ignore

from config import config

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

    schema = Schema(
        id=ID(stored=True),
        title=TEXT(stored=True),
        author=TEXT(stored=False),
        rating=NUMERIC(stored=False),
        rating_count=NUMERIC(stored=False),
        review_count=NUMERIC(stored=False),
        description=TEXT(stored=True, analyzer=StemmingAnalyzer()),
    )

    def create_index(self, data_path: Path, index_dir: Path) -> None:
        """Creates a new index."""
        logger.info(f"Creating new index in {index_dir}")
        index = create_in(index_dir, self.schema)

        logger.info(f"Indexing data from {data_path}")

        # Read the JSON Lines file
        with index.writer() as writer:
            with open(data_path) as file:
                for line in file:
                    item = json.loads(line)
                    if "description" in item and item["description"]:
                        writer.add_document(
                            id=str(item["id"]),
                            title=item["title"],
                            author=item["author"],
                            rating=item["rating"],
                            rating_count=item["rating_count"],
                            review_count=item["review_count"],
                            description=item["description"],
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
        self,
        n_trees: int = 100,
        vector_size: int = 384,
        metric: Literal["angular", "euclidean"] = "angular",
    ) -> None:
        super().__init__()
        self.n_trees = n_trees
        self.vector_size = vector_size
        self.metric = metric
        self.model = SentenceTransformer(config.get("MODEL_NAME"))

    def create_index(self, data_path: Path, index_dir: Path) -> None:
        logger.info(f"Creating new index in {index_dir}")
        index = AnnoyIndex(self.vector_size, self.metric)

        # Read the JSON Lines file
        docs = {"id": [], "content": []}
        with open(data_path) as file:
            for line in file:
                item = json.loads(line)
                if "description" in item and item["description"]:
                    docs["id"].append(item["id"])
                    docs["content"].append(item["title"] + " " + item["description"])

        logger.info(f"Encoding {len(docs)} documents")
        embeddings = self.model.encode(docs["content"], show_progress_bar=True)

        for i, vector in tqdm(enumerate(embeddings), total=len(embeddings)):
            index.add_item(i, vector)

        logger.info(f"Building index with {self.n_trees} trees")
        index.build(self.n_trees)
        index.save(str(index_dir / "index.ann"))

    def clear_index(self, index_dir: Path) -> None:
        """Clears the index directory."""
        logger.info(f"Clearing index directory {index_dir}")
        shutil.rmtree(index_dir)


if __name__ == "__main__":
    # indexer = WhooshIndexer()
    # indexer.create_index(Path("data/books.jsonl"), Path("index"))

    indexer = AnnoyIndexer(vector_size=384, n_trees=5)
    indexer.create_index(Path("data/books.jsonl"), Path("index"))

from pathlib import Path

import pandas as pd
from whoosh.analysis import StemmingAnalyzer
from whoosh.fields import ID, STORED, TEXT, Schema
from whoosh.index import Index, create_in


def create_index(index_dir: Path) -> Index:
    schema = Schema(
        id=ID(stored=True),
        title=TEXT(stored=True),
        description=TEXT(analyzer=StemmingAnalyzer(), stored=True),
    )

    if not index_dir.exists():
        index_dir.mkdir()

    ix = create_in(index_dir, schema)
    return ix


def index_docs(ix: Index, data_file: Path) -> None:
    writer = ix.writer()
    data = pd.read_csv(data_file)
    for index, row in data.iterrows():
        if not pd.isna(row["description"]):
            writer.add_document(
                id=str(row["id"]), title=row["title"], description=row["description"]
            )
    writer.commit()


def main() -> None:
    DATA_DIR = Path("data")
    index_dir = DATA_DIR / "index"
    data_file = DATA_DIR / "books.csv"
    ix = create_index(index_dir)
    index_docs(ix, data_file)


if __name__ == "__main__":
    main()

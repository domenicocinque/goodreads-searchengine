from pathlib import Path

import pandas as pd

import config
from app import app
from app.service.dataset import BookDataset
from app.service.search_engine import Indexer
from args import get_parser


def build_index():
    print("Indexing started.")
    index_path: Path = config.INDEX_DIR
    data_path: Path = config.DATA_DIR
    data_filename = config.BOOK_DATA_FILENAME
    if not index_path.exists():
        raise Exception(f"Index directory {index_path} does not exist!")
    if not data_path.exists():
        raise Exception(f"Data directory {data_path} does not exist!")

    indexer = Indexer()
    indexer.create_index(index_path)
    indexer.index_dataframe(pd.read_csv(data_path / data_filename))
    print("Indexing completed.")


def setup_data():
    print("Starting data setup.")
    dataset = BookDataset(
        data_dir=config.DATA_DIR,
        raw_html_dir=config.RAW_HTML_DIR,
        book_list_file=config.BOOK_LIST_FILENAME,
        book_data_file=config.BOOK_DATA_FILENAME,
    )
    dataset.setup()
    print("Data setup completed.")


def main():
    parser = get_parser()
    args = parser.parse_args()
    if args.run:
        app.run()
    elif args.index:
        build_index()
    elif args.setup:
        setup_data()
    else:
        print("No command specified. Use --help to see available commands.")


if __name__ == "__main__":
    main()

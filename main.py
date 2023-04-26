from pathlib import Path

import pandas as pd

import config
from app import app
from app.service.dataset import BookDataset
from app.service.search_engine import Indexer
from args import get_parser


def build_index():
    """Clears the index directory and creates a new index."""
    print("Indexing started.")
    index_path: Path = config.INDEX_DIR
    data_path: Path = config.DATA_DIR
    data_filename = config.BOOK_DATA_FILENAME
    if not data_path.exists():
        raise Exception(f"Data directory {data_path} does not exist!")
    if not index_path.exists():
        raise Exception(f"Index directory {index_path} does not exist!")

    indexer = Indexer()
    indexer.clear_index(index_path)
    indexer.create_index(index_path)
    indexer.index_dataframe(pd.read_csv(data_path / data_filename))
    print("Indexing completed.")


def setup_data():
    """Performs the whole data setup process which includes:

    - downloading the book list
    - downloading the html files
    - parsing the html files
    - saving the parsed data to a csv file
    """
    print("Starting data setup.")
    dataset = BookDataset(
        num_pages=config.MAX_NUM_PAGES,
        data_dir=config.DATA_DIR,
        raw_html_dir=config.RAW_HTML_DIR,
        book_list_file=config.BOOK_LIST_FILENAME,
        book_data_file=config.BOOK_DATA_FILENAME,
    )
    dataset.setup()
    print("Data setup completed.")


def fix_data():
    """Tries to fix book with missing fields."""
    print("Starting data fix.")
    dataset = BookDataset(
        data_dir=config.DATA_DIR,
        raw_html_dir=config.RAW_HTML_DIR,
        book_list_file=config.BOOK_LIST_FILENAME,
        book_data_file=config.BOOK_DATA_FILENAME,
    )
    dataset.fix()
    print("Data fix completed.")


def scrape():
    """Performs only the scraping part of the data setup process.

    This is useful if we change the scraping logic and want to re-scrape the html files without
    having to re-download the book list.
    """
    print("Starting data scraping.")
    dataset = BookDataset(
        num_pages=config.MAX_NUM_PAGES,
        data_dir=config.DATA_DIR,
        raw_html_dir=config.RAW_HTML_DIR,
        book_list_file=config.BOOK_LIST_FILENAME,
        book_data_file=config.BOOK_DATA_FILENAME,
    )
    dataset.scrape()
    print("Data scraping completed.")


def main():
    parser = get_parser()
    args = parser.parse_args()
    if args.run:
        app.run()
    elif args.index:
        build_index()
    elif args.setup:
        setup_data()
    elif args.fix:
        fix_data()
    elif args.scrape:
        scrape()
    else:
        print("No command specified. Use --help to see available commands.")


if __name__ == "__main__":
    main()

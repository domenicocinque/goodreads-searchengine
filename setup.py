from argparse import ArgumentParser

import config
from app.core import BookDatasetBuilder
from app.core.search_engine import Indexer


def setup_data(force_redownload: bool = False):
    """Performs the data setup process"""
    print("Starting data setup.")
    dataset = BookDatasetBuilder(
        data_dir=config.DATA_DIR,
        book_list_file=config.BOOK_LIST_FILENAME,
        html_dirname=config.RAW_HTML_DIR,
        book_data_file=config.BOOK_DATA_FILENAME,
        max_page=config.MAX_NUM_PAGES
    )
    dataset.setup(force_redownload=force_redownload)
    print("Data setup completed.")

def setup_index():
    """Clears the index directory and creates a new index."""
    print("Indexing started.")
    index_path = config.INDEX_DIR
    data_path = config.DATA_DIR
    data_filename = config.BOOK_DATA_FILENAME
    data_file = data_path / data_filename

    if not data_path.exists():
        raise Exception(f"Data directory {data_path} does not exist!")
    if not index_path.exists():
        raise Exception(f"Index directory {index_path} does not exist!")

    indexer = Indexer(index_dir=index_path)
    indexer.setup(data_file=data_file)
    print("Indexing completed.")

def setup(force_redownload: bool = False):
    """Performs the whole setup process which includes:

    - downloading the book list
    - downloading the html files
    - parsing the html files
    - saving the parsed data to a csv file
    - building the search index

    Args:
        force_redownload: If True, the html files will be redownloaded. 
    """

    print("Starting setup.")
    setup_data(force_redownload)
    setup_index()
    print("Setup completed.")

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--force-redownload",
        action="store_true",
        help="Force redownload of the book list and html files.",
    )
    args = parser.parse_args()
    setup(force_redownload=args.force_redownload)
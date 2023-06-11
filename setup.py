from config import config
from data_processing.scrape import run_scraping
from data_processing.indexer import create_indexes
from argparse import ArgumentParser

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--scrape", action="store_true", help="Scrape the website."
    )
    parser.add_argument(
        "--index", action="store_true", help="Create the indexes."
    )
    parser.add_argument(
        "--all", action="store_true", help="Run all the steps."
    )
    args = parser.parse_args()
    if args.scrape or args.all:
        run_scraping(config)
    if args.index or args.all:
        create_indexes(config)

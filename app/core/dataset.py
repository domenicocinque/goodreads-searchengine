import random
import re
import time
from functools import wraps
from logging import getLogger
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from app.core import BOOK_LIST_URL, BOOK_PAGE_URL, Book
from app.core.scrape import scrape_book_data

logger = getLogger(__name__)


def delay(min_delay=1, max_delay=4):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            time.sleep(random.randint(min_delay, max_delay))
            return func(*args, **kwargs)

        return wrapper

    return decorator


def parse_book_id(id_string: str) -> tuple:
    pattern = r"(\d+)([-\.](\w+))?"
    match = re.match(pattern, id_string)

    if match:
        number = int(match.group(1))
        return number
    else:
        raise ValueError(f"Invalid ID format {id_string}")


def parse_book(book) -> Book:
    book_id = book.find("a", {"class": "bookTitle"})["href"].split("/")[-1]
    id_num = parse_book_id(book_id)
    book_url = f"{BOOK_PAGE_URL}/{book_id}.html"
    title = book.find("a", {"class": "bookTitle"}).text.strip()
    author = book.find("a", {"class": "authorName"}).text.strip()
    return Book(id=id_num, url=book_url, title=title, author=author)


class BookDatasetBuilder:
    """Class to build the dataset of books.

    Attributes:
        data_dir: The directory where the data is stored.
        book_list_file: The name of the file where the book list is stored.
        html_dirname: The name of the directory where the HTML files are stored.
        book_data_file: The name of the file where the book data is stored.
        max_page: The maximum number of pages to scrape.
    """

    base_url = BOOK_LIST_URL
    book_list = {"id_num": [], "id_name": [], "title": [], "author": []}

    def __init__(
        self,
        data_dir: Path,
        book_list_file: str,
        html_dirname: str,
        book_data_file: str,
        max_page: int,
    ):
        self.data_dir = data_dir
        self.book_list_file = self.data_dir / book_list_file
        self.html_dir = self.data_dir / html_dirname
        self.book_data_file = self.data_dir / book_data_file
        self.max_page = max_page

    @delay(min_delay=1, max_delay=4)
    def download_page(self, page_number):
        """Download and parse a single page."""
        response = requests.get(self.base_url + str(page_number), timeout=5)
        soup = BeautifulSoup(response.text, "html.parser")
        books_on_page = [
            parse_book(book)
            for book in soup.find_all("tr", {"itemtype": "http://schema.org/Book"})
        ]
        return books_on_page

    def download_book_list(self) -> None:
        """Download and save the book list."""
        logger.info("Downloading book list.")
        books = []  # List to store the book data
        for page in range(1, self.max_page + 1):
            print(f"Processing page {page}...")
            books_on_page = self.download_page(page)
            books.extend(books_on_page)

        # Convert list of data class instances to a dataframe and save to CSV
        df = pd.DataFrame([dict(book) for book in books])
        df.to_csv(self.book_list_file, index=False)

    @delay(min_delay=1, max_delay=4)
    def download_single_html(self, url: str, filename: str) -> None:
        """Download the webpage of a single book."""
        try:
            response = requests.get(url, timeout=5)
        except requests.exceptions.Timeout:
            logger.warning(f"Request for {url} timed out. Skipping...")
            return
        with open(filename, "w", encoding="utf-8") as f:
            f.write(response.text)

    def download_all_html(self, force: bool = False) -> None:
        """Download the webpages of all books in the book list."""
        logger.info("Downloading webpages...")
        book_list = pd.read_csv(self.book_list_file)
        if not self.html_dir.exists():
            self.html_dir.mkdir()

        with tqdm(total=len(book_list)) as pbar:
            for index, row in book_list.iterrows():
                book_id = row["url"].split("/")[-1]
                html_filename = self.html_dir / book_id
                if html_filename.exists() and not force:
                    continue
                self.download_single_html(row["url"], filename=html_filename)
                pbar.update(1)

    def download(self, force: bool = False) -> None:
        # self.download_book_list()
        self.download_all_html(force=force)
        logger.info("Data downloaded.")

    def scrape_html_dir(self) -> None:
        """Scrape the HTML files in the HTML directory."""
        logger.info("Scraping HTML files.")
        books = []
        with tqdm(total=len(list(self.html_dir.glob("*.html")))) as pbar:
            for html_file in self.html_dir.glob("*.html"):
                book = scrape_book_data(html_file)
                books.append(dict(book))
                pbar.update(1)
        df = pd.DataFrame(books)
        df.to_csv(self.book_data_file, index=False)
        logger.info("Scraping complete.")

    def setup(self, force_redownload: bool = False) -> None:
        """Setup the dataset."""
        self.download(force=force_redownload)
        self.scrape_html_dir()
        logger.info("Dataset setup complete.")

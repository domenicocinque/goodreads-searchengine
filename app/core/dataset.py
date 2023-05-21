import random
import time
from dataclasses import asdict
from logging import getLogger
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from app.core.scrape import scrape_book_data

logger = getLogger(__name__)


class BookDatasetBuilder:
    """Class to build the dataset of books.

    Attributes:
        data_dir: The directory where the data is stored.
        html_dirname: The name of the directory where the HTML files are stored.
        book_list_file: The name of the file where the book list is stored.
        max_page: The maximum number of pages to scrape.
    """

    base_url = "https://www.goodreads.com/list/show/1.Best_Books_Ever?page="

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

    def download_book_list(self) -> None:
        """Get the list of books from the Goodreads website and save it to a CSV file."""
        logger.info("Downloading book list.")
        book_list = {"id": [], "title": [], "author": []}
        for page in range(1, self.max_page + 1):
            print(f"Processing page {page}...")
            response = requests.get(self.base_url + str(page), timeout=5)
            soup = BeautifulSoup(response.text, "html.parser")
            for book in soup.find_all("tr", {"itemtype": "http://schema.org/Book"}):
                book_list["id"].append(
                    book.find("a", {"class": "bookTitle"})["href"].split("/")[-1]
                )
                book_list["title"].append(book.find("a", {"class": "bookTitle"}).text.strip())
                book_list["author"].append(book.find("a", {"class": "authorName"}).text.strip())
            time.sleep(random.randint(1, 4))
        df = pd.DataFrame(book_list)
        df.to_csv(self.book_list_file, index=False)

    def download_single_html(self, id: str, force: bool = False) -> None:
        """Download the webpage of a single book.

        The force parameter can be used to force the download even if the file already exists. This
        is useful if the file is corrupted or if the webpage has changed since the last download.
        """

        # Check if the webpage has already been downloaded
        html_file = self.html_dir / (id + ".html")
        if html_file.exists() and not force:
            return
        try:
            response = requests.get("https://www.goodreads.com/book/show/" + id, timeout=5)
        except requests.exceptions.Timeout:
            logger.warning(f"Request for book {id} timed out. Skipping...")
            return
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(response.text)
        # Sleep for random seconds to avoid being blocked
        time.sleep(random.randint(1, 4))

    def download_all_html(self, force: bool = False) -> None:
        """Download the webpages of all books in the book list."""
        logger.info("Downloading webpages...")
        book_list = pd.read_csv(self.book_list_file)
        if not self.html_dir.exists():
            self.html_dir.mkdir()

        with tqdm(total=len(book_list)) as pbar:
            for index, row in book_list.iterrows():
                self.download_single_html(row["id"], force)
                pbar.update(1)

    def download(self, force: bool = False) -> None:
        self.download_book_list()
        self.download_all_html(force=force)
        logger.info("Data downloaded.")

    def scrape_html_dir(self) -> None:
        """Scrape the HTML files in the HTML directory."""
        logger.info("Scraping HTML files.")
        books = []
        for html_file in self.html_dir.glob("*.html"):
            book = scrape_book_data(html_file)
            books.append(asdict(book))
        df = pd.DataFrame(books)
        df.to_csv(self.book_data_file, index=False)
        logger.info("Scraping complete.")

    def setup(self, force_redownload: bool = False) -> None:
        """Setup the dataset."""
        self.download(force=force_redownload)
        self.scrape_html_dir()
        logger.info("Dataset setup complete.")

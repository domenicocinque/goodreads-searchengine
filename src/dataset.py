import logging
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import nltk
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from scipy.sparse import load_npz, save_npz
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_book_list(max_page: int) -> pd.DataFrame:
    """Build a DataFrame from the book list."""
    base_url = "https://www.goodreads.com/list/show/1.Best_Books_Ever?page="
    book_list = {"id": [], "title": [], "author": []}
    for page in range(1, max_page + 1):
        print(f"Processing page {page}...")
        response = requests.get(base_url + str(page), timeout=5)
        soup = BeautifulSoup(response.text, "html.parser")
        for book in soup.find_all("tr", {"itemtype": "http://schema.org/Book"}):
            book_list["id"].append(book.find("a", {"class": "bookTitle"})["href"].split("/")[-1])
            book_list["title"].append(book.find("a", {"class": "bookTitle"}).text.strip())
            book_list["author"].append(book.find("a", {"class": "authorName"}).text.strip())
        # Sleep for 2 second to avoid being blocked
        time.sleep(2)
    return pd.DataFrame(book_list)


def get_title(soup: BeautifulSoup) -> str:
    if soup.find("h1", {"class": "Text Text__title1"}):
        return soup.find("h1", {"class": "Text Text__title1"}).text.strip()
    else:
        return None


def get_description(soup: BeautifulSoup) -> str:
    if soup.find("div", {"class": "DetailsLayoutRightParagraph__widthConstrained"}):
        return soup.find(
            "div", {"class": "DetailsLayoutRightParagraph__widthConstrained"}
        ).text.strip()
    else:
        return None


def get_rating(soup: BeautifulSoup) -> float:
    if soup.find("div", {"class": "RatingStatistics__rating"}):
        return float(soup.find("div", {"class": "RatingStatistics__rating"}).text)
    else:
        return None


def get_rating_count(soup: BeautifulSoup) -> int:
    if soup.find("span", {"data-testid": "ratingsCount"}):
        return int(
            soup.find("span", {"data-testid": "ratingsCount"})
            .text.replace("\xa0", " ")
            .split(" ")[0]
            .replace(",", "")
        )
    else:
        return None


def get_review_count(soup: BeautifulSoup) -> int:
    if soup.find("span", {"data-testid": "reviewsCount"}):
        return int(
            soup.find("span", {"data-testid": "reviewsCount"})
            .text.replace("\xa0", " ")
            .split(" ")[0]
            .replace(",", "")
        )
    else:
        return None


@dataclass
class Book:
    id: str
    title: str
    description: str

    def to_dict(self) -> dict:
        return asdict(self)


def scrape_book_data(book_id: str, html_file: Path) -> Book:
    with open(html_file, encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")
    return Book(id=book_id, title=get_title(soup), description=get_description(soup))


class BookDataset:
    NUM_PAGES = 3
    book_list_file = "book_list.csv"
    raw_html_dir = "raw_html"
    book_data_file = "book_data.csv"

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.raw_html_dir = self.data_dir / self.raw_html_dir
        self.book_list_file = self.data_dir / self.book_list_file
        self.book_data_file = self.data_dir / self.book_data_file

        self._setup()

    def download_data_list(self) -> None:
        # Check if the book list already exists
        if not self.book_list_file.exists():
            logger.info("Downloading book list...")
            book_list = get_book_list(self.NUM_PAGES)
            book_list.to_csv(self.book_list_file, index=False)

    def download_raw_html(self) -> None:
        # Check if the webpages have already been downloaded
        if not self.raw_html_dir.exists():
            logger.info("Downloading webpages...")
            self.raw_html_dir.mkdir()
            """Download book webpages."""
            book_list = pd.read_csv(self.book_list_file)
            with tqdm(total=len(book_list)) as pbar:
                for index, row in book_list.iterrows():
                    response = requests.get(
                        "https://www.goodreads.com/book/show/" + row["id"], timeout=5
                    )
                    with open(self.raw_html_dir + row["id"] + ".html", "w", encoding="utf-8") as f:
                        f.write(response.text)
                    # Sleep for 2 second to avoid being blocked
                    time.sleep(2)
                    pbar.update(1)

    def download(self) -> None:
        self.download_data_list()
        self.download_raw_html()
        logger.info("Data downloaded.")

    def scrape(self) -> None:
        # Check if the book data has been scraped
        if not self.book_data_file.exists():
            logger.info("Scraping book data...")
            book_list = pd.read_csv(self.book_list_file)
            book_data_list = []
            with tqdm(total=len(book_list)) as pbar:
                for index, row in book_list.iterrows():
                    html_file = self.raw_html_dir / (row["id"] + ".html")
                    if html_file.exists():
                        book_data_list.append(scrape_book_data(row["id"], html_file))
                    pbar.update(1)
            book_data_df = pd.DataFrame(book_data_list)
            book_data_df.to_csv(self.book_data_file, index=False)
        logger.info("Book data scraped.")

    def _setup(self) -> None:
        self.download()
        self.scrape()
        logger.info("Setup complete")

    def load_dataset(self) -> pd.DataFrame:
        return pd.read_csv(self.book_data_file)

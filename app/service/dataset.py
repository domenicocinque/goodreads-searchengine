import random
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from app.logger import get_logger

logger = get_logger(__name__)


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
        time.sleep(random.randint(1, 4))
    return pd.DataFrame(book_list)


def get_title(soup: BeautifulSoup) -> str:
    if soup.find("h1", {"class": "Text Text__title1"}):
        return soup.find("h1", {"class": "Text Text__title1"}).text.strip()
    else:
        return None


def get_author(soup: BeautifulSoup) -> str:
    if soup.find("span", {"class": "ContributorLink__name"}):
        return soup.find("span", {"class": "ContributorLink__name"}).text.strip()
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
    author: str
    rating: Optional[float]
    rating_count: Optional[int]
    review_count: Optional[int]
    description: Optional[str]

    def to_dict(self) -> dict:
        return asdict(self)


def scrape_book_data(book_id: str, html_file: Path) -> Book:
    with open(html_file, encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")
    return Book(
        id=book_id,
        title=get_title(soup),
        description=get_description(soup),
        author=get_author(soup),
        rating=get_rating(soup),
        rating_count=get_rating_count(soup),
        review_count=get_review_count(soup),
    )


class BookDataset:
    def __init__(
        self,
        num_pages: int,
        data_dir: Path,
        raw_html_dir: str,
        book_list_file: str,
        book_data_file: str,
    ):
        self.num_pages = num_pages
        self.data_dir = data_dir
        self.raw_html_dir = self.data_dir / raw_html_dir
        self.book_list_file = self.data_dir / book_list_file
        self.book_data_file = self.data_dir / book_data_file

    def download_data_list(self) -> None:
        """Download the list of books from the website."""
        logger.info("Downloading book list...")
        book_list = get_book_list(self.num_pages)
        book_list.to_csv(self.book_list_file, index=False)

    def download_page(self, id: str, force: bool = False) -> None:
        """Download the webpage of a single book."""
        # Check if the webpage has already been downloaded
        html_file = self.raw_html_dir / (id + ".html")
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

    def download_raw_html(self) -> None:
        """Download the webpages of all books in the book list."""
        logger.info("Downloading webpages...")
        book_list = self.load_data_list()
        if not self.raw_html_dir.exists():
            self.raw_html_dir.mkdir()

        with tqdm(total=len(book_list)) as pbar:
            for index, row in book_list.iterrows():
                self.download_page(row["id"])
                pbar.update(1)

    def download(self) -> None:
        self.download_data_list()
        self.download_raw_html()
        logger.info("Data downloaded.")

    def scrape(self) -> None:
        logger.info("Scraping book data...")
        book_list = self.load_data_list()
        book_data = []
        with tqdm(total=len(book_list)) as pbar:
            for index, row in book_list.iterrows():
                html_file = self.raw_html_dir / (row["id"] + ".html")
                if html_file.exists():
                    book_data.append(scrape_book_data(row["id"], html_file))
                pbar.update(1)
        book_data_df = pd.DataFrame(book_data)
        book_data_df.to_csv(self.book_data_file, index=False)
        logger.info("Book data scraped.")

    def setup(self) -> None:
        self.download()
        self.scrape()
        logger.info("Setup complete")

    def fix(self) -> None:
        """Fix the corrupted rows in the dataset."""
        book_list = self.load_data_list()

        corrupted_rows = book_list[book_list["description"].isnull()]
        count_corrupted_rows = len(corrupted_rows)
        if count_corrupted_rows == 0:
            logger.info("No corrupted rows found.")
        else:
            logger.info(f"Trying to fix {count_corrupted_rows} corrupted rows...")
            count_fixed_rows = 0
            for index, row in corrupted_rows.iterrows():
                # Download the page again
                self.download_page(row["id"], force=True)
                html_file = self.raw_html_dir / (row["id"] + ".html")
                book_data = scrape_book_data(row["id"], html_file)
                if book_data.description is not None and book_data.title is not None:
                    book_list.loc[index, "title"] = book_data.title
                    book_list.loc[index, "description"] = book_data.description
                    count_fixed_rows += 1
                    logger.info(f"Fixed book_id = {row['id']}")
                else:
                    logger.info(f"Could not fix book_id: {row['id']}")
            logger.info(
                f"Fixed {count_fixed_rows/count_corrupted_rows*100:.2f}% of corrupted rows."
            )
            book_list.to_csv(self.book_data_file, index=False)

    def load_data_list(self) -> pd.DataFrame:
        return pd.read_csv(self.book_list_file)

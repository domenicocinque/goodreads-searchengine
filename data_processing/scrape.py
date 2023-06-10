import asyncio
import json
import logging
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup, NavigableString, Tag
from rich import print

logger = logging.getLogger("Scraping")

MAX_RETRIES = 5
BOOK_PAGE_URL = "https://www.goodreads.com/"
BOOK_LIST_URL = "https://www.goodreads.com/list/show/1.Best_Books_Ever?page="
DATA_PATH = Path(__file__).parent.parent / "data" / "books.json"


@dataclass
class Book:
    id: int
    title: str
    author: str
    rating: float
    rating_count: int
    review_count: int
    description: str
    url: str


def get_title(soup: BeautifulSoup) -> Optional[str]:
    return soup.find("h1", {"class": "Text Text__title1"}).text.strip()


def get_author(soup: BeautifulSoup) -> Optional[str]:
    author_element = soup.find("span", {"class": "ContributorLink__name"}).text.strip()


def get_description(soup: BeautifulSoup) -> Optional[str]:
    return soup.find(
        "div", {"class": "DetailsLayoutRightParagraph__widthConstrained"}
    ).text.strip()


def get_rating(soup: BeautifulSoup) -> Optional[float]:
    return float(soup.find("div", {"class": "RatingStatistics__rating"}).text.strip())


def parse_count(element: Tag | NavigableString) -> int:
    return int(element.text.replace("\xa0", " ").split(" ")[0].replace(",", "").strip())


def get_rating_count(soup: BeautifulSoup) -> Optional[int]:
    return parse_count(soup.find("span", {"data-testid": "ratingsCount"}))


def get_review_count(soup: BeautifulSoup) -> Optional[int]:
    return parse_count(soup.find("span", {"data-testid": "reviewsCount"}))


def get_book_urls(limit: int) -> list[str]:
    url_list = []

    with httpx.Client() as client:
        for page in range(1, limit + 1):
            response = client.get(BOOK_LIST_URL + str(page))
            soup = BeautifulSoup(response.text, "html.parser")
            book_list = soup.find_all("a", {"class": "bookTitle"})
            url_list.extend([urljoin(BOOK_PAGE_URL, book["href"]) for book in book_list])

    return url_list


async def get_book_data(session: httpx.Client, url: str) -> Book:
    for _ in range(MAX_RETRIES):
        try:
            response = await session.get(url, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")
            return Book(
                id=int(re.search(r"\d+", url).group()),
                title=get_title(soup),
                author=get_author(soup),
                rating=get_rating(soup),
                rating_count=get_rating_count(soup),
                review_count=get_review_count(soup),
                description=get_description(soup),
                url=url,
            )
        except (httpx.ReadTimeout, AttributeError) as e:
            print(f"Error parsing book {url}: {str(e)}")

async def run_scraper(url_list: list[str]) -> list[Book]:
    async with httpx.AsyncClient() as session:
        tasks = [get_book_data(session, url) for url in url_list]
        results = await asyncio.gather(*tasks)
        return [result for result in results if result is not None]


def main():
    existing_books = set()

    # Check if the books.json file exists
    if DATA_PATH.exists():
        with open(DATA_PATH) as f:
            for line in f:
                book = json.loads(line)
                existing_books.add(book["url"])
    print(f"Found {len(existing_books)} existing books")

    # Get the book urls
    book_list = set(get_book_urls(1))
    book_list -= existing_books
    print(f"Scraping {len(book_list)} books")

    # Scrape the book data
    books = asyncio.run(run_scraper(book_list))
    print(f"Scraped {len(books)} books")

    # Append the new books to the existing ones
    with open(DATA_PATH, "a") as f:
        for book in books:
            f.write(json.dumps(asdict(book)) + "\n")


if __name__ == "__main__":
    main()

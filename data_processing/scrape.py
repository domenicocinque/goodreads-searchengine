import logging
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import httpx
from bs4 import BeautifulSoup, NavigableString, Tag

logger = logging.getLogger("Scraping")

BOOK_PAGE_URL = "https://www.goodreads.com/book/show"


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
    title_element = soup.find("h1", {"class": "Text Text__title1"})
    return title_element.text.strip() if title_element else None


def get_author(soup: BeautifulSoup) -> Optional[str]:
    author_element = soup.find("span", {"class": "ContributorLink__name"})
    return author_element.text.strip() if author_element else None


def get_description(soup: BeautifulSoup) -> Optional[str]:
    description_element = soup.find(
        "div", {"class": "DetailsLayoutRightParagraph__widthConstrained"}
    )
    return description_element.text.strip() if description_element else None


def get_rating(soup: BeautifulSoup) -> Optional[float]:
    rating_element = soup.find("div", {"class": "RatingStatistics__rating"})
    return float(rating_element.text) if rating_element else None


def parse_count(element: Tag | NavigableString) -> int:
    return int(element.text.replace("\xa0", " ").split(" ")[0].replace(",", ""))


def get_rating_count(soup: BeautifulSoup) -> Optional[int]:
    rating_count_element = soup.find("span", {"data-testid": "ratingsCount"})
    return parse_count(rating_count_element) if rating_count_element else None


def get_review_count(soup: BeautifulSoup) -> Optional[int]:
    review_count_element = soup.find("span", {"data-testid": "reviewsCount"})
    return parse_count(review_count_element) if review_count_element else None


def scrape_book_data(soup: BeautifulSoup) -> dict:
    title = get_title(soup)
    description = get_description(soup)
    author = get_author(soup)
    rating = get_rating(soup)
    rating_count = get_rating_count(soup)
    review_count = get_review_count(soup)

    return dict(
        title=title,
        author=author,
        rating=rating,
        rating_count=rating_count,
        review_count=review_count,
        description=description,
    )


def parse_book_basic_info(book) -> tuple[int, str]:
    id_string = book.find("a", {"class": "bookTitle"})["href"].split("/")[-1]
    pattern = r"(\d+)([-\.](\w+))?"
    match = re.match(pattern, id_string)

    if match:
        number = int(match.group(1))
        return number, id_string
    else:
        raise ValueError(f"Invalid ID format {id_string}")


def get_book_data(session: httpx.Client, book_info: tuple[int, str]) -> Book:
    book_id, url_postfix = book_info
    book_url = BOOK_PAGE_URL + "/" + url_postfix

    response = session.get(book_url)
    soup = BeautifulSoup(response.text, "html.parser")
    book_data = scrape_book_data(soup)
    book_data["id"] = book_id
    book_data["url"] = book_url
    book = Book(**book_data)
    print(f"Scraped {book.title}, {book.url}")
    return book


def get_book_list(limit: int) -> list[tuple[int, str]]:
    start = "https://www.goodreads.com/list/show/1.Best_Books_Ever?page="

    book_list = []

    client = httpx.Client()
    for page in range(1, limit + 1):
        response = client.get(start + str(page))
        soup = BeautifulSoup(response.text, "html.parser")
        books_on_page = [
            parse_book_basic_info(book)
            for book in soup.find_all("tr", {"itemtype": "http://schema.org/Book"})
        ]
        book_list.extend(books_on_page)

    client.close()

    return book_list


def run_scraper(limit: int) -> None:
    book_list = get_book_list(limit)
    print(f"Scraping {len(book_list)} books")

    with httpx.Client() as client:
        books = [get_book_data(client, book_info) for book_info in book_list]

    Path("data").mkdir(exist_ok=True)
    with open("data/books.json", "w") as f:
        f.write("[")
        for book in books:
            f.write(str(asdict(book)) + ",\n")
        f.write("]")

    print("Done!")


if __name__ == "__main__":
    run_scraper(1)

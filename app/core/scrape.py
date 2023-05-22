from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup, NavigableString, Tag

from app.core import BOOK_PAGE_URL, Book


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


def scrape_book_data(html_file: Path) -> Book:
    with open(html_file, encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    book_id = html_file.name.replace(".html", "")
    url = f"{BOOK_PAGE_URL}/{book_id}"
    title = get_title(soup)
    description = get_description(soup)
    author = get_author(soup)
    rating = get_rating(soup)
    rating_count = get_rating_count(soup)
    review_count = get_review_count(soup)

    return Book(
        id=book_id,
        title=title,
        author=author,
        url=url,
        rating=rating,
        rating_count=rating_count,
        review_count=review_count,
        description=description,
    )

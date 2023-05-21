from typing import Optional
from pathlib import Path
from dataclasses import dataclass, asdict
from bs4 import BeautifulSoup

@dataclass
class Book:    
    id: str
    title: str
    author: str
    rating: Optional[float]
    rating_count: Optional[int]
    review_count: Optional[int]
    description: Optional[str]

    def to_dict(self) -> dict: return asdict(self)
    
def get_title(soup) -> str:
    title_element = soup.find("h1", {"class": "Text Text__title1"})
    return title_element.text.strip() if title_element else None

def get_author(soup) -> str:
    author_element = soup.find("span", {"class": "ContributorLink__name"})
    return author_element.text.strip() if author_element else None

def get_description(soup) -> str:
    description_element = soup.find("div", {"class": "DetailsLayoutRightParagraph__widthConstrained"})
    return description_element.text.strip() if description_element else None

def get_rating(soup) -> float:
    rating_element = soup.find("div", {"class": "RatingStatistics__rating"})
    return float(rating_element.text) if rating_element else None

def parse_count(element): return int(element.text.replace("\xa0", " ").split(" ")[0].replace(",", ""))

def get_rating_count(soup) -> int:
    rating_count_element = soup.find("span", {"data-testid": "ratingsCount"})
    return parse_count(rating_count_element) if rating_count_element else None

def get_review_count(soup) -> int:
    review_count_element = soup.find("span", {"data-testid": "reviewsCount"})
    return parse_count(review_count_element) if review_count_element else None

def scrape_book_data(html_file: Path) -> Book:
    with open(html_file, encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    book_id = html_file.name.replace(".html", "")
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
        rating=rating,
        rating_count=rating_count,
        review_count=review_count,
        description=description,
    )

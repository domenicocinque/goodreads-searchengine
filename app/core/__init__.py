from dataclasses import asdict, dataclass
from typing import Optional

BOOK_PAGE_URL = "https://www.goodreads.com/book/show"
BOOK_LIST_URL = "https://www.goodreads.com/list/show/1.Best_Books_Ever?page="


@dataclass
class Book:
    id: str
    url: str
    title: Optional[str] = None
    author: Optional[str] = None
    rating: Optional[float] = None
    rating_count: Optional[int] = None
    review_count: Optional[int] = None
    description: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)

    def __iter__(self):
        return iter(self.to_dict().items())

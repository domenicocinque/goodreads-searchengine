import asyncio
import re
from time import perf_counter

import httpx
from bs4 import BeautifulSoup


def get_book_id(book) -> tuple[int, str]:
    href = book.find("h3").find("a")["href"]
    return href


def sync_get_book_list(limit: int) -> list[tuple[int, str]]:
    start = "https://books.toscrape.com/catalogue/page-{}.html"
    book_list = []
    for page in range(1, limit + 1):
        response = httpx.get(start.format(page))
        soup = BeautifulSoup(response.text, "html.parser")
        books_on_page = [
            get_book_id(book) for book in soup.find_all("article", {"class": "product_pod"})
        ]
        book_list.extend(books_on_page)
    return book_list


async def get_book_id_async(book) -> tuple[int, str]:
    href = book.find("h3").find("a")["href"]
    return href


async def async_get_book_list(limit: int) -> list[tuple[int, str]]:
    start = "https://books.toscrape.com/catalogue/page-{}.html"
    book_list = []
    async with httpx.AsyncClient() as client:
        for page in range(1, limit + 1):
            response = await client.get(start.format(page))
            soup = BeautifulSoup(response.text, "html.parser")
            books_on_page = [
                await get_book_id_async(book)
                for book in soup.find_all("article", {"class": "product_pod"})
            ]
            book_list.extend(books_on_page)
    return book_list


if __name__ == "__main__":
    perf = perf_counter()
    book_list = sync_get_book_list(10)
    print(f"Time taken (sync): {perf_counter() - perf}")

    perf = perf_counter()
    asyncio.run(async_get_book_list(10))
    print(f"Time taken (async): {perf_counter() - perf}")

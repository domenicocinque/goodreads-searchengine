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


def scrape_book_data(soup: BeautifulSoup) -> dict:
    try:
        title = soup.find("h1").text
    except AttributeError:
        title = None
    try:
        description = soup.find("div", {"id": "product_description"}).find_next("p").text
    except AttributeError:
        description = None

    return dict(
        title=title,
        description=description,
    )


async def get_book_data(session: httpx.AsyncClient, url_postfix: str) -> dict:
    book_url = "https://books.toscrape.com/catalogue/" + url_postfix

    print(f"Scraping {book_url}")
    response = await session.get(book_url)
    soup = BeautifulSoup(response.text, "html.parser")
    book_data = scrape_book_data(soup)
    book_data["url"] = book_url
    return book_data


async def scrape(book_list: list[tuple[int, str]]) -> list[dict]:
    async with httpx.AsyncClient() as session:
        tasks = [get_book_data(session, book) for book in book_list]
        return await asyncio.gather(*tasks)


if __name__ == "__main__":
    # perf = perf_counter()
    # book_list = sync_get_book_list(10)
    # print(f"Time taken (sync): {perf_counter() - perf}")

    book_list = asyncio.run(async_get_book_list(2))

    result = asyncio.run(scrape(book_list))
    print(result)

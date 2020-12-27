from asyncio import Queue
from typing import List

import aiohttp
import bs4

from db import DB
from helpers import Link, Page, URL


class Crawler:
    def __init__(self, db: DB, initial_url: str):
        self.db = db
        self.url_queue = Queue()
        self.url_queue.put_nowait(initial_url)

    async def crawl(self, page_queue: Queue, link_queue: Queue):
        while True:
            print('Пополз')
            next_url = await self.url_queue.get()
            print('Пополз дальше')

            async with aiohttp.ClientSession() as session:
                async with session.get(next_url) as response:
                    doc = bs4.BeautifulSoup(await response.text(encoding='utf8'), 'html.parser')
                    [s.extract() for s in doc(['style', 'script', '[document]', 'head', 'title'])]

            url = await self.get_url_obj(next_url)
            page_queue.put_nowait(Page(url, doc.getText()))

            for link in (await self.load_urls(doc, url)):
                self.url_queue.put_nowait(link.next_url.url)
                await link_queue.put(link)

    async def load_urls(self, doc: bs4.BeautifulSoup, url: URL) -> List[Link]:
        result: List[Link] = []
        next_url: str

        for tag_a in doc.find_all('a'):
            try:
                if (next_url := tag_a.attrs['href']).startswith('http'):
                    result.append(Link(prev_url=url, next_url=await self.get_url_obj(next_url), text=tag_a.text))
            except KeyError:
                print('У тэга "a" не обнаружен атрибут href, пропускаю')

        return result

    async def get_url_obj(self, url: str) -> URL:
        id_: int = await self.db.query_scalar(
            """
            select id
            from url
            where url = $1::INT
            """,
            url
        )
        if id_ is None:
            id_ = await self.db.query_scalar(
                """
                insert into url (url)
                select $1::INT
                returning id
                """,
                url
            )
        return URL(id_, url)

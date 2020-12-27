import asyncio
import re
import string
from asyncio import Queue
from typing import Dict, List, Optional, Tuple

from db import DB
from helpers import Link, Page, URL, get_link_id


class Indexer:
    def __init__(self, db):
        self.db: DB = db

    async def index_pages(self, page_queue: Queue):
        """
        Индексировать страницы.

        :param page_queue: Очередь страниц на индексацию.
        """
        while True:
            print('Жду страницы для индексации')
            page: Page = await page_queue.get()
            print(f'Индексирую страницу {page.url.url}')

            if await self.is_page_indexed(page.url):
                print(f'Страница {page.url.url} уже проиндексирована')
                continue

            words, analytics, location = await self.get_words(page.text)
            await self.index_words(words, analytics, page.url, location)

            await self.db.query(
                """
                update url
                set is_indexed=true
                where id = $1::int
                """,
                page.url.id
            )
            print(f'Закончил индексировать {page.url.url}')

    async def index_links(self, link_queue: Queue):
        """
        Индексировать ссылки.

        :param link_queue: Очередь ссылок на индексацию.
        """
        while True:
            link: Link = await link_queue.get()
            print(f'Индексирую ссылку с текстом "{link.text}" на страницу {link.next_url.url}')

            if self.is_link_indexed(link):
                continue

            link_id = await get_link_id(link, self.db)

            words, analytics, location = self.get_words(link.text)
            await self.index_words(words, analytics, link.prev_url, location, link_id)

    async def index_words(
            self, words: List[str], analytics: List[bool], url: URL, location: List[int], link_id: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Проиндексировать слова.

        :param words: Список слов.
        :param analytics: Список аналитик слов. Согласован с words.
        :param url: URL страницы, на которой находятся слова.
        :param location: Порядок слов на странице. Согласован с words.
        :param link_id: Идентификатор ссылки, которой принадлежат слова.
        :return: Отображение {слово: идентификатор}
        """

        # Добавить новые слова в индекс. Если такие слова уже существуют, вернуть их идентификатор.
        word_ids = await self.db.query(
            """
            with inserted as (
                insert into word (word, analytics)
                (
                    select
                        unnest($1::text[]) as word,
                        unnest($2::bool[]) as analytics
                )
                on conflict do nothing
                returning word, id
            )
            select * from inserted

            union

            select word.word, id from word
            where word.word = any($1::text[])
            """,
            words,
            analytics
        )
        word_locations = dict(zip(words, location))
        word_ids = {rec.get('word'): rec.get('id') for rec in word_ids}

        # Записать информацию о том, какой странице принадлежат слова.
        await self.db.query(
            """
            insert into word_to_url (word_id, location, url_id)
            (
                select
                    unnest($1::INT[]) as word_id,
                    unnest($2::INT[]) as location,
                    $3::int as url_id
            )
            """,
            [word_ids[word] for word in words],
            [word_locations[word] for word in words],
            url.id
        )

        # Если слова составляли ссылку на другую страницу, сохранить информацию об этом.
        if link_id:
            await self.db.query(
                """
                insert into word_to_link (word_id, link_id)
                (
                    select
                        unnest($1::INT[]) as word_id,
                        $2::int as url_id
                )
                """,
                word_ids.values(),
                link_id
            )

        return word_ids

    @staticmethod
    async def get_words(text) -> Tuple[List[str], List[bool], List[int]]:
        words = re.sub('[' + string.punctuation + ']', '', text).split()
        analytics = []

        for word in words:
            alphabetic_only = set(filter(lambda letter: letter.isalpha(), word))
            analytics.append(
                bool(alphabetic_only) and alphabetic_only.issubset({chr(sym) for sym in range(1040, 1104)})
            )

        location = list(range(len(words)))
        return words, analytics, location

    async def is_page_indexed(self, url: URL) -> bool:
        is_indexed = await self.db.query_scalar(
            """
            select is_indexed from url where id = $1::INT
            """,
            url.id
        )
        return is_indexed

    async def is_link_indexed(self, link: Link) -> bool:
        is_indexed = await self.db.query_scalar(
            """
            select exists(select null from url_to_url where prev_url_id = $1::INT and next_url_id = $2::INT)
            """,
            link.prev_url.id,
            link.next_url.id
        )
        return is_indexed

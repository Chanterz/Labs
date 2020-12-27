import asyncio
from asyncio import Queue

from db import DB
from indexer import Indexer
from crawler import Crawler

if __name__ == '__main__':
    page_indexers_count = 5
    link_indexers_count = 2

    page_queue = Queue()
    link_queue = Queue()
    db = DB()
    db.init_db()
    loop = asyncio.get_event_loop()
    tasks = [
                loop.create_task(Crawler(db, 'https:\\ru.wikipedia.org').crawl(page_queue, link_queue))
            ] + [
                loop.create_task(Indexer(db).index_pages(page_queue)) for _ in range(page_indexers_count)
            ] + [
                loop.create_task(Indexer(db).index_links(link_queue)) for _ in range(link_indexers_count)
            ]
    loop.run_forever()
    loop.close()

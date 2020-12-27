import asyncio

import asyncpg


class DB:
    def __init__(self):
        try:
            loop = asyncio.get_event_loop()
        except:
            loop = asyncio.get_running_loop()

        self.pool = loop.run_until_complete(
            asyncpg.create_pool(
                host='localhost',
                port=5432,
                database='crawler',
                user='postgres',
                password='postgres'
            )
        )

    def init_db(self):
        asyncio.get_event_loop().run_until_complete(self.query("""
            DROP TABLE IF EXISTS word CASCADE;
            CREATE TABLE word (
                id INT GENERATED ALWAYS AS IDENTITY,
                word TEXT,
                analytics BOOLEAN,
                UNIQUE(word),
                UNIQUE(id)
            );
            CREATE INDEX words ON word (word);
            DROP TABLE IF EXISTS url CASCADE;
            CREATE TABLE url (
                id INT GENERATED ALWAYS AS IDENTITY,
                url TEXT,
                is_indexed BOOLEAN,
                UNIQUE(url),
                UNIQUE(id)
            );
            CREATE INDEX urls ON url (url);

            DROP TABLE IF EXISTS word_to_url CASCADE;
            CREATE TABLE word_to_url(
               id INT GENERATED ALWAYS AS IDENTITY,
               word_id INT,
               url_id INT,
               location INT,
               PRIMARY KEY(id),
               CONSTRAINT fk_word
                   FOREIGN KEY(word_id)
                   REFERENCES word(id),
               CONSTRAINT fk_url
                   FOREIGN KEY(url_id)
                   REFERENCES url(id),
            UNIQUE(id)
            );
            DROP TABLE IF EXISTS url_to_url CASCADE;
            CREATE TABLE url_to_url(
               id INT GENERATED ALWAYS AS IDENTITY,
               prev_url_id INT,
               next_url_id INT,
               PRIMARY KEY(id),
               CONSTRAINT fk_prev_url
                   FOREIGN KEY(prev_url_id)
                   REFERENCES url(id),
               CONSTRAINT fk_next_url
                   FOREIGN KEY(next_url_id)
                   REFERENCES url(id),
               UNIQUE(id),
               UNIQUE(prev_url_id, next_url_id)
            );
            DROP TABLE IF EXISTS word_to_link CASCADE;
            CREATE TABLE word_to_link(
               id INT GENERATED ALWAYS AS IDENTITY,
               word_id INT,
               link_id INT,
               PRIMARY KEY(id),
               CONSTRAINT fk_word
                   FOREIGN KEY(word_id)
                   REFERENCES word(id),
               CONSTRAINT fk_link_id
                   FOREIGN KEY(link_id)
                   REFERENCES url_to_url(id),
               UNIQUE(id)
            );
        """))
        print('база инициализирована')

    async def query(self, query, *args):
        async with self.pool.acquire() as conn:
            result = await conn.execute(query, *args)
        return result

    async def query_scalar(self, query, *args):
        async with self.pool.acquire() as conn:
            result = await conn.fetchval(query, *args)
        return result

    async def query_record(self, query, *args):
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow(query, *args)
        return result

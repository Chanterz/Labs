from typing import NamedTuple


class URL(NamedTuple):
    id: int
    url: str


class Page(NamedTuple):
    url: URL
    text: str


class Link(NamedTuple):
    prev_url: URL
    next_url: URL
    text: str


async def get_link_id(link: Link, db) -> int:
    id_: int = await db.query_scalar(
        """
        select id
        from url_to_url
        where prev_url_id = $1::int and next_url_id = $2::int
        """,
        link.prev_url.id,
        link.next_url.id,
    )
    return id_

from __future__ import annotations

from .BasePaginator import BasePaginator


class CursorPaginator(BasePaginator):
    """Laravel-style cursor paginator.

    Returns rows plus opaque cursors and the canonical cursor-page metadata.
    """

    def __init__(self, result, limit, next_cursor=None, prev_cursor=None, url=None):
        self.result = result
        self.limit = limit
        self.next_cursor = next_cursor
        self.prev_cursor = prev_cursor
        self.url = url

    def has_more_pages(self):
        return self.next_cursor is not None

    def serialize(self, *args, **kwargs):
        meta = {
            "limit": self.limit,
            "has_more": self.has_more_pages(),
            "next_cursor": self.next_cursor,
        }
        if self.prev_cursor is not None:
            meta["prev_cursor"] = self.prev_cursor
        return {
            "data": self.result.serialize(*args, **kwargs)
            if self.result is not None
            else [],
            "meta": meta,
        }

from __future__ import annotations

import math

from .BasePaginator import BasePaginator


class LengthAwarePaginator(BasePaginator):
    def __init__(
        self,
        result,
        per_page,
        current_page,
        total,
        url=None,
    ):
        self.result = result
        self.current_page = current_page
        self.per_page = per_page
        self.count = len(self.result)
        # ``per_page=0`` is malformed but reachable from internal
        # callers that bypass ``Pagination.from_validated`` (queue jobs,
        # console commands, tests). Without the guard,
        # ``ceil(total / 0)`` raises ``ZeroDivisionError`` and crashes
        # the response. ``max(1, ...)`` also means an empty result set
        # reports "page 1 of 1" instead of the semantically broken
        # "page 1 of 0" that the storefront pager UI mis-parses.
        if per_page and per_page > 0:
            self.last_page = max(1, int(math.ceil(total / per_page)))
        else:
            self.last_page = 1
        self.next_page = (int(self.current_page) + 1) if self.has_more_pages() else None
        # ``previous_page`` clamps to ``last_page`` when the caller
        # requested a page past the end — otherwise ``page=999`` on a
        # 5-item table emits ``previous_page=998`` (a phantom page
        # that also doesn't exist), and the client clicking "previous"
        # walks the user through 997 empty pages before reaching real
        # content.
        current_int = int(self.current_page)
        if current_int > self.last_page:
            self.previous_page = self.last_page
        else:
            self.previous_page = (current_int - 1) or None
        self.total = total
        self.url = url

    def serialize(self, *args, **kwargs):
        return {
            "data": self.result.serialize(*args, **kwargs),
            "meta": {
                "total": self.total,
                "next_page": self.next_page,
                "count": self.count,
                "previous_page": self.previous_page,
                "last_page": self.last_page,
                "current_page": self.current_page,
            },
        }

    def has_more_pages(self):
        return self.current_page < self.last_page

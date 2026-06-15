from __future__ import annotations

from .BasePaginator import BasePaginator


class SimplePaginator(BasePaginator):
    def __init__(self, result, per_page, current_page, url=None):
        self.per_page = per_page
        self.current_page = current_page
        # The query fetches per_page + 1 rows so we can detect a next page
        # without a COUNT query. Trim the sentinel row before exposing data.
        self._has_more = len(result) > per_page
        if self._has_more:
            result = result[:per_page]
        self.result = result
        self.count = len(self.result)
        self.next_page = (int(self.current_page) + 1) if self._has_more else None
        self.previous_page = (int(self.current_page) - 1) or None
        self.url = url

    def serialize(self, *args, **kwargs):
        return {
            "data": self.result.serialize(*args, **kwargs),
            "meta": {
                "next_page": self.next_page,
                "count": self.count,
                "previous_page": self.previous_page,
                "current_page": self.current_page,
            },
        }

    def has_more_pages(self):
        return self._has_more

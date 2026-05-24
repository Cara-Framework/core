import base64
import json

from .BasePaginator import BasePaginator


class CursorPaginator(BasePaginator):
    """Laravel-style cursor paginator.

    Returns rows plus opaque ``next_cursor`` / ``prev_cursor`` strings that can
    be passed back to ``cursor_paginate`` on a subsequent request. Cursors are
    keyset based (``WHERE id > last_id``), so pagination remains stable when
    records are inserted or deleted between requests.
    """

    def __init__(self, result, per_page, next_cursor=None, prev_cursor=None, url=None):
        self.result = result
        self.per_page = per_page
        self.count = len(self.result) if result is not None else 0
        self.next_cursor = next_cursor
        self.prev_cursor = prev_cursor
        self.url = url

    def has_more_pages(self):
        return self.next_cursor is not None

    def serialize(self, *args, **kwargs):
        return {
            "data": self.result.serialize(*args, **kwargs)
            if self.result is not None
            else [],
            "meta": {
                "per_page": self.per_page,
                "count": self.count,
                "next_cursor": self.next_cursor,
                "prev_cursor": self.prev_cursor,
            },
        }

    # ----- cursor encode / decode -----
    @staticmethod
    def encode(value):
        """Encode a keyset value into an opaque URL-safe cursor string."""
        raw = json.dumps({"v": value}, default=str).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

    # Allowlist of cursor-value types that make sense as a keyset
    # comparand. A keyset query is ``WHERE id > ?`` (or compound
    # equivalent) where ``?`` is bound as a query parameter — only
    # scalars are meaningful, and only scalars round-trip cleanly
    # through every supported DB driver. Pre-fix, ``decode`` returned
    # whatever ``json.loads(...).get("v")`` produced — a hand-crafted
    # cursor could smuggle a ``{}`` / ``[]`` / ``null`` body through
    # the keyset boundary, and the downstream query crashed with a
    # non-actionable psycopg2 error (``can't adapt type 'dict'``)
    # instead of being treated as "invalid cursor → start from
    # beginning". Booleans (``True``/``False``) are excluded too —
    # they're technically scalar in Python but never a real keyset.
    _ALLOWED_CURSOR_TYPES: tuple[type, ...] = (int, float, str)

    @staticmethod
    def decode(cursor):
        """Decode a cursor string back to the keyset value.

        Returns ``None`` for missing, malformed, or compound-payload
        cursors — same contract on every failure mode so the caller
        can branch once ("invalid cursor → start from beginning")
        without having to discriminate the failure cause.
        """
        if cursor is None:
            return None
        padding = "=" * (-len(cursor) % 4)
        try:
            raw = base64.urlsafe_b64decode(cursor + padding).decode("utf-8")
            payload = json.loads(raw)
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        value = payload.get("v")
        # ``isinstance(True, int)`` is True in Python — explicit bool
        # rejection prevents a tampered cursor smuggling a boolean
        # into a numeric keyset column.
        if isinstance(value, bool):
            return None
        if not isinstance(value, CursorPaginator._ALLOWED_CURSOR_TYPES):
            return None
        return value

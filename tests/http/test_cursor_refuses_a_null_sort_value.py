"""``slice_page_with_lookahead`` refuses a NULL sort value (ORM audit).

A cursor minted from a NULL sort value has no keyset to seek past
(``col > NULL`` is never true), so on PostgreSQL the next page came back empty
and the listing silently ended. Refused as the ORM's ``cursor_paginate``
refuses it.
"""

from __future__ import annotations

import pytest

from cara.http.Cursor import decode_cursor, slice_page_with_lookahead

SECRET = "c" * 40
FINGERPRINT = "0" * 64


def _slice(rows, limit=2):
    return slice_page_with_lookahead(
        rows,
        limit,
        sort_field="placed_at",
        direction="desc",
        fingerprint=FINGERPRINT,
        scope="orders.index",
        secret=SECRET,
    )


def test_a_null_sort_value_on_the_last_visible_row_is_refused():
    rows = [
        {"id": 3, "placed_at": "2026-11-03"},
        {"id": 2, "placed_at": None},
        {"id": 1, "placed_at": "2026-11-01"},
    ]

    with pytest.raises(ValueError, match="cursor column must be NOT NULL"):
        _slice(rows)


def test_a_null_beyond_the_page_or_on_the_last_page_needs_no_cursor():
    # No next page: nothing is minted, so nothing is refused.
    visible, cursor, has_more = _slice([{"id": 2, "placed_at": None}])
    assert (len(visible), cursor, has_more) == (1, None, False)


def test_a_present_sort_value_still_mints_a_cursor():
    rows = [
        {"id": 3, "placed_at": "2026-11-03"},
        {"id": 2, "placed_at": "2026-11-02"},
        {"id": 1, "placed_at": None},
    ]

    visible, cursor, has_more = _slice(rows)

    assert [row["id"] for row in visible] == [3, 2]
    assert has_more is True
    payload = decode_cursor(
        cursor,
        direction="desc",
        fingerprint=FINGERPRINT,
        scope="orders.index",
        secret=SECRET,
    )
    assert (payload["v"], payload["id"]) == ("2026-11-02", 2)

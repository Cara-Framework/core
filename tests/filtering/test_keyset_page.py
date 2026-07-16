"""``FilterPipeline.keyset_page`` — the cursor (keyset) pagination terminal.

Exercised with a fake builder (no DB): pins the deep-scroll contract —
LIMIT n+1 lookahead, NO count, a minted ``next_cursor`` derived from the last
visible row, raw rows when no ``resource`` is passed, and that the caller's
keyset WHERE + ORDER BY are composed onto the (filtered) builder.
"""

from __future__ import annotations

import pytest

from cara.filtering import FilterPipeline
from cara.http.Cursor import cursor_fingerprint, decode_cursor

_FILTERS = {"tenant_id": 7, "status": "active"}
_FINGERPRINT = cursor_fingerprint(_FILTERS)
_SCOPE = "tests.rows"


@pytest.fixture(autouse=True)
def _cursor_key(monkeypatch) -> None:
    monkeypatch.setenv("APP_KEY", "cursor-test-key-" * 3)


class _Row:
    def __init__(self, id_: int) -> None:
        self.id = id_


class _FakeBuilder:
    """Records the keyset composition + returns canned rows from ``get()``."""

    def __init__(self, rows: list[_Row]) -> None:
        self._rows = rows
        self.where_raw_calls: list[tuple] = []
        self.order_by_sql: str | None = None
        self.limit_n: int | None = None

    def clone(self) -> _FakeBuilder:
        return self

    def where_raw(self, sql: str, params: list) -> _FakeBuilder:
        self.where_raw_calls.append((sql, list(params)))
        return self

    def order_by_raw(self, sql: str) -> _FakeBuilder:
        self.order_by_sql = sql
        return self

    def limit(self, n: int) -> _FakeBuilder:
        self.limit_n = n
        return self

    def get(self) -> list[_Row]:
        return self._rows[: self.limit_n] if self.limit_n is not None else self._rows


def _pipe(rows: list[_Row]) -> FilterPipeline:
    return FilterPipeline(_FakeBuilder(rows), filters=None, sorts=None)


def test_keyset_page_first_page_has_more_and_mints_cursor() -> None:
    # 3 rows fetched for limit=2 → the lookahead row signals "more".
    res = _pipe([_Row(5), _Row(4), _Row(3)]).keyset_page(
        limit=2,
        sort_field="id",
        direction="desc",
        fingerprint=_FINGERPRINT,
        scope=_SCOPE,
        order_by_sql="t.id DESC",
        primary_key="id",
    )
    assert [r.id for r in res["data"]] == [5, 4]  # lookahead row trimmed; RAW rows
    assert res["has_more"] is True
    assert res["next_cursor"]  # token minted
    assert (
        decode_cursor(
            res["next_cursor"],
            direction="desc",
            fingerprint=_FINGERPRINT,
            scope=_SCOPE,
        )["id"]
        == 4
    )
    assert "total" not in res  # NO count — the whole point of keyset
    assert res["limit"] == 2


def test_keyset_page_last_page_no_more_no_cursor() -> None:
    # Exactly `limit` rows available → no lookahead row → end of feed.
    res = _pipe([_Row(2), _Row(1)]).keyset_page(
        limit=2,
        sort_field="id",
        direction="desc",
        fingerprint=_FINGERPRINT,
        scope=_SCOPE,
        order_by_sql="t.id DESC",
        primary_key="id",
    )
    assert [r.id for r in res["data"]] == [2, 1]
    assert res["has_more"] is False
    assert res["next_cursor"] is None


def test_keyset_page_composes_keyset_where_orderby_and_lookahead_limit() -> None:
    fake = _FakeBuilder([_Row(3), _Row(2)])
    FilterPipeline(fake, filters=None, sorts=None).keyset_page(
        limit=2,
        sort_field="id",
        direction="desc",
        fingerprint=_FINGERPRINT,
        scope=_SCOPE,
        order_by_sql="t.id DESC",
        keyset_where=("(t.id) < (%s)", [4]),
        primary_key="id",
    )
    assert ("(t.id) < (%s)", [4]) in fake.where_raw_calls  # keyset predicate applied
    assert fake.order_by_sql == "t.id DESC"
    assert fake.limit_n == 3  # LIMIT n+1 (the lookahead row)

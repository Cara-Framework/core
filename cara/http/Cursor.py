"""Cursor (keyset) pagination codec — generic, domain-free.

Companion to ``paging_rules`` — the offset-based helper stays the
supported pattern for shallow pagination. This module adds the cursor
path for hot endpoints where deep offsets degrade (the DB has to skip N
rows before returning the slice).

Cursor format
-------------
Opaque-to-the-client base64url-encoded JSON::

    cursor := base64url(utf8(json.dumps({
        "v": <sort-value>,    # value of the sort column on the last row
        "id": <primary-key>,  # tiebreaker on the canonical pk
    }, sort_keys=True, separators=(',', ':'))))

The structure is deliberately tiny so a paginated URL stays small.
``sort_keys=True`` + ``separators=(',', ':')`` make the token byte-stable
across encodes of the same logical position — useful for cache keys.

The SQL-building half (``build_keyset_where``) deliberately lives in the
app, not here: it needs an app-supplied allow-list of sortable columns to
guard against SQL injection, which is domain knowledge. This module ships
only the column-agnostic codec.
"""

from __future__ import annotations

import base64
import binascii
import json
from typing import Any, Literal

# Cursor "values" payload — small, JSON-safe, stable. Kept loose
# (``dict[str, Any]``) because the sort_value type varies by sort
# column: int for ids, float for prices/scores, str for ISO dates.
CursorPayload = dict[str, Any]
SortDirection = Literal["asc", "desc"]

# The two payload keys are intentionally short. A cursor that ships in
# every paginated URL is part of the wire surface; "v" / "id" is 9 bytes
# vs "sort_value" / "primary_key" at 23.
_KEY_SORT_VALUE = "v"
_KEY_ID = "id"


def encode_cursor(sort_value: Any, primary_key: int) -> str:
    """Build the opaque cursor token for the last row of a page.

    ``sort_value`` is whatever value the sort column held on that row
    (int / float / str / None). ``primary_key`` is the canonical row id —
    the tiebreaker so rows with equal ``sort_value`` are still totally
    ordered. The returned token is base64url-encoded JSON; clients should
    treat it as opaque.
    """
    payload: CursorPayload = {_KEY_SORT_VALUE: sort_value, _KEY_ID: int(primary_key)}
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(blob).rstrip(b"=").decode("ascii")


def decode_cursor(token: str | None) -> CursorPayload | None:
    """Parse an opaque cursor back into ``{v, id}``.

    Returns ``None`` for any malformed input — empty / non-base64 /
    base64-but-not-JSON / JSON-but-missing-required-keys — so the caller
    can branch on ``None`` to mean "no cursor, start from the beginning"
    without a try/except. Silently falling back to page 1 is friendlier
    than 422-ing on a stale URL.
    """
    if not token or not isinstance(token, str):
        return None
    # ``urlsafe_b64decode`` requires correct padding; the encoder strips it.
    padded = token + "=" * (-len(token) % 4)
    try:
        blob = base64.urlsafe_b64decode(padded.encode("ascii"))
    except (binascii.Error, ValueError, UnicodeDecodeError):
        return None
    try:
        payload = json.loads(blob.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if _KEY_SORT_VALUE not in payload or _KEY_ID not in payload:
        return None
    # ``id`` must be an int — anything else means a tampered cursor.
    if not isinstance(payload[_KEY_ID], int) or isinstance(payload[_KEY_ID], bool):
        return None
    return payload


def slice_page_with_lookahead(
    rows: list[Any], limit: int, *, sort_field: str, primary_key: str = "id"
) -> tuple[list[Any], str | None, bool]:
    """Given a query result fetched with ``LIMIT limit + 1``, return the
    displayed page + next-cursor + has_more flag.

    Accepts dict rows (from ``DB.select``) or attribute-style row objects
    (model instances) — falls back via ``getattr``. Returns
    ``(visible_rows, next_cursor, has_more)``. When ``has_more`` is False,
    ``next_cursor`` is ``None`` and the caller should NOT advertise a next
    page.
    """
    has_more = len(rows) > limit
    visible = rows[:limit]
    if not visible or not has_more:
        return visible, None, False
    last = visible[-1]
    sort_value = _extract_field(last, sort_field)
    pk_value = _extract_field(last, primary_key)
    if pk_value is None:
        # Without a primary key on the last row we can't build a cursor —
        # fall back to "no next page" rather than emit a broken token.
        return visible, None, False
    return visible, encode_cursor(sort_value, int(pk_value)), True


def _extract_field(row: Any, field: str) -> Any:
    """Dict-or-attribute access. ``row["created_at"]`` vs ``row.created_at``
    — both shapes appear in practice."""
    if isinstance(row, dict):
        return row.get(field)
    return getattr(row, field, None)


def cursor_rules(*, max_limit: int = 100, min_limit: int = 1) -> dict[str, str]:
    """Validation rules for cursor-paginated endpoints.

    Mirrors ``paging_rules`` — the offset / page validator — but accepts
    ``cursor`` instead of ``offset``. ``limit`` rules are identical so the
    two helpers can compose if a single endpoint accepts both modes.
    """
    return {
        "limit": f"nullable|integer|between:{min_limit},{max_limit}",
        # ``cursor`` is an opaque token — length-cap at 4 KB to block
        # obvious payload-stuffing without rejecting any legitimate token.
        "cursor": "nullable|string|max:4096",
    }

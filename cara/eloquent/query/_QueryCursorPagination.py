"""Cursor pagination execution for ``QueryBuilder``."""

from __future__ import annotations

import logging

from cara.http import decode_cursor, encode_cursor

from ..pagination import CursorPaginator, keyset_operator

_logger = logging.getLogger("cara.eloquent.query")
QueryBuilder: type


def _bind_query_builder(builder_type: type) -> None:
    global QueryBuilder
    QueryBuilder = builder_type


def _qb_cursor_paginate(
    self,
    per_page: int,
    *,
    cursor=None,
    column: str = "id",
    primary_key: str = "id",
    direction: str = "asc",
    scope: str,
    filter_fingerprint: str,
):
    """Laravel-style cursor pagination.

    Returns a CursorPaginator carrying the rows and next/prev cursor strings.
    Use the cursor returned in the response on subsequent calls to fetch the
    next page. Avoids OFFSET — stable under inserts/deletes.

    Args:
        per_page: Page size.
        cursor: Opaque cursor string from a previous response.
        column: Column to keyset-paginate by (must be unique + indexed).
        direction: "asc" or "desc".

    Returns:
        CursorPaginator
    """

    if (
        isinstance(per_page, bool)
        or not isinstance(per_page, int)
        or not 1 <= per_page <= 100
    ):
        raise ValueError("per_page must be an integer between 1 and 100")
    if direction not in {"asc", "desc"}:
        raise ValueError("direction must be 'asc' or 'desc'")
    if not isinstance(column, str) or not column:
        raise ValueError("column must be a non-empty string")
    if not isinstance(primary_key, str) or not primary_key:
        raise ValueError("primary_key must be a non-empty string")
    if not isinstance(scope, str) or not scope or len(scope) > 160:
        raise ValueError("scope must be a non-empty string of at most 160 characters")
    if (
        not isinstance(filter_fingerprint, str)
        or len(filter_fingerprint) != 64
        or any(char not in "0123456789abcdef" for char in filter_fingerprint)
    ):
        raise ValueError("filter_fingerprint must be lowercase SHA-256 hex")

    builder = self.clone()
    # One answer to "which comparison does a keyset seek use" for the
    # whole framework: this fluent form and the raw-SQL forms in
    # ``KeysetPredicate`` must never drift apart.
    op = keyset_operator(direction)
    if cursor is not None:
        decoded = decode_cursor(
            cursor,
            direction=direction,
            fingerprint=filter_fingerprint,
            scope=scope,
        )
        sort_value = decoded["v"]
        row_id = decoded["id"]
        builder = builder.where(
            lambda outer: outer.where(column, op, sort_value).or_where(
                lambda tied: tied.where(column, "=", sort_value).where(
                    primary_key, op, row_id
                )
            )
        )

    # Fetch one extra to know whether a next page exists.
    results = (
        builder.order_by(column, direction)
        .order_by(primary_key, direction)
        .limit(per_page + 1)
        .get()
    )

    has_more = len(results) > per_page
    if has_more:
        results = results[:per_page]

    next_cursor = None
    if has_more and len(results) > 0:
        last = results[-1]
        value = (
            getattr(last, column, None)
            if not isinstance(last, dict)
            else last.get(column)
        )
        row_id = (
            getattr(last, primary_key, None)
            if not isinstance(last, dict)
            else last.get(primary_key)
        )
        next_cursor = encode_cursor(
            value,
            row_id,
            direction=direction,
            fingerprint=filter_fingerprint,
            scope=scope,
        )

    return CursorPaginator(results, per_page, next_cursor, None)

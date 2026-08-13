"""InvalidCursor."""

from __future__ import annotations

from .HttpException import HttpException


class InvalidCursor(HttpException, ValueError):
    """A pagination cursor is malformed, tampered with, or belongs to another query.

    A tampered cursor is bad CLIENT input, so it answers 422 like any
    other validation failure. It used to be a bare ``ValueError`` living
    in ``cara.http.Cursor``, outside the taxonomy and therefore without a
    ``status_code`` — ``get_status_code`` fell through to "default to 500
    for unknown exceptions", so ``QueryBuilder.cursor_paginate`` turned an
    edited query string into a 500 with an ERROR-level traceback: a client
    fault recorded as a server fault, burning the error budget and paging
    oncall. Both products had to restate the translation themselves.

    Also a ``ValueError`` — a malformed cursor IS a value error, and the
    call sites that catch ``(InvalidCursor, TypeError, ValueError)`` around
    cursor decoding stay correct either way.
    """

    status_code = 422
    error_type = "validation_error"

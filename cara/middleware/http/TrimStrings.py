"""
Trim Strings Middleware for the Cara framework.

This module provides middleware that trims whitespace from all string inputs in HTTP requests.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from cara.http import Request, Response
from cara.middleware.Middleware import Middleware


class TrimStrings(Middleware):
    async def handle(
        self, request: Request, next_fn: Callable[..., Awaitable[Any]]
    ) -> Response:
        if hasattr(request, "_input") and request._input is not None:
            # Written back by KEY, not through ``InputBag.set``: ``set`` reads
            # a dot in the name as a path, so trimming ``?utm.source=x`` tried
            # to create ``utm → source`` while this loop was iterating the
            # same dict ("dictionary changed size during iteration"), and
            # ``?a=1&a.b=x`` tried to index into the string ``"1"``. Either
            # was a 500 any caller could trigger with a dotted parameter name.
            inputs = request._input.all()
            for name, value in inputs.items():
                if isinstance(value, str):
                    inputs[name] = value.strip()

        return await next_fn(request)

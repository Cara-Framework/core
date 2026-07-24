"""
Trim Strings Middleware for the Cara framework.

This module provides middleware that trims whitespace from all string inputs in HTTP requests.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from cara.http import Request, Response
from cara.middleware import Middleware


class TrimStrings(Middleware):
    async def handle(
        self, request: Request, next_fn: Callable[..., Awaitable[Any]]
    ) -> Response:
        if hasattr(request, "_input") and request._input is not None:
            for k, v in request._input.all().items():
                if isinstance(v, str):
                    request._input.set(k, v.strip())

        return await next_fn(request)

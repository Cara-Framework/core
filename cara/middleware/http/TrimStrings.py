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
    async def handle(self, request: Request, next_fn: Callable[..., Awaitable[Any]]) -> Response:
        # HTTP string trimming logic
        # Only works for dict-like input bags
        if hasattr(request, "input_bag"):
            for k, v in request.input_bag.all().items():
                if isinstance(v, str):
                    request.input_bag[k] = v.strip()

        return await next_fn(request)

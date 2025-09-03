"""
Trim Strings Middleware for the Cara framework.

This module provides middleware that trims whitespace from all string inputs in HTTP requests.
"""

from typing import Callable

from cara.http import Request
from cara.middleware import Middleware


class TrimStrings(Middleware):
    async def handle(self, request: Request, next: Callable):
        # HTTP string trimming logic
        # Only works for dict-like input bags
        if hasattr(request, "input_bag"):
            for k, v in request.input_bag.all().items():
                if isinstance(v, str):
                    request.input_bag[k] = v.strip()

        return await next(request)

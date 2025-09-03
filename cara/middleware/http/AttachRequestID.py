"""
Attach Request ID Middleware for the Cara framework.

This module provides middleware that attaches a unique request ID to each HTTP response for
traceability.
"""

from typing import Callable

from cara.http import Request
from cara.middleware import Middleware


class AttachRequestID(Middleware):
    async def handle(self, request: Request, next: Callable):
        response = await next(request)
        response.header("X-Request-ID", request.request_id)
        return response

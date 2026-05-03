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
        # Tag the Sentry scope as early as possible so any error fired
        # from downstream handlers carries the request ID. The header
        # echo on the response is for the client; the tag is for ops
        # to correlate a user-reported incident to the Sentry event.
        try:
            from cara.observability.Sentry import set_request_tag
            set_request_tag("request_id", request.request_id)
        except Exception:
            pass

        response = await next(request)
        response.header("X-Request-ID", request.request_id)
        return response

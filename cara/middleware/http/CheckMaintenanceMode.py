"""
Maintenance Mode Middleware for the Cara framework.

This module provides middleware that checks for maintenance mode and returns a 503 response if
enabled.
"""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from typing import Any

from cara.http import Request, Response
from cara.middleware import Middleware
from cara.support import paths


class CheckMaintenanceMode(Middleware):
    async def handle(
        self, request: Request, next_fn: Callable[..., Awaitable[Any]]
    ) -> Response:
        # Use paths() helper to get base path with MAINTENANCE file
        maintenance_path = paths("base", "MAINTENANCE")

        if os.path.exists(maintenance_path):
            resp = Response(self.application)
            # Canonical envelope ``{error, type}`` — same shape the
            # global exception handler uses, so clients can branch on
            # ``type === "maintenance_mode"`` instead of substring-
            # matching the human-readable error text.
            return resp.json(
                {
                    "error": "Service Unavailable (maintenance mode)",
                    "type": "maintenance_mode",
                },
                503,
            )

        return await next_fn(request)

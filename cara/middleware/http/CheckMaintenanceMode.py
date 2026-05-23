"""
Maintenance Mode Middleware for the Cara framework.

This module provides middleware that checks for maintenance mode and returns a 503 response if
enabled.
"""

import os
from collections.abc import Callable

from cara.http import Request, Response
from cara.middleware import Middleware
from cara.support import paths


class CheckMaintenanceMode(Middleware):
    async def handle(self, request: Request, next: Callable):
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

        return await next(request)

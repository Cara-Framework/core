"""
Can middleware for authorization checks.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from cara.exceptions import AuthorizationFailedException
from cara.http import Request, Response
from cara.middleware import Middleware


class CanPerform(Middleware):
    """Middleware for checking authorization abilities with automatic parameter parsing."""

    def __init__(self, application, ability: str = "view", resource: str | None = None):
        super().__init__(application)
        self.ability = ability
        self.resource = resource

    async def handle(
        self, request: Request, next_fn: Callable[[Any], Awaitable[Any]]
    ) -> Response:
        """Handle authorization check."""
        try:
            # Get gate from container
            gate = self.application.make("gate")

            # Extract parameters for authorization
            parameters = self._extract_parameters(request)

            # Check authorization
            if not gate.allows(self.ability, *parameters):
                raise AuthorizationFailedException(
                    message=f"This action is unauthorized. Missing ability: {self.ability}",
                    ability=self.ability,
                    status_code=403,
                )

            return await next_fn(request)

        except AuthorizationFailedException as e:
            response = Response(self.application)
            return response.json(
                {
                    "error": str(e) if str(e) else "This action is unauthorized",
                    "type": "authorization_error",
                },
                403,
            )
        except Exception:
            # Non-auth exceptions (DB, connection, etc.) must NOT be
            # masked as 403. Re-raise so the global exception handler
            # surfaces the real error (500) instead of misleading
            # clients into thinking they lack permission.
            raise

    def _extract_parameters(self, request: Request) -> list:
        """Extract parameters for the ability check."""
        parameters = []

        # Add resource if specified
        if self.resource:
            parameters.append(self.resource)

        # Add route parameters if available
        if hasattr(request, "route_params"):
            for key, value in request.route_params.items():
                # Try to resolve model instances from route parameters
                if key.endswith("_id") or key == "id":
                    # This could be extended to auto-resolve models
                    parameters.append(value)

        return parameters

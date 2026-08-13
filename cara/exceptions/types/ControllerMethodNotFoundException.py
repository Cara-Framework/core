"""ControllerMethodNotFoundException."""

from __future__ import annotations

from .RouteRegistrationException import RouteRegistrationException


class ControllerMethodNotFoundException(RouteRegistrationException):
    """Thrown when a controller method referenced in routes doesn't exist."""

    def __init__(
        self,
        controller_name: str,
        method_name: str,
        available_methods: list | None = None,
    ):
        message = f"Method '{method_name}' not found in controller '{controller_name}'"
        if available_methods:
            message += f". Available methods: {available_methods}"
        super().__init__(message, controller_name, method_name)
        self.available_methods = available_methods or []

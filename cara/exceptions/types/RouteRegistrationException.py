"""RouteRegistrationException."""

from __future__ import annotations

from .AppException import AppException


class RouteRegistrationException(AppException):
    """Thrown when route registration fails during application startup."""

    def __init__(
        self,
        message: str,
        controller_path: str | None = None,
        method_name: str | None = None,
    ):
        super().__init__(message)
        self.controller_path = controller_path
        self.method_name = method_name

    def get_debug_info(self):
        """Get debugging information for route registration failure."""
        return {
            "error": "Route Registration Failed",
            "message": str(self),
            "controller_path": self.controller_path,
            "method_name": self.method_name,
            "help": "Check your routes/api.py file for missing controller methods",
        }

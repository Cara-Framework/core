"""Application-level exceptions that don't fit elsewhere."""

from .base import CaraException


class AppException(CaraException):
    """Base for high-level "app" errors."""

    pass


class RouteRegistrationException(AppException):
    """Thrown when route registration fails during application startup."""

    def __init__(
        self, message: str, controller_path: str = None, method_name: str = None
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


class ControllerMethodNotFoundException(RouteRegistrationException):
    """Thrown when a controller method referenced in routes doesn't exist."""

    def __init__(
        self, controller_name: str, method_name: str, available_methods: list = None
    ):
        message = f"Method '{method_name}' not found in controller '{controller_name}'"
        if available_methods:
            message += f". Available methods: {available_methods}"
        super().__init__(message, controller_name, method_name)
        self.available_methods = available_methods or []



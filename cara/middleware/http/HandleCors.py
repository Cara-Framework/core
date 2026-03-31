"""
CORS Middleware for the Cara framework.

Laravel-style CORS middleware with configurable options.
Handles cross-origin requests with proper preflight support.
"""

from cara.configuration import config
from cara.http import Request, Response
from cara.middleware import Middleware


class HandleCors(Middleware):
    """
    Laravel-style CORS middleware (HandleCors).

    Configurable through config/cors.py or inline parameters.
    Handles OPTIONS preflight requests automatically.
    """

    def __init__(self, application, parameters=None):
        """
        Initialize CORS middleware.

        Args:
            application: The Cara application instance
            parameters: Optional inline parameters (overrides config)
        """
        super().__init__(application)
        self.parameters = parameters or []

        # Load configuration (Laravel style)
        self.config = self._load_config()

    def _load_config(self) -> dict:
        """Load CORS configuration from config or use defaults."""
        # Default Laravel-style CORS config
        defaults = {
            "paths": ["api/*"],
            "allowed_methods": ["*"],
            "allowed_origins": ["*"],
            "allowed_origins_patterns": [],
            "allowed_headers": ["*"],
            "exposed_headers": [],
            "max_age": 0,
            "supports_credentials": False,
        }

        # Try to load from config/cors.py (Laravel style)
        try:
            cors_config = config("cors", {})
            defaults.update(cors_config)
        except:
            pass

        return defaults

    async def handle(self, request: Request, next_handler):
        """
        Handle CORS request (Laravel style).

        Args:
            request: The HTTP request
            next_handler: Next middleware/handler in chain

        Returns:
            Response: CORS-enabled response
        """
        # Handle preflight OPTIONS requests
        if request.method.upper() == "OPTIONS":
            return self._handle_preflight(request)

        # Process request normally
        response = await next_handler(request)

        # Add CORS headers to response
        self._add_cors_headers(request, response)

        return response

    def _handle_preflight(self, request: Request) -> Response:
        """Handle OPTIONS preflight request."""
        response = Response(self.application)
        response.status(204)

        # Add CORS headers
        self._add_cors_headers(request, response)

        return response

    def _add_cors_headers(self, request: Request, response: Response) -> None:
        """Add CORS headers to response (Laravel style)."""
        origin = request.header("Origin")

        # Access-Control-Allow-Origin
        if self._is_origin_allowed(origin):
            if (
                "*" in self.config["allowed_origins"]
                and not self.config["supports_credentials"]
            ):
                response.header("Access-Control-Allow-Origin", "*")
            else:
                response.header("Access-Control-Allow-Origin", origin or "*")

        # Access-Control-Allow-Methods
        response.header(
            "Access-Control-Allow-Methods", ", ".join(self.config["allowed_methods"])
        )

        # Access-Control-Allow-Headers
        response.header(
            "Access-Control-Allow-Headers", ", ".join(self.config["allowed_headers"])
        )

        # Access-Control-Expose-Headers
        if self.config["exposed_headers"]:
            response.header(
                "Access-Control-Expose-Headers", ", ".join(self.config["exposed_headers"])
            )

        # Access-Control-Allow-Credentials
        if self.config["supports_credentials"]:
            response.header("Access-Control-Allow-Credentials", "true")

        # Access-Control-Max-Age
        response.header("Access-Control-Max-Age", str(self.config["max_age"]))

    def _is_origin_allowed(self, origin: str) -> bool:
        """Check if origin is allowed."""
        if not origin:
            return True

        allowed_origins = self.config["allowed_origins"]

        if "*" in allowed_origins:
            return True

        return origin in allowed_origins

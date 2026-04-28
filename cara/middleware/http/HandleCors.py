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
        """Load CORS configuration from config/cors.py using dot-path access."""
        return {
            "paths": config("cors.cors.paths", ["api/*"]),
            "allowed_methods": config("cors.cors.allowed_methods", ["*"]),
            "allowed_origins": config("cors.cors.allowed_origins", ["*"]),
            "allowed_origins_patterns": config("cors.cors.allowed_origins_patterns", []),
            "allowed_headers": config("cors.cors.allowed_headers", ["*"]),
            "exposed_headers": config("cors.cors.exposed_headers", []),
            "max_age": config("cors.cors.max_age", 0),
            "supports_credentials": config("cors.cors.supports_credentials", False),
        }

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
        """Add CORS headers to response (Laravel style).

        Security note — when ``supports_credentials`` is True we MUST
        NOT honour ``"*"`` in ``allowed_origins``: the browser refuses
        to send cookies / Authorization to a wildcard, but more
        importantly reflecting an arbitrary ``Origin`` together with
        ``Access-Control-Allow-Credentials: true`` is the textbook CSRF
        primitive. The previous implementation took the ``else`` branch
        — reflecting whatever the attacker's site sent — when wildcard
        was configured alongside credentials. We now treat that
        configuration as "no origin allowed" and emit no ACAO header.
        """
        origin = request.header("Origin")
        creds = bool(self.config["supports_credentials"])

        # Access-Control-Allow-Origin
        if self._is_origin_allowed(origin):
            allow_origin: str | None = None
            if creds:
                # Credentials path — only echo the origin if it matches
                # an EXPLICIT allowlist entry (string or regex), never
                # a wildcard. ``_is_origin_allowed`` would have returned
                # True for the wildcard case; double-check here.
                if origin and self._is_origin_explicitly_allowed(origin):
                    allow_origin = origin
            elif "*" in self.config["allowed_origins"]:
                allow_origin = "*"
            elif origin:
                allow_origin = origin

            if allow_origin is not None:
                response.header("Access-Control-Allow-Origin", allow_origin)
                if allow_origin != "*":
                    # When the ACAO value depends on the Origin header, proxies and
                    # CDNs must key their cache by it — otherwise one origin's
                    # response is served to another.
                    response.header("Vary", "Origin")

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

        # Access-Control-Allow-Credentials — only when explicitly
        # configured AND we actually emitted a non-wildcard ACAO above.
        if creds:
            response.header("Access-Control-Allow-Credentials", "true")

        # Access-Control-Max-Age
        response.header("Access-Control-Max-Age", str(self.config["max_age"]))

    def _is_origin_allowed(self, origin: str) -> bool:
        """Check if origin is allowed (any rule)."""
        if not origin:
            return True

        if "*" in self.config["allowed_origins"]:
            return True

        return self._is_origin_explicitly_allowed(origin)

    def _is_origin_explicitly_allowed(self, origin: str) -> bool:
        """Check if origin matches a NON-WILDCARD allowlist entry.

        Used by the credentials path where ``"*"`` must not match — the
        browser would block the cookie / header, and reflecting an
        arbitrary origin alongside ``Allow-Credentials: true`` is a
        cross-site request forgery primitive.
        """
        if not origin:
            return False
        if origin in self.config["allowed_origins"]:
            return True
        import re

        for pattern in self.config.get("allowed_origins_patterns", []):
            if re.match(pattern, origin):
                return True
        return False

"""
HTTP Response Module - Main Response Class.

Laravel-inspired modular response system with clean separation of concerns.
Orchestrates BaseResponse, ResponseFactory, HeaderManager, ContentTypeDetector, and StreamingResponse.
"""

from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Union

from .BaseResponse import BaseResponse
from .ContentTypeDetector import ContentTypeDetector
from .HeaderManager import HeaderManager
from .ResponseFactory import ResponseFactory
from .StreamingResponse import StreamingResponse


class Response(BaseResponse):
    """
    Main Laravel-inspired HTTP Response class.

    Clean orchestrator that delegates functionality to specialized components:
    - BaseResponse: Core ASGI handling
    - ResponseFactory: Laravel-style factory methods
    - HeaderManager: Robust header management
    - ContentTypeDetector: Smart fallback detection
    - StreamingResponse: Streaming capabilities
    """

    def __init__(self, application: Any):
        """
        Initialize Response with modular components.

        Args:
            application: The application container instance
        """
        super().__init__(application)

        # Initialize specialized components
        self.headers = HeaderManager(self.header_bag)
        self.factory = ResponseFactory(self)
        self.streaming = StreamingResponse(self)

    def clone_from(self, other: "Response") -> None:
        """
        Clone all attributes from another Response object.

        This method handles the Laravel-style response cloning pattern where
        controller methods return Response objects that need to be merged
        into the original response instance.

        Args:
            other: The Response object to clone from

        Note:
            Handles self-cloning gracefully to prevent infinite recursion
            and state corruption that can occur during route processing.
        """
        # Prevent self-cloning which can cause state corruption
        if self is other:
            return

        # Preserve critical header state before cloning
        # This prevents loss of explicitly set content-types during the clone process
        other_content_type_explicit = False
        other_content_type_value = None

        if hasattr(other, "headers") and other.headers:
            other_content_type_explicit = other.headers.is_content_type_explicit()
            other_content_type_value = other.headers.get("Content-Type")

        # Clone base response attributes (content, status, etc.)
        super().clone_from(other)

        # Recreate specialized components with fresh instances
        self.headers = HeaderManager(self.header_bag)
        self.factory = ResponseFactory(self)
        self.streaming = StreamingResponse(self)

        # Copy header data and preserve explicit content-type state
        if hasattr(other, "headers") and other.headers:
            self.headers.copy_from(other.headers)

            # Manually restore explicit content-type flag if it was set
            # This is necessary because HeaderManager recreation resets the flag
            if other_content_type_explicit and other_content_type_value:
                self.headers._content_type_explicitly_set = True

    async def __call__(self, scope: Dict, receive: Any, send: Any) -> None:
        """Handle ASGI response with finalization."""
        if self._sent:
            return

        try:
            self.prepare_content()
            self._finalize_response()
            await self._send_response(scope, receive, send)
            self._sent = True
        except Exception as e:
            await self._handle_error(e, send)

    def _finalize_response(self) -> None:
        """Finalize response before sending (Laravel-style)."""
        content_length = len(self.to_bytes())

        # Use smart detection if content-type not explicitly set
        default_content_type = None
        if not self.headers.is_content_type_explicit():
            default_content_type = ContentTypeDetector.detect(self.content)

        self.headers.finalize(content_length, default_content_type)

    # =============================================================================
    # EXPLICIT FACTORY METHODS (Laravel-style - Priority)
    # =============================================================================

    def json(
        self,
        payload: Any,
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style JSON response."""
        self.factory.json(payload, status, headers)
        return self

    def html(
        self,
        content: str,
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style HTML response."""
        self.factory.html(content, status, headers)
        return self

    def text(
        self,
        content: str,
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style plain text response."""
        self.factory.text(content, status, headers)
        return self

    def xml(
        self,
        content: str,
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style XML response."""
        self.factory.xml(content, status, headers)
        return self

    def css(
        self,
        content: str,
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style CSS response."""
        self.factory.css(content, status, headers)
        return self

    def javascript(
        self,
        content: str,
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style JavaScript response."""
        self.factory.javascript(content, status, headers)
        return self

    def svg(
        self,
        content: str,
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style SVG response."""
        self.factory.svg(content, status, headers)
        return self

    # =============================================================================
    # CONVENIENCE METHODS (Laravel-style shortcuts)
    # =============================================================================

    def success(
        self,
        data: Any = None,
        message: str = "Success",
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style success response."""
        self.factory.success(data, message, status, headers)
        return self

    def error(
        self,
        message: str = "Error",
        errors: Any = None,
        status: int = 400,
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style error response."""
        self.factory.error(message, errors, status, headers)
        return self

    def validation_error(
        self,
        errors: Dict[str, List[str]],
        message: str = "Validation failed",
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style validation error response."""
        self.factory.validation_error(errors, message, headers)
        return self

    def not_found(
        self,
        message: str = "Resource not found",
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style 404 response."""
        self.factory.not_found(message, headers)
        return self

    def unauthorized(
        self,
        message: str = "Unauthorized",
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style 401 response."""
        self.factory.unauthorized(message, headers)
        return self

    def forbidden(
        self,
        message: str = "Forbidden",
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style 403 response."""
        self.factory.forbidden(message, headers)
        return self

    def server_error(
        self,
        message: str = "Internal Server Error",
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style 500 response."""
        self.factory.server_error(message, headers)
        return self

    # =============================================================================
    # SPECIALIZED RESPONSES
    # =============================================================================

    def redirect(
        self,
        url: str,
        status: int = 302,
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style redirect response."""
        self.factory.redirect(url, status, headers)
        return self

    def download(
        self,
        content: Union[str, bytes],
        filename: str,
        content_type: str = "application/octet-stream",
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style download response."""
        self.factory.download(content, filename, content_type, headers)
        return self

    def file(
        self,
        file_path: str,
        filename: str = None,
        content_type: str = None,
        headers: Dict[str, str] = None,
    ) -> "Response":
        """Laravel-style file response for controlled access."""
        self.factory.file(file_path, filename, content_type, headers)
        return self

    def no_content(self, headers: Dict[str, str] = None) -> "Response":
        """Laravel-style 204 No Content response."""
        self.factory.no_content(headers)
        return self

    # =============================================================================
    # HEADER MANAGEMENT (Laravel-style) - FORCED NEW API
    # =============================================================================

    def header(self, name: str, value: str = None) -> Union["Response", Optional[str]]:
        """
        Laravel-style header method - get or set header.

        Usage:
            response.header('Content-Type', 'application/json')  # Set
            content_type = response.header('Content-Type')       # Get

        Args:
            name: Header name
            value: Header value (if setting)

        Returns:
            Response: Self (if setting)
            str: Header value (if getting)
        """
        if value is None:
            return self.headers.get(name)

        self.headers.set(name, value)
        return self

    def with_headers(self, headers: Dict[str, str]) -> "Response":
        """Laravel-style method to set multiple headers."""
        self.headers.merge(headers)
        return self

    def content_type(self, type_: str) -> "Response":
        """Laravel-style content-type setter."""
        self.headers.content_type(type_)
        return self

    def cache_control(self, cache_control: str) -> "Response":
        """Set Cache-Control header."""
        self.headers.cache_control(cache_control)
        return self

    def cors(
        self,
        origin: str = "*",
        methods: str = "GET, POST, PUT, DELETE, OPTIONS",
        headers: str = "Content-Type, Authorization",
    ) -> "Response":
        """Set CORS headers."""
        self.headers.cors(origin, methods, headers)
        return self

    def secure(self) -> "Response":
        """Set security headers."""
        self.headers.secure()
        return self

    def no_cache(self) -> "Response":
        """Set no-cache headers."""
        self.headers.no_cache()
        return self

    def csp(self, policy: str) -> "Response":
        """Set Content Security Policy header."""
        self.headers.csp(policy)
        return self

    def hsts(
        self, max_age: int = 31536000, include_subdomains: bool = True
    ) -> "Response":
        """Set HTTP Strict Transport Security header."""
        self.headers.hsts(max_age, include_subdomains)
        return self

    # =============================================================================
    # STREAMING SUPPORT (Laravel-style)
    # =============================================================================

    async def stream(
        self,
        generator: AsyncGenerator[bytes, None],
        send: Callable,
        status: int = 200,
        content_type: str = "application/octet-stream",
        headers: Dict[str, str] = None,
    ) -> None:
        """Laravel-style streaming response."""
        await self.streaming.stream(generator, send, status, content_type, headers)

    async def stream_json_lines(
        self,
        data_generator: AsyncGenerator[Any, None],
        send: Callable,
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> None:
        """Stream JSON Lines (JSONL) format."""
        await self.streaming.stream_json_lines(data_generator, send, status, headers)

    async def stream_sse(
        self,
        event_generator: AsyncGenerator[Dict[str, Any], None],
        send: Callable,
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> None:
        """Stream Server-Sent Events."""
        await self.streaming.stream_server_sent_events(
            event_generator, send, status, headers
        )

    async def stream_download(
        self,
        file_generator: AsyncGenerator[bytes, None],
        filename: str,
        send: Callable,
        content_type: str = "application/octet-stream",
        content_length: int = None,
        headers: Dict[str, str] = None,
    ) -> None:
        """Stream file download."""
        await self.streaming.stream_file_download(
            file_generator, filename, send, content_type, content_length, headers
        )

    async def stream_csv(
        self,
        data_generator: AsyncGenerator[List[str], None],
        filename: str,
        send: Callable,
        headers: Dict[str, str] = None,
    ) -> None:
        """Stream CSV data."""
        await self.streaming.stream_csv(data_generator, filename, send, headers)

    # =============================================================================
    # COMPATIBILITY AND UTILITY METHODS
    # =============================================================================

    def get_headers(self) -> List[tuple]:
        """Get headers for ASGI compatibility."""
        return self.headers.to_asgi()

    def get_status_code(self) -> int:
        """Get current status code."""
        return self._status

    def __repr__(self) -> str:
        """String representation of Response."""
        status = self.get_status_code()
        content_type = self.headers.content_type() or "auto-detect"
        content_size = len(self.to_bytes())
        return f"Response(status={status}, content_type='{content_type}', size={content_size})"

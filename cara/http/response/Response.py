"""
HTTP Response Module - Main Response Class.

Laravel-inspired modular response system with clean separation of concerns.
Orchestrates BaseResponse, ResponseFactory, HeaderManager, ContentTypeDetector, and StreamingResponse.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any

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
        self._stream_spec: tuple[
            AsyncGenerator[bytes],
            int,
            str,
            dict[str, str] | None,
        ] | None = None

    def clone_from(self, other: Response) -> None:
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

        # ``RouteDispatcher`` merges a controller-returned response into the
        # request-scoped response object.  Streaming state must survive that
        # merge just like content/status/headers do; otherwise a controller can
        # configure a stream successfully and the conductor will later emit an
        # empty regular response.
        other_stream_spec = getattr(other, "_stream_spec", None)

        # Clone base response attributes (content, status, etc.)
        super().clone_from(other)

        # Recreate specialized components with fresh instances
        self.headers = HeaderManager(self.header_bag)
        self.factory = ResponseFactory(self)
        self.streaming = StreamingResponse(self)
        self._stream_spec = other_stream_spec

        # Copy header data and preserve explicit content-type state
        if hasattr(other, "headers") and other.headers:
            self.headers.copy_from(other.headers)

            # Manually restore explicit content-type flag if it was set
            # This is necessary because HeaderManager recreation resets the flag
            if other_content_type_explicit and other_content_type_value:
                self.headers._content_type_explicitly_set = True

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        """Handle ASGI response with finalization."""
        if self._sent:
            return

        try:
            if self._stream_spec is not None:
                generator, status, content_type, headers = self._stream_spec
                await self.streaming.stream(
                    generator,
                    send,
                    status=status,
                    content_type=content_type,
                    headers=headers,
                )
                return

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
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Laravel-style JSON response."""
        self.factory.json(payload, status, headers)
        return self

    def envelope(
        self,
        data: Any,
        meta: dict | None = None,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Return a standard API envelope: {"data": ..., "meta": {...}}.

        Omits the meta key when None is passed (lighter payloads for
        single-resource responses). Pass an empty dict explicitly to
        include an empty meta object for contract consistency.
        """
        self.factory.envelope(data, meta, status, headers)
        return self

    def paginated(
        self,
        data: Any,
        *,
        limit: int,
        has_more: bool,
        next_cursor: str | None,
        prev_cursor: str | None = None,
        status: int = 200,
        headers: dict[str, str] | None = None,
        **extra_meta: Any,
    ) -> Response:
        """Cursor-paginated collection with ``LIMIT n+1`` lookahead metadata."""
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 100
        ):
            raise ValueError("cursor pagination limit must be between 1 and 100")
        if not isinstance(has_more, bool):
            raise TypeError("has_more must be boolean")
        if next_cursor is not None and (
            not isinstance(next_cursor, str) or not next_cursor
        ):
            raise TypeError("next_cursor must be a non-empty string or None")
        if prev_cursor is not None and (
            not isinstance(prev_cursor, str) or not prev_cursor
        ):
            raise TypeError("prev_cursor must be a non-empty string or None")
        if has_more and not next_cursor:
            raise ValueError("has_more=True requires next_cursor")
        if not has_more and next_cursor is not None:
            raise ValueError("next_cursor must be None on the final page")
        if hasattr(data, "to_array") and callable(data.to_array):
            serialized = data.to_array()
        elif hasattr(data, "to_list") and callable(data.to_list):
            serialized = data.to_list()
        else:
            serialized = data

        meta: dict[str, Any] = {
            "limit": limit,
            "has_more": has_more,
            "next_cursor": next_cursor,
        }
        if prev_cursor is not None:
            meta["prev_cursor"] = prev_cursor
        if extra_meta:
            meta.update(extra_meta)

        payload = {"data": serialized, "meta": meta}
        return self.json(payload, status=status, headers=headers)

    def html(
        self,
        content: str,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Laravel-style HTML response."""
        self.factory.html(content, status, headers)
        return self

    def text(
        self,
        content: str,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Laravel-style plain text response."""
        self.factory.text(content, status, headers)
        return self

    def xml(
        self,
        content: str,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Laravel-style XML response."""
        self.factory.xml(content, status, headers)
        return self

    def css(
        self,
        content: str,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Laravel-style CSS response."""
        self.factory.css(content, status, headers)
        return self

    def javascript(
        self,
        content: str,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Laravel-style JavaScript response."""
        self.factory.javascript(content, status, headers)
        return self

    def svg(
        self,
        content: str,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> Response:
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
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Laravel-style success response."""
        self.factory.success(data, message, status, headers)
        return self

    def error(
        self,
        message: str = "Error",
        errors: Any = None,
        status: int = 400,
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Laravel-style error response."""
        self.factory.error(message, errors, status, headers)
        return self

    def validation_error(
        self,
        errors: dict[str, list[str]],
        message: str = "Validation failed",
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Laravel-style validation error response."""
        self.factory.validation_error(errors, message, headers)
        return self

    def not_found(
        self,
        message: str = "Resource not found",
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Laravel-style 404 response."""
        self.factory.not_found(message, headers)
        return self

    def unauthorized(
        self,
        message: str = "Unauthorized",
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Laravel-style 401 response."""
        self.factory.unauthorized(message, headers)
        return self

    def forbidden(
        self,
        message: str = "Forbidden",
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Laravel-style 403 response."""
        self.factory.forbidden(message, headers)
        return self

    def server_error(
        self,
        message: str = "Internal Server Error",
        headers: dict[str, str] | None = None,
    ) -> Response:
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
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Laravel-style redirect response."""
        self.factory.redirect(url, status, headers)
        return self

    def download(
        self,
        content: str | bytes,
        filename: str,
        content_type: str = "application/octet-stream",
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Laravel-style download response."""
        self.factory.download(content, filename, content_type, headers)
        return self

    def file(
        self,
        file_path: str,
        filename: str | None = None,
        content_type: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Laravel-style file response for controlled access."""
        self.factory.file(file_path, filename, content_type, headers)
        return self

    def no_content(self, headers: dict[str, str] | None = None) -> Response:
        """Laravel-style 204 No Content response."""
        self.factory.no_content(headers)
        return self

    # =============================================================================
    # HEADER MANAGEMENT (Laravel-style) - FORCED NEW API
    # =============================================================================

    def header(self, name: str, value: str | None = None) -> Response | str | None:
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

    def with_headers(self, headers: dict[str, str]) -> Response:
        """Laravel-style method to set multiple headers."""
        self.headers.merge(headers)
        return self

    def content_type(self, type_: str) -> Response:
        """Laravel-style content-type setter."""
        self.headers.content_type(type_)
        return self

    def cache_control(self, cache_control: str) -> Response:
        """Set Cache-Control header."""
        self.headers.cache_control(cache_control)
        return self

    def cors(
        self,
        origin: str = "*",
        methods: str = "GET, POST, PUT, DELETE, OPTIONS",
        headers: str = "Content-Type, Authorization",
    ) -> Response:
        """Set CORS headers."""
        self.headers.cors(origin, methods, headers)
        return self

    def secure(self) -> Response:
        """Set security headers."""
        self.headers.secure()
        return self

    def no_cache(self) -> Response:
        """Set no-cache headers."""
        self.headers.no_cache()
        return self

    def csp(self, policy: str) -> Response:
        """Set Content Security Policy header."""
        self.headers.csp(policy)
        return self

    def hsts(self, max_age: int = 31536000, include_subdomains: bool = True) -> Response:
        """Set HTTP Strict Transport Security header."""
        self.headers.hsts(max_age, include_subdomains)
        return self

    # =============================================================================
    # STREAMING SUPPORT (Laravel-style)
    # =============================================================================

    def stream(
        self,
        generator: AsyncGenerator[bytes],
        status: int = 200,
        content_type: str = "application/octet-stream",
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Configure a deferred streaming response.

        Controllers do not receive the raw ASGI ``send`` callable; the
        conductor owns it and invokes the returned response after middleware
        completes.  Streaming is therefore configured here and consumed later
        by :meth:`__call__`, exactly like a regular ``json()``/``text()``
        response.  No content-length is calculated and the generator is not
        advanced while the controller is running.
        """
        self.content = b""
        self._status = int(status)
        stream_headers = dict(headers) if headers else None
        self.headers.content_type(content_type)
        if stream_headers:
            self.headers.merge(stream_headers)
        self._stream_spec = (
            generator,
            self._status,
            content_type,
            stream_headers,
        )
        return self

    def stream_json_lines(
        self,
        data_generator: AsyncGenerator[Any],
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Stream JSON Lines (JSONL) format."""
        import json

        async def chunks() -> AsyncGenerator[bytes]:
            async for data in data_generator:
                yield (
                    json.dumps(data, ensure_ascii=False, default=str) + "\n"
                ).encode("utf-8")

        return self.stream(
            chunks(),
            status=status,
            content_type="application/x-ndjson; charset=utf-8",
            headers=headers,
        )

    def stream_sse(
        self,
        event_generator: AsyncGenerator[dict[str, Any]],
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Stream Server-Sent Events."""
        async def chunks() -> AsyncGenerator[bytes]:
            async for event in event_generator:
                yield self.streaming._format_sse_event(event).encode("utf-8")

        sse_headers = {
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Cache-Control",
        }
        if headers:
            sse_headers.update(headers)

        return self.stream(
            chunks(),
            status=status,
            content_type="text/event-stream; charset=utf-8",
            headers=sse_headers,
        )

    def stream_download(
        self,
        file_generator: AsyncGenerator[bytes],
        filename: str,
        content_type: str = "application/octet-stream",
        content_length: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Stream file download."""
        download_headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
        }
        if content_length is not None:
            download_headers["Content-Length"] = str(content_length)
        if headers:
            download_headers.update(headers)

        return self.stream(
            file_generator,
            content_type=content_type,
            headers=download_headers,
        )

    def stream_csv(
        self,
        data_generator: AsyncGenerator[list[Any]],
        filename: str,
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Configure a chunked CSV attachment without materialising its body."""
        download_headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
        }
        if headers:
            download_headers.update(headers)

        return self.stream(
            self.streaming.csv_chunks(data_generator),
            content_type="text/csv; charset=utf-8",
            headers=download_headers,
        )

    # =============================================================================
    # COMPATIBILITY AND UTILITY METHODS
    # =============================================================================

    def get_headers(self) -> list[tuple]:
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

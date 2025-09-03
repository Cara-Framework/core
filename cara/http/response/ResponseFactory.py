"""
Response Factory Module.

Laravel-style factory methods for creating different types of HTTP responses.
Provides explicit methods with priority over intelligent detection.
"""

import json
from typing import Any, Dict, List, Union

from .BaseResponse import BaseResponse
from .ContentTypeDetector import ContentTypeDetector
from .HeaderManager import HeaderManager


class ResponseFactory:
    """
    Laravel-style response factory.

    Provides explicit methods for creating different types of responses:
    json(), html(), text(), xml(), css(), javascript(), etc.

    These methods take priority over intelligent content-type detection.
    """

    def __init__(self, base_response: BaseResponse):
        """
        Initialize ResponseFactory with BaseResponse.

        Args:
            base_response: BaseResponse instance to work with
        """
        self.response = base_response
        self.headers = HeaderManager(base_response.header_bag)

    # =============================================================================
    # EXPLICIT CONTENT-TYPE METHODS (Laravel-style - Priority)
    # =============================================================================

    def json(
        self,
        payload: Any,
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> BaseResponse:
        """
        Create a JSON response (Laravel-style explicit method).

        Args:
            payload: Data to be JSON encoded
            status: HTTP status code
            headers: Additional headers

        Returns:
            BaseResponse: Configured response
        """
        self.response._status = status
        self.response.set_content(json.dumps(payload, ensure_ascii=False))

        # Explicitly set content-type (Laravel approach)
        self.headers.content_type("application/json; charset=utf-8")

        if headers:
            self.headers.merge(headers)
        return self.response

    def html(
        self,
        content: str,
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> BaseResponse:
        """
        Create an HTML response (Laravel-style explicit method).

        Args:
            content: HTML content
            status: HTTP status code
            headers: Additional headers

        Returns:
            BaseResponse: Configured response
        """
        self.response._status = status
        self.response.set_content(content)

        # Explicitly set content-type (Laravel approach)
        self.headers.content_type("text/html; charset=utf-8")

        if headers:
            self.headers.merge(headers)
        return self.response

    def text(
        self,
        content: str,
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> BaseResponse:
        """
        Create a plain text response (Laravel-style explicit method).

        Args:
            content: Text content
            status: HTTP status code
            headers: Additional headers

        Returns:
            BaseResponse: Configured response
        """
        self.response._status = status
        self.response.set_content(content)

        # Explicitly set content-type (Laravel approach)
        self.headers.content_type("text/plain; charset=utf-8")

        if headers:
            self.headers.merge(headers)
        return self.response

    def xml(
        self,
        content: str,
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> BaseResponse:
        """
        Create an XML response (Laravel-style explicit method).

        Args:
            content: XML content
            status: HTTP status code
            headers: Additional headers

        Returns:
            BaseResponse: Configured response
        """
        self.response._status = status
        self.response.set_content(content)

        # Explicitly set content-type (Laravel approach)
        self.headers.content_type("application/xml; charset=utf-8")

        if headers:
            self.headers.merge(headers)
        return self.response

    def css(
        self,
        content: str,
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> BaseResponse:
        """
        Create a CSS response (Laravel-style explicit method).

        Args:
            content: CSS content
            status: HTTP status code
            headers: Additional headers

        Returns:
            BaseResponse: Configured response
        """
        self.response._status = status
        self.response.set_content(content)

        # Explicitly set content-type (Laravel approach)
        self.headers.content_type("text/css; charset=utf-8")

        if headers:
            self.headers.merge(headers)
        return self.response

    def javascript(
        self,
        content: str,
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> BaseResponse:
        """
        Create a JavaScript response (Laravel-style explicit method).

        Args:
            content: JavaScript content
            status: HTTP status code
            headers: Additional headers

        Returns:
            BaseResponse: Configured response
        """
        self.response._status = status
        self.response.set_content(content)

        # Explicitly set content-type (Laravel approach)
        self.headers.content_type("application/javascript; charset=utf-8")

        if headers:
            self.headers.merge(headers)
        return self.response

    def svg(
        self,
        content: str,
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> BaseResponse:
        """
        Create an SVG response (Laravel-style explicit method).

        Args:
            content: SVG content
            status: HTTP status code
            headers: Additional headers

        Returns:
            BaseResponse: Configured response
        """
        self.response._status = status
        self.response.set_content(content)

        # Explicitly set content-type (Laravel approach)
        self.headers.content_type("image/svg+xml; charset=utf-8")

        if headers:
            self.headers.merge(headers)
        return self.response

    # =============================================================================
    # CONVENIENCE METHODS (Laravel-style shortcuts)
    # =============================================================================

    def success(
        self,
        data: Any = None,
        message: str = "Success",
        status: int = 200,
        headers: Dict[str, str] = None,
    ) -> BaseResponse:
        """Laravel-style success response."""
        payload = {"success": True, "message": message}
        if data is not None:
            payload["data"] = data
        return self.json(payload, status, headers)

    def error(
        self,
        message: str = "Error",
        errors: Any = None,
        status: int = 400,
        headers: Dict[str, str] = None,
    ) -> BaseResponse:
        """Laravel-style error response."""
        payload = {"success": False, "message": message}
        if errors is not None:
            payload["errors"] = errors
        return self.json(payload, status, headers)

    def validation_error(
        self,
        errors: Dict[str, List[str]],
        message: str = "Validation failed",
        headers: Dict[str, str] = None,
    ) -> BaseResponse:
        """Laravel-style validation error response."""
        return self.json(
            {"success": False, "message": message, "errors": errors}, 422, headers
        )

    def not_found(
        self,
        message: str = "Resource not found",
        headers: Dict[str, str] = None,
    ) -> BaseResponse:
        """Laravel-style 404 response."""
        return self.error(message, status=404, headers=headers)

    def unauthorized(
        self,
        message: str = "Unauthorized",
        headers: Dict[str, str] = None,
    ) -> BaseResponse:
        """Laravel-style 401 response."""
        return self.error(message, status=401, headers=headers)

    def forbidden(
        self,
        message: str = "Forbidden",
        headers: Dict[str, str] = None,
    ) -> BaseResponse:
        """Laravel-style 403 response."""
        return self.error(message, status=403, headers=headers)

    def server_error(
        self,
        message: str = "Internal Server Error",
        headers: Dict[str, str] = None,
    ) -> BaseResponse:
        """Laravel-style 500 response."""
        return self.error(message, status=500, headers=headers)

    # =============================================================================
    # SPECIALIZED RESPONSES
    # =============================================================================

    def redirect(
        self,
        url: str,
        status: int = 302,
        headers: Dict[str, str] = None,
    ) -> BaseResponse:
        """
        Create a redirect response.

        Args:
            url: URL to redirect to
            status: HTTP status code (default: 302)
            headers: Additional headers

        Returns:
            BaseResponse: Redirect response
        """
        self.response._status = status
        self.response.set_content("")

        # Set Location header
        self.headers.location(url)

        if headers:
            self.headers.merge(headers)
        return self.response

    def download(
        self,
        content: Union[str, bytes],
        filename: str,
        content_type: str = "application/octet-stream",
        headers: Dict[str, str] = None,
    ) -> BaseResponse:
        """
        Create a file download response.

        Args:
            content: File content
            filename: Download filename
            content_type: Content type
            headers: Additional headers

        Returns:
            BaseResponse: Download response
        """
        self.response.set_content(content)

        # Set download headers
        self.headers.content_type(content_type)
        self.headers.set("Content-Disposition", f'attachment; filename="{filename}"')

        if headers:
            self.headers.merge(headers)
        return self.response

    def no_content(self, headers: Dict[str, str] = None) -> BaseResponse:
        """
        Create a 204 No Content response.

        Args:
            headers: Additional headers

        Returns:
            BaseResponse: No content response
        """
        self.response._status = 204
        self.response.set_content("")

        if headers:
            self.headers.merge(headers)
        return self.response

    # =============================================================================
    # SMART DETECTION FALLBACK
    # =============================================================================

    def auto_detect_content_type(self) -> BaseResponse:
        """
        Use smart content-type detection as fallback.

        Only used when explicit methods are not called.

        Returns:
            BaseResponse: Response with detected content-type
        """
        if not self.headers.is_content_type_explicit():
            detected_type = ContentTypeDetector.detect(self.response.content)
            self.headers.content_type(detected_type)
        return self.response

    def finalize(self) -> BaseResponse:
        """
        Finalize response before sending.

        Sets Content-Length and applies smart detection if needed.

        Returns:
            BaseResponse: Finalized response
        """
        content_length = len(self.response.to_bytes())

        # Use smart detection if content-type not explicitly set
        default_content_type = None
        if not self.headers.is_content_type_explicit():
            default_content_type = ContentTypeDetector.detect(self.response.content)

        self.headers.finalize(content_length, default_content_type)
        return self.response

    def file(
        self,
        file_path: str,
        filename: str = None,
        content_type: str = None,
        headers: Dict[str, str] = None,
    ) -> None:
        """
        Laravel-style file response for controlled access.

        Args:
            file_path: Path to the file to serve
            filename: Optional filename for download (if not provided, uses file's name)
            content_type: Optional content type (auto-detected if not provided)
            headers: Optional additional headers
        """
        import mimetypes
        import os

        if not os.path.isfile(file_path):
            self.response.status(404)
            self.response.set_content("File not found")
            return

        # Get file info
        stat = os.stat(file_path)
        file_size = stat.st_size

        # Determine content type
        if content_type is None:
            content_type, _ = mimetypes.guess_type(file_path)
            if content_type is None:
                content_type = "application/octet-stream"

        # Set headers
        self.response.header("Content-Type", content_type)
        self.response.header("Content-Length", str(file_size))
        self.response.header("Accept-Ranges", "bytes")

        # Set filename if provided (for downloads)
        if filename:
            self.response.header(
                "Content-Disposition", f'attachment; filename="{filename}"'
            )

        # Add additional headers
        if headers:
            self.response.with_headers(headers)

        # Read and serve file content
        try:
            with open(file_path, "rb") as f:
                file_content = f.read()
                self.response.set_content(file_content)
        except IOError:
            self.response.status(500)
            self.response.set_content("Error reading file")

"""
Static Files Middleware for Cara Framework.

This middleware serves static files from the public directory, similar to Laravel's
static file serving capability. It handles files like images, CSS, JS, etc.
"""

import mimetypes
import os

from cara.http import Request, Response
from cara.middleware import Middleware
from cara.support.paths import public_path


class ServeStaticFiles(Middleware):
    """
    Middleware to serve static files from public directory.

    This middleware checks if the request is for a static file in the public
    directory and serves it directly, bypassing the normal routing system.
    """

    async def handle(self, request: Request, next_handler):
        """
        Handle the request - serve static file if it exists, otherwise continue.

        Args:
            request: The HTTP request
            next_handler: The next middleware/handler in the chain

        Returns:
            Response: Either the static file response or the result of next_handler
        """
        # Get the public directory path using Laravel-style helper
        public_dir = public_path()

        # Only handle GET and HEAD requests for static files
        if request.method not in ["GET", "HEAD"]:
            return await next_handler(request)

        # Check if this looks like a static file request
        path = request.path.lstrip("/")

        # Skip if path is empty or doesn't exist
        if not path:
            return await next_handler(request)

        if not os.path.exists(public_dir):
            return await next_handler(request)

        # Check if the file exists in the public directory
        full_path = os.path.join(public_dir, path)

        # Security check: ensure the path is within public directory
        if not self._is_safe_path(full_path, public_dir):
            return await next_handler(request)

        if not os.path.isfile(full_path):
            return await next_handler(request)

        # Serve the static file
        try:
            return self._serve_file(full_path, request.method == "HEAD")
        except Exception:
            # If static file serving fails, continue with normal routing
            return await next_handler(request)

    def _is_safe_path(self, path: str, public_dir: str) -> bool:
        """
        Check if the requested path is safe (within public directory).

        Args:
            path: The full file path to check
            public_dir: The public directory path

        Returns:
            bool: True if path is safe, False otherwise
        """
        try:
            # Resolve both paths to absolute paths
            public_dir = os.path.abspath(public_dir)
            requested_path = os.path.abspath(path)

            # Check if requested path is within public directory
            return requested_path.startswith(public_dir)
        except:
            return False

    def _serve_file(self, file_path: str, head_only: bool = False) -> Response:
        """
        Serve a static file.

        Args:
            file_path: Path to the file to serve
            head_only: If True, only send headers (for HEAD requests)

        Returns:
            Response: The file response
        """
        response = Response(self.application)

        # Get file info
        stat = os.stat(file_path)
        file_size = stat.st_size

        # Determine content type
        content_type, _ = mimetypes.guess_type(file_path)
        if content_type is None:
            content_type = "application/octet-stream"

        # Set headers
        response.header("Content-Type", content_type)
        response.header("Content-Length", str(file_size))
        response.header("Accept-Ranges", "bytes")

        # Add caching headers for static files
        response.header("Cache-Control", "public, max-age=3600")

        # For HEAD requests, don't send body
        if head_only:
            return response

        # Read and serve file content
        try:
            with open(file_path, "rb") as f:
                file_content = f.read()
                # Directly set content attribute
                response.content = file_content
        except IOError:
            response.status(404)
            response.content = b"File not found"

        return response

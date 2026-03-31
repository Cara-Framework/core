"""
Header Management Module.

Laravel-inspired header management for HTTP responses with case-insensitive operations.
Clean, robust header handling with explicit content-type tracking.
"""

from typing import Dict, List, Optional, Tuple, Union

from cara.http.request import Header, HeaderBag


class HeaderManager:
    """
    Laravel-style header manager for HTTP responses.

    Provides clean header management with case-insensitive operations,
    explicit content-type tracking, and robust HeaderBag integration.
    """

    def __init__(self, header_bag: HeaderBag):
        """
        Initialize HeaderManager with existing HeaderBag.

        Args:
            header_bag: HeaderBag instance to manage
        """
        self.header_bag = header_bag
        self._content_type_explicitly_set = False

    # =============================================================================
    # CORE HEADER OPERATIONS (Laravel-style)
    # =============================================================================

    def set(self, name: str, value: str) -> None:
        """
        Set header with case-insensitive handling (Laravel-style).

        Args:
            name: Header name
            value: Header value
        """
        # Track explicit content-type setting
        if name.lower() == "content-type":
            self._content_type_explicitly_set = True

        # HeaderBag automatically handles case-insensitive and duplicates
        self.header_bag.add(Header(name, value))

    def get(self, name: str, default: str = None) -> Optional[str]:
        """
        Get header value by name (Laravel-style).

        Args:
            name: Header name
            default: Default value if not found

        Returns:
            str: Header value or default
        """
        return self.header_bag.get(name, default)

    def has(self, name: str) -> bool:
        """
        Check if header exists (Laravel-style).

        Args:
            name: Header name

        Returns:
            bool: True if header exists
        """
        return name.lower() in self.header_bag

    def remove(self, name: str) -> None:
        """
        Remove header by name (Laravel-style).

        Args:
            name: Header name to remove
        """
        name_lower = name.lower()
        if name_lower in self.header_bag.bag:
            del self.header_bag.bag[name_lower]

    def merge(self, headers: Dict[str, str]) -> None:
        """
        Merge multiple headers (Laravel-style).

        Args:
            headers: Dictionary of headers to merge
        """
        for name, value in headers.items():
            self.set(name, value)

    def all(self) -> Dict[str, str]:
        """
        Get all headers as dictionary (Laravel-style).

        Returns:
            Dict[str, str]: All headers
        """
        return self.header_bag.to_dict()

    def to_asgi(self) -> List[Tuple[bytes, bytes]]:
        """
        Get headers formatted for ASGI.

        Returns:
            List[Tuple[bytes, bytes]]: Headers for ASGI
        """
        return self.header_bag.render()

    def clear(self) -> None:
        """Clear all headers (Laravel-style)."""
        self.header_bag = HeaderBag()
        self._content_type_explicitly_set = False

    # =============================================================================
    # CONTENT-TYPE MANAGEMENT (Laravel-style)
    # =============================================================================

    def content_type(self, type_: str = None) -> Union[Optional[str], None]:
        """
        Get or set Content-Type header (Laravel-style).

        Args:
            type_: Content-Type value (if setting)

        Returns:
            str: Content-Type value (if getting)
        """
        if type_ is None:
            return self.get("Content-Type")

        self.set("Content-Type", type_)

    def is_content_type_explicit(self) -> bool:
        """
        Check if Content-Type was explicitly set.

        Returns:
            bool: True if Content-Type was explicitly set
        """
        return self._content_type_explicitly_set

    # =============================================================================
    # COMMON HEADERS (Laravel-style shortcuts)
    # =============================================================================

    def cache_control(self, value: str) -> None:
        """Set Cache-Control header (Laravel-style)."""
        self.set("Cache-Control", value)

    def content_length(self, length: int) -> None:
        """Set Content-Length header (Laravel-style)."""
        self.set("Content-Length", str(length))

    def location(self, url: str) -> None:
        """Set Location header for redirects (Laravel-style)."""
        self.set("Location", url)

    def authorization(self, token: str) -> None:
        """Set Authorization header (Laravel-style)."""
        self.set("Authorization", token)

    # =============================================================================
    # CORS HEADERS (Laravel-style)
    # =============================================================================

    def cors(
        self,
        origin: str = "*",
        methods: str = "GET, POST, PUT, DELETE, OPTIONS",
        headers: str = "Content-Type, Authorization",
    ) -> None:
        """
        Set CORS headers (Laravel-style).

        Args:
            origin: Allowed origin
            methods: Allowed methods
            headers: Allowed headers
        """
        self.merge(
            {
                "Access-Control-Allow-Origin": origin,
                "Access-Control-Allow-Methods": methods,
                "Access-Control-Allow-Headers": headers,
            }
        )

    def cors_credentials(self, allow: bool = True) -> None:
        """Set CORS credentials header (Laravel-style)."""
        self.set("Access-Control-Allow-Credentials", "true" if allow else "false")

    def cors_max_age(self, seconds: int = 86400) -> None:
        """Set CORS max age header (Laravel-style)."""
        self.set("Access-Control-Max-Age", str(seconds))

    # =============================================================================
    # SECURITY HEADERS (Laravel-style)
    # =============================================================================

    def secure(self) -> None:
        """Set common security headers (Laravel-style)."""
        self.merge(
            {
                "X-Content-Type-Options": "nosniff",
                "X-Frame-Options": "DENY",
                "X-XSS-Protection": "1; mode=block",
                "Referrer-Policy": "strict-origin-when-cross-origin",
            }
        )

    def no_cache(self) -> None:
        """Set no-cache headers (Laravel-style)."""
        self.merge(
            {
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            }
        )

    def csp(self, policy: str) -> None:
        """Set Content Security Policy header (Laravel-style)."""
        self.set("Content-Security-Policy", policy)

    def hsts(self, max_age: int = 31536000, include_subdomains: bool = True) -> None:
        """Set HTTP Strict Transport Security header (Laravel-style)."""
        value = f"max-age={max_age}"
        if include_subdomains:
            value += "; includeSubDomains"
        self.set("Strict-Transport-Security", value)

    # =============================================================================
    # UTILITY METHODS
    # =============================================================================

    def finalize(self, content_length: int, default_content_type: str = None) -> None:
        """
        Finalize headers before sending (Laravel-style).

        Args:
            content_length: Content length in bytes
            default_content_type: Default content-type if not set
        """
        # Always set Content-Length
        self.content_length(content_length)

        # Set Content-Type only if not explicitly set
        if not self._content_type_explicitly_set and default_content_type:
            self.content_type(default_content_type)

    def copy_from(self, other: "HeaderManager") -> None:
        """
        Copy headers and state from another HeaderManager instance.

        This method performs a deep copy of all header data and internal state
        from the source HeaderManager to this instance.

        Args:
            other: The HeaderManager instance to copy from

        Note:
            This method preserves the explicit content-type flag and all
            header data. It's used during Response cloning operations.
        """
        if not other:
            return

        # Deep copy the header bag to avoid shared references
        self.header_bag = other.header_bag.copy()

        # Preserve the explicit content-type state
        self._content_type_explicitly_set = other._content_type_explicitly_set

    def __repr__(self) -> str:
        """String representation of HeaderManager."""
        headers_count = len(self.header_bag.to_dict())
        content_type_status = "explicit" if self._content_type_explicitly_set else "auto"
        return (
            f"HeaderManager(headers={headers_count}, content_type={content_type_status})"
        )

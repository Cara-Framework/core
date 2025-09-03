"""
Request Helpers Mixin for HTTP Request.

This mixin provides utility helper methods for URL handling, path matching,
and query parameter access.
"""

import re
from typing import Any


class RequestHelpersMixin:
    """
    Mixin providing utility helper methods for HTTP requests.

    Includes URL construction, path matching, and query parameter access methods.
    """

    def query(self, key: str = None, default: Any = None) -> Any:
        """
        Query parameter access.

        Args:
            key: Query parameter key (if None, returns all)
            default: Default value if key not found

        Returns:
            Query parameter value or all query params
        """
        if key is None:
            return self._input.all()
        return self._input.get(key, default)

    def url(self) -> str:
        """
        URL without query string.

        Returns:
            str: Current URL without query parameters
        """
        scheme = self.scope.get("scheme", "http")
        host = self.get_host()
        path = self.path
        return f"{scheme}://{host}{path}"

    def fullUrl(self) -> str:
        """
        Full URL with query string.

        Returns:
            str: Complete URL including query parameters
        """
        base_url = self.url()
        query_string = self.scope.get("query_string", b"").decode()
        if query_string:
            return f"{base_url}?{query_string}"
        return base_url

    def is_path(self, *patterns: str) -> bool:
        """
        Path matching with wildcard support.

        Args:
            *patterns: Path patterns to match (supports wildcards)

        Returns:
            bool: True if current path matches any pattern
        """
        current_path = self.path.lstrip("/")

        for pattern in patterns:
            pattern = pattern.lstrip("/")

            # Convert wildcard to regex
            regex_pattern = pattern.replace("*", ".*")
            regex_pattern = f"^{regex_pattern}$"

            if re.match(regex_pattern, current_path):
                return True

        return False

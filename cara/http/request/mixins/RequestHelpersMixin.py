"""
Request Helpers Mixin for HTTP Request.

This mixin provides utility helper methods for URL handling, path matching,
and query parameter access.
"""

import re
from typing import Any, List, Optional

from cara.exceptions.types.authentication import AuthenticationException
from cara.exceptions.types.validation import ValidationException


class RequestHelpersMixin:
    """
    Mixin providing utility helper methods for HTTP requests.

    Includes URL construction, path matching, and query parameter access methods.
    """

    async def integer(self, key: str, default: int = 0) -> int:
        """Retrieve input as int, returning default on missing/invalid values."""
        value = await self.input(key)
        if value is None or value == "":
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    async def float_val(self, key: str, default: float = 0.0) -> float:
        """Retrieve input as float, returning default on missing/invalid values."""
        value = await self.input(key)
        if value is None or value == "":
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    async def array_or_csv(self, key: str, default: Optional[List[Any]] = None) -> List[Any]:
        """Retrieve input that may arrive as a JSON array OR a comma-separated string.

        Common for query/body params that can be either ``?ids=1,2,3`` or
        ``?ids[]=1&ids[]=2``. Returns an empty list (or ``default``) when the
        input is missing.
        """
        value = await self.input(key)
        if value is None or value == "":
            return list(default) if default is not None else []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return [p.strip() for p in value.split(",") if p.strip()]
        return [value]

    async def id_list(
        self,
        key: str,
        max_ids: int = 100,
        required: bool = False,
    ) -> List[int]:
        """Parse input as a list of positive integers (JSON array OR CSV).

        Raises ``ValidationException`` (HTTP 422) if the input contains
        non-integer values, zero/negative ids, or exceeds ``max_ids``. Returns
        an empty list when missing (unless ``required=True``).
        """
        raw = await self.array_or_csv(key)
        if not raw:
            if required:
                raise ValidationException(
                    validation_errors={key: [f"{key} is required"]}
                )
            return []
        if len(raw) > max_ids:
            raise ValidationException(
                validation_errors={
                    key: [f"{key} may contain at most {max_ids} ids"],
                }
            )
        out: List[int] = []
        for item in raw:
            try:
                n = int(item)
            except (TypeError, ValueError):
                raise ValidationException(
                    validation_errors={key: [f"{key} must contain integers"]}
                )
            if n <= 0:
                raise ValidationException(
                    validation_errors={key: [f"{key} must contain positive ids"]}
                )
            out.append(n)
        return out

    def user_or_401(self) -> Any:
        """Return ``self.user`` or raise AuthenticationException (HTTP 401).

        Controllers pair this with auth middleware — if middleware is
        misconfigured or the user context is missing, this surfaces a
        predictable 401 instead of a 500.
        """
        user = getattr(self, "user", None)
        if user is None:
            raise AuthenticationException("Authentication required")
        return user

    def query(self, key: Optional[str] = None, default: Any = None) -> Any:
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

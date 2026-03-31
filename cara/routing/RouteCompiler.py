"""
Route compiler for URL pattern compilation.

This module handles the transformation of URL patterns into regex matchers and provides parameter
extraction functionality in the Cara framework.
"""

import re
from typing import Dict, Any, List


class RouteCompiler:
    """Handles route compilation and parameter extraction."""

    def __init__(self, url: str, compilers: Dict[str, str]):
        self._compiled_regex = None
        self.url_list: List[str] = []
        self.compilers = compilers or {"default": r"([^/]+)"}
        self.compile_route(url)

    def compile_route(self, url: str) -> str:
        """Compile a route URL into a regex, tracking parameter names."""
        parts = url.strip("/").split("/")
        regex = "^"
        url_list: List[str] = []

        for part in parts:
            if part == "":
                continue

            # Required parameter: "@id" or "@id:int"
            if part.startswith("@") and not part.endswith("?"):
                name, _, compiler_name = part[1:].partition(":")
                pattern = self.compilers.get(compiler_name, self.compilers["default"])
                regex += f"/{pattern}"
                url_list.append(name)

            # Optional parameter in Laravel style: "@id?" or "@id:int?"
            elif part.startswith("@") and part.endswith("?"):
                raw = part[1:-1]  # strip "@" and "?"
                name, _, compiler_name = raw.partition(":")
                pattern = self.compilers.get(compiler_name, self.compilers["default"])
                # Wrap slash+pattern in a single optional non-capturing group
                regex += f"(?:/{pattern})?"
                url_list.append(name)

            # Static segment
            else:
                regex += f"/{re.escape(part)}"

        # Allow an optional trailing slash, then anchor end
        regex += r"/?$"
        self.url_list = url_list
        self._compiled_regex = re.compile(regex)
        return regex

    def extract_parameters(self, path: str) -> Dict[str, Any]:
        """Extract parameters from a given path using the compiled regex."""
        if not self._compiled_regex:
            return {}

        match = self._compiled_regex.match(path)
        if not match:
            return {}

        raw_groups = match.groups()
        params: Dict[str, Any] = {}
        # Iterate over named positions; if a group is None, substitute empty string
        for idx, name in enumerate(self.url_list):
            value = raw_groups[idx] if idx < len(raw_groups) else None
            params[name] = value if value is not None else ""
        return params

    def matches(self, path: str) -> bool:
        """Check if the route matches the given path."""
        return bool(self._compiled_regex.match(path)) if self._compiled_regex else False

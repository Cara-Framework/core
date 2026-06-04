"""
Route compiler for URL pattern compilation.

This module handles the transformation of URL patterns into regex matchers and provides parameter
extraction functionality in the Cara framework.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import unquote


class RouteCompiler:
    """Handles route compilation and parameter extraction."""

    def __init__(self, url: str, compilers: dict[str, str]):
        self._compiled_regex = None
        self.url_list: list[str] = []
        # Maps parameter name → compiler type name (e.g. "id" → "int")
        self.param_types: dict[str, str] = {}
        self.compilers = compilers or {"default": r"([^/]+)"}
        self.compile_route(url)

    def compile_route(self, url: str) -> str:
        """Compile a route URL into a regex, tracking parameter names."""
        parts = url.strip("/").split("/")
        regex = "^"
        url_list: list[str] = []

        for part in parts:
            if part == "":
                continue

            # Required parameter: "@id" or "@id:int"
            if part.startswith("@") and not part.endswith("?"):
                name, _, compiler_name = part[1:].partition(":")
                pattern = self.compilers.get(compiler_name, self.compilers["default"])
                regex += f"/{pattern}"
                url_list.append(name)
                if compiler_name:
                    self.param_types[name] = compiler_name

            # Optional parameter in Laravel style: "@id?" or "@id:int?"
            elif part.startswith("@") and part.endswith("?"):
                raw = part[1:-1]  # strip "@" and "?"
                name, _, compiler_name = raw.partition(":")
                pattern = self.compilers.get(compiler_name, self.compilers["default"])
                # Wrap slash+pattern in a single optional non-capturing group
                regex += f"(?:/{pattern})?"
                url_list.append(name)
                if compiler_name:
                    self.param_types[name] = compiler_name

            # Static segment
            else:
                regex += f"/{re.escape(part)}"

        # Allow an optional trailing slash, then anchor end
        regex += r"/?$"
        self.url_list = url_list
        self._compiled_regex = re.compile(regex)
        return regex

    def extract_parameters(self, path: str) -> dict[str, Any]:
        r"""Extract parameters from a given path using the compiled regex.

        Percent-decoding policy
        -----------------------
        Matched parameter values are passed through ``urllib.parse.unquote``
        so a slug like ``caf%C3%A9`` lands on the handler as ``café``.

        - Compliant ASGI servers (uvicorn, hypercorn) already pre-decode
          ``scope['path']`` per the ASGI HTTP spec — for those, ``unquote``
          is idempotent (``unquote('café') == 'café'``) and the call is
          a cheap no-op.
        - Non-compliant servers, mounted middleware that bypasses path
          decoding, and test fixtures that pass raw URLs all leak the
          raw percent-encoded value into the binding pre-fix.
          ``Product.where('slug', 'caf%C3%A9')`` silently misses the
          ``café`` row and the user sees a phantom 404.
        - ``%2F`` (encoded slash) stays segregated by the time we get
          here: the ``[^/]+`` / ``[\w-]+`` regexes don't backtrack
          across literal ``/`` boundaries so the *match shape* still
          treats each path segment as one slug. Decoding the captured
          value afterwards is safe — it cannot retroactively expand
          one segment into two.

        Pre-fix: zero decoding. Verified by ``RouteCompiler('@slug').
        extract_parameters('/caf%C3%A9')`` returning ``{'slug':
        'caf%C3%A9'}`` instead of ``{'slug': 'café'}``.

        Optional params stay None when missing (not empty string) so
        callers can distinguish "absent" from "empty".
        """
        if not self._compiled_regex:
            return {}

        match = self._compiled_regex.match(path)
        if not match:
            return {}

        raw_groups = match.groups()
        params: dict[str, Any] = {}
        for idx, name in enumerate(self.url_list):
            if idx >= len(raw_groups):
                params[name] = None
                continue
            raw = raw_groups[idx]
            if raw is None:
                # Optional param that didn't match — preserve None so
                # callers see "absent" rather than empty string.
                params[name] = None
                continue
            # Decode percent-encoded bytes. Wrapped in try/except to
            # ensure a malformed percent triplet (e.g. ``%ZZ``) can't
            # crash the router — fall back to the raw value so the
            # handler can decide what to do.
            try:
                params[name] = unquote(raw)
            except (UnicodeDecodeError, ValueError):
                params[name] = raw
        return params

    def matches(self, path: str) -> bool:
        """Check if the route matches the given path."""
        return bool(self._compiled_regex.match(path)) if self._compiled_regex else False

"""
Route decorator for defining HTTP routes.

This module provides decorators for registering routes with support for
HTTP methods, middleware, and route grouping in the Cara framework.

Supported extras (passed as kwargs):
  - prefix: str                # route group prefix
  - namespace: str             # controller namespace
  - middleware: List[Any]      # list of middleware classes or callables
"""

from __future__ import annotations

from .RouteDecorator import RouteDecorator

# Strongly‐typed metadata shape for clarity


# Global decorator instance
route = RouteDecorator()


_pending_routes: list[dict] = []


def all_pending() -> list[dict]:
    """Return pending routes collected from @route decorators."""
    return _pending_routes.copy()


def clear() -> None:
    """Clear pending routes after they have been loaded."""
    _pending_routes.clear()

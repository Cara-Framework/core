"""Shared builders for the middleware tests.

Leading underscore: a test-support module, not a test file itself (mirrors
``tests/architecture/_fixtures.py`` and ``tests/docs/_fixtures.py``).
"""

from __future__ import annotations

from cara.middleware.http.ThrottleRequests import ThrottleRequests


def throttle_middleware(limit=None, window=None) -> ThrottleRequests:
    """Build the middleware without the provider boot the base
    ``Middleware.__init__`` triggers."""
    middleware = ThrottleRequests.__new__(ThrottleRequests)
    middleware.custom_limit = limit
    middleware.custom_window_minutes = window
    return middleware

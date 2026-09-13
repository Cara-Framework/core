"""Shared builders for the middleware tests.

Leading underscore: a test-support module, not a test file itself (mirrors
``tests/architecture/_fixtures.py`` and ``tests/docs/_fixtures.py``).
"""

from __future__ import annotations

from cara.middleware.http.ThrottleRequests import ThrottleRequests


def throttle_middleware(limiter=None) -> ThrottleRequests:
    """Build the middleware without the provider boot the base
    ``Middleware.__init__`` triggers."""
    middleware = ThrottleRequests.__new__(ThrottleRequests)
    middleware.limiter_name = limiter
    return middleware

"""The headers ``ThrottleRequests`` sends, as an OpenAPI response-header contract.

A product maps its throttle alias family to it —
``middleware_headers={"throttle:*": RATE_LIMIT_HEADERS}`` — and every operation
behind a named limiter documents the IETF pair on the responses it returns and
``Retry-After`` on the 429. ``tests/middleware/test_throttle_requests_contract.py``
holds the middleware to exactly these names.
"""

from __future__ import annotations

from .MiddlewareHeaders import MiddlewareHeaders

RATE_LIMIT_HEADERS = MiddlewareHeaders(
    components={
        "RateLimit-Policy": {
            "description": (
                "The named quota this operation spends: q requests per w seconds "
                "(draft-ietf-httpapi-ratelimit-headers)."
            ),
            "schema": {"type": "string"},
            "example": '"api";q=600;w=60',
        },
        "RateLimit": {
            "description": (
                "What is left of that quota: r requests remain, and the bucket is "
                "full again in t seconds."
            ),
            "schema": {"type": "string"},
            "example": '"api";r=119;t=1',
        },
        "Retry-After": {
            "description": "Seconds until one more request fits the quota.",
            "schema": {"type": "integer", "minimum": 0},
        },
    },
    passed=("RateLimit-Policy", "RateLimit"),
    refused={429: ("Retry-After", "RateLimit-Policy", "RateLimit")},
)

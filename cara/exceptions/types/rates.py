"""
Rate Limiting Exception Type for the Cara framework.

This module defines exception types related to rate limiting operations.
"""

from .base import CaraException


class RateLimitConfigurationException(CaraException):
    """Thrown when rate‐limit configuration is missing or invalid."""

    pass

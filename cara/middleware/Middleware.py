# cara/middleware/Middleware.py

"""
Core Middleware base class for Cara framework.
Laravel-style middleware with automatic parameter parsing and dependency injection.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import (
    Any,
)

from .MiddlewareParameterParser import MiddlewareParameterParser


class Middleware(ABC):
    """
    Base class for all middleware with Laravel-style parameter parsing.

    Supports automatic parameter parsing from method signatures:
    - middleware:param1,param2 -> __init__(self, application, param1, param2)
    - Type hints for automatic conversion: int, bool, List[str], Optional[str]
    - Default values supported
    """

    def __init__(self, application: Any, **kwargs):
        self.application = application

        # Store parsed parameters as attributes
        for key, value in kwargs.items():
            setattr(self, key, value)

    @abstractmethod
    async def handle(self, request: Any, next_fn: Callable[[Any], Awaitable[Any]]) -> Any:
        """Handle the request/context. Must be implemented by all middleware."""
        pass

    async def terminate(self, request: Any, response: Any) -> None:
        """Called after response is sent. Override for terminable middleware."""
        pass

    @classmethod
    def create_with_parameters(
        cls, application: Any, parameters: list[str] | None = None
    ) -> Middleware:
        """
        Factory method to create middleware instance with automatic parameter parsing.
        Uses method signature inspection for type-safe parameter injection.
        """
        return MiddlewareParameterParser.parse_and_create(
            cls, application, parameters or []
        )

    @classmethod
    def with_parameters(cls, *parameters: Any) -> Callable[[Any], Middleware]:
        """Laravel-style factory for manual parameter setting."""

        def factory(application: Any) -> Middleware:
            return cls.create_with_parameters(application, [str(p) for p in parameters])

        return factory

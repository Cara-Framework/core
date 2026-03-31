"""
Base provider definitions for service registration and bootstrapping.

This module defines Provider (always‐eager) which all concrete providers should extend.
"""

from __future__ import annotations

from abc import ABC


class Provider(ABC):
    """
    Base Provider class that all (non‐deferred) service providers should extend.

    Subclasses should implement:
        def register(self) -> None:   # to bind services into the container
        def boot(self) -> None:       # (optional) any post‐registration steps
    """

    def __init__(self, application: Application) -> None:
        """
        Initialize the provider.

        Args:
            application: The Application (IoC container) instance
        """
        self.application = application

    def register(self) -> None:
        """
        Register any application services into the container.

        To be overridden by subclasses.
        """
        pass

    def boot(self) -> None:
        """
        Boot any application services (after all providers are registered).

        To be overridden by subclasses if needed.
        """
        pass

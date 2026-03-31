"""
View Provider - Service provider for Cara view engine

This file provides the service provider for the view engine.
"""

from typing import List

from cara.configuration import config
from cara.foundation import DeferredProvider
from cara.support import paths
from cara.view import View, ViewEngine


class ViewProvider(DeferredProvider):
    """Service provider for view engine."""

    @classmethod
    def provides(cls) -> list[str]:
        return ["view"]

    def register(self):
        """Register view services with configuration."""
        settings = config("view", {})

        # Create view engine and main factory
        self._add_view_engine(settings)

    def _add_view_engine(self, settings: dict) -> None:
        """Register view engine with configuration."""
        engine = ViewEngine(
            view_paths=self._get_view_paths(settings),
            cache_path=self._get_cache_path(settings),
        )

        # Create main View factory with engine
        view_factory = View(engine)

        # Bind the main View factory as "view"
        self.application.bind("view", view_factory)

    def _get_view_paths(self, settings: dict) -> List[str]:
        """Get view paths from configuration."""

        # Use configured paths or defaults
        configured_paths = settings.get("paths", [])
        if configured_paths:
            return configured_paths

        # Default paths
        return [
            paths("views"),
        ]

    def _get_cache_path(self, settings: dict) -> str:
        """Get cache path from configuration."""

        # Use configured cache path or default
        cache_path = settings.get("cache_path")
        if cache_path:
            return cache_path

        return paths("storage", "framework")

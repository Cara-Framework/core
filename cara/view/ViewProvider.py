"""
View Provider - Service provider for Cara view engine

This file provides the service provider for the view engine.
"""

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
        view_paths = config("view.paths") or [paths("views")]
        cache_path = config("view.cache_path") or paths("storage", "framework")

        engine = ViewEngine(view_paths=view_paths, cache_path=cache_path)
        self.application.bind("view", View(engine))

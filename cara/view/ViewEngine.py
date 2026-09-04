"""
View Engine - resolves the template search paths for the mail renderer.

Mail and notification templates are the only templates cara renders, and
they are authored in standard Jinja2 (see ``cara.mail.JinjaRenderer``).
This engine therefore owns nothing but the search paths plus the renderer
handle; there is no Blade-style compiler behind it any more.
"""

from __future__ import annotations

from cara.support import paths


class ViewEngine:
    """Template search-path holder for the Jinja2 mail renderer."""

    def __init__(self, view_paths: list[str] | None = None):
        """Initialize view engine."""
        self.view_paths = view_paths or [paths("views")]

    def get_renderer(self, factory=None):
        """Get renderer instance."""
        from cara.view.ViewRenderer import (
            ViewRenderer,  # local: cycle with cara.view.ViewRenderer
        )

        return ViewRenderer(self, factory)

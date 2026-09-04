"""
View - Main view factory for Cara framework

This file provides the main View factory functionality. Cara renders mail
and notification templates only; the engine behind it is Jinja2.
"""

from __future__ import annotations

from typing import Any

from cara.view.ViewEngine import ViewEngine


class View:
    """Main View factory bound as the ``view`` service."""

    def __init__(self, engine: ViewEngine | None = None):
        """Initialize view factory."""
        self.engine = engine or ViewEngine()

    def render_mail(self, view: str, data: dict[str, Any] = None) -> str:
        """Render mail template with mail-specific data."""
        renderer = self.engine.get_renderer(factory=self)
        return renderer.render_mail_template(view, data)

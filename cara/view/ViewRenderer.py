"""
View Renderer - Rendering functionality for Cara view engine

This file provides view rendering capabilities.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from cara.mail import render_mail_view
from cara.view.ViewEngine import ViewEngine


class ViewRenderer:
    """View renderer for processing and rendering templates."""

    def __init__(self, engine: ViewEngine | None = None, factory=None):
        """Initialize view renderer."""
        self.engine = engine or ViewEngine()
        self.factory = factory

    def render_mail_template(self, template: str, data: dict[str, Any] = None) -> str:
        """Render a mail template through the dedicated Jinja2 renderer.

        Mail / notification templates under ``resources/views/mail`` are
        authored in standard Jinja2 (``| default`` / ``| float`` / ``| length``
        filters, ``~`` concatenation, ``{% for %}`` loops), and Jinja2
        (autoescape on) is the only template engine cara ships.
        """

        mail_data = {
            "app_name": "Cara Application",
            "app_url": "http://localhost",
        }

        # Merge with provided data
        if data:
            mail_data.update(data)

        # ``render_mail_view`` finds its search paths by resolving ``view``
        # from the container it is handed. The ``View`` factory carries no
        # ``application``, so it was handed ``None``, the lookup failed and
        # the renderer fell back to ``paths("views")`` — the engine's
        # configured ``view_paths`` (``config("view.paths")``) never applied.
        # Handing it a resolver bound to THIS engine renders from exactly the
        # paths this renderer was built with.
        engine = self.engine
        scope = SimpleNamespace(make=lambda _abstract: engine)
        return render_mail_view(scope, template, mail_data)

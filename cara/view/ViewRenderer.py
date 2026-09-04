"""
View Renderer - Rendering functionality for Cara view engine

This file provides view rendering capabilities.
"""

from __future__ import annotations

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

        application = getattr(self.factory, "application", None)
        return render_mail_view(application, template, mail_data)

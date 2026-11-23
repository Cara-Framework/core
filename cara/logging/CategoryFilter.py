"""
Logging Category Filter for the Cara framework.

This module provides a filter for categorizing log records by category.
"""

from __future__ import annotations

import logging as pylogging

from cara.configuration import config


class CategoryFilter:
    """
    Checks per-category log level in logging configuration.

    Categories are FLAT keys of the ``logging.categories`` section
    (``"cara.http.requests": {"enabled": ..., "level": ...}``), so the
    category is looked up as one key of that section, never as a dotted
    configuration path: the configuration layer splits paths on dots, and
    ``logging.categories.cara.http.requests`` walks into a ``cara`` key that
    does not exist.

    Rules:

    - No category: always allow.
    - Unknown category (or one without settings): allowed at DEBUG and up.
    - ``level``: records below it are dropped.
    - ``enabled: False`` (or a category set to ``None``) silences records
      BELOW ERROR only. ERROR and CRITICAL always pass: a category switch is a
      noise control, and it must never hide an error.
    """

    @staticmethod
    def should_log(level: str, category: str | None) -> bool:
        """
        Check if a log message should be logged based on category configuration.

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            category: Category name (e.g., 'cara.http.requests', 'database')

        Returns:
            True if the message should be logged, False otherwise
        """
        # If no category specified, always log
        if not category:
            return True

        levelno = getattr(pylogging, str(level).upper(), None)
        if not isinstance(levelno, int):
            # If level parsing fails, allow logging
            return True

        # ONE read of the section, then plain dict lookups: this runs on every
        # categorized log call (each SQL statement, each request line).
        categories = config("logging.categories", None)
        if not isinstance(categories, dict) or category not in categories:
            return True
        settings = categories[category]
        if settings is None:
            # A category set to None reads as disabled.
            settings = {"enabled": False}
        elif not isinstance(settings, dict):
            settings = {}

        if not settings.get("enabled", True) and levelno < pylogging.ERROR:
            return False

        cat_level = settings.get("level") or "DEBUG"
        cat_levelno = getattr(pylogging, str(cat_level).upper(), None)
        if not isinstance(cat_levelno, int):
            return True
        return levelno >= cat_levelno

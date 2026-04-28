"""
Logging Category Filter for the Cara framework.

This module provides a filter for categorizing log records by category.
"""

import logging as pylogging
from typing import Optional

from cara.configuration import config


class CategoryFilter:
    """
    Checks per-category log level in logging configuration.

    If no category is specified, always allow.
    """

    @staticmethod
    def should_log(level: str, category: Optional[str]) -> bool:
        """
        Check if a log message should be logged based on category configuration.

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            category: Category name (e.g., 'db.queries', 'http.requests')

        Returns:
            True if the message should be logged, False otherwise
        """
        # If no category specified, always log
        if not category:
            return True

        enabled = config(f"logging.categories.{category}.enabled", True)
        if not enabled:
            return False

        try:
            levelno = getattr(pylogging, level.upper(), 0)
            cat_level = config(f"logging.categories.{category}.level", "DEBUG")
            cat_levelno = getattr(pylogging, (cat_level or "DEBUG").upper(), 0)
            return levelno >= cat_levelno
        except AttributeError:
            # If level parsing fails, allow logging
            return True

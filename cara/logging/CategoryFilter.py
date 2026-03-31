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

        # Get categories from config and convert to pure Python dict
        categories_cfg = config("logging.categories", {})
        if hasattr(categories_cfg, "_data"):
            categories_cfg = categories_cfg._data

        # Get category configuration
        cat_cfg = categories_cfg.get(category, {})

        # If no configuration for this category, allow logging
        if not cat_cfg:
            return True

        # Check if category is enabled
        enabled = cat_cfg.get("enabled", True)

        if not enabled:
            return False

        # Check log level
        try:
            levelno = getattr(pylogging, level.upper(), 0)
            cat_levelno = getattr(pylogging, cat_cfg.get("level", "DEBUG").upper(), 0)
            return levelno >= cat_levelno
        except AttributeError:
            # If level parsing fails, allow logging
            return True

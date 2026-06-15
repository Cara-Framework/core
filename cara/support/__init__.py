"""
Support Module Initialization.

This module provides core support classes and utilities for the Cara framework. It includes the
Collection class which offers a fluent, Laravel-style interface for working with arrays and data
collections.
"""

from .BestEffort import best_effort, best_effort_ctx
from .Collection import Collection, collect, flatten
from .Currency import currency_symbol, default_currency, format_money
from .Date import Date
from .HtmlString import HtmlString
from .Image import Image
from .Macroable import Macroable
from .Manager import Manager
from .ModuleManager import ModuleManager
from .Number import safe_divide_decimal, to_decimal
from .PathManager import PathManager
from .Pipeline import Pipeline
from .Process import Process, ProcessFailedException, ProcessResult
from .Retry import Retry
from .Sleep import Sleep
from .Str import (
    email_mask,
    sanitize_text,
    slugify,
    strip_tags,
)
from .SupportProvider import SupportProvider
from .Time import to_pendulum
from .modules import get_classes, modules
from .paths import base_path, paths, public_path, storage_path

__all__ = [
    "Collection",
    "Date",
    "HtmlString",
    "Image",
    "Macroable",
    "Manager",
    "ModuleManager",
    "PathManager",
    "Pipeline",
    "Process",
    "ProcessFailedException",
    "ProcessResult",
    "Retry",
    "Sleep",
    "SupportProvider",
    "base_path",
    "best_effort",
    "best_effort_ctx",
    "collect",
    "currency_symbol",
    "default_currency",
    "email_mask",
    "flatten",
    "format_money",
    "get_classes",
    "modules",
    "paths",
    "public_path",
    "safe_divide_decimal",
    "sanitize_text",
    "slugify",
    "storage_path",
    "strip_tags",
    "to_decimal",
    "to_pendulum",
]

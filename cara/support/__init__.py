"""
Support Module Initialization.

This module provides core support classes and utilities for the Cara framework. It includes the
Collection class which offers a fluent, Laravel-style interface for working with arrays and data
collections.
"""

from .Arr import Arr
from .Collection import Collection, collect, flatten
from .CommandPipeline import workflow
from .Conditionable import Conditionable
from .Macroable import Macroable
from .Tappable import Tappable
from .Stringable import Stringable
from .Once import Once, once
from .Fluent import Fluent
from .HtmlString import HtmlString
from .Lottery import Lottery
from .Sleep import Sleep
from .Benchmark import Benchmark
from .Uri import Uri
from .Reflector import Reflector
from .Timebox import Timebox
from .Date import Date
from .Process import Process, ProcessResult, ProcessFailedException
from .Defer import Defer, defer
from .Manager import Manager
from .HigherOrderTapProxy import HigherOrderTapProxy
from .Image import Image
from .ModuleManager import ModuleManager
# Dynamic helpers (Laravel-style)
from .modules import (app, config, get_classes, get_functions, import_module,
                      m, module_exists, modules, routes)
from .PathManager import PathManager
from .paths import app as app_paths
from .paths import app_path, base_path
from .paths import config as config_paths
from .paths import config_path, p, paths, public_path, storage, storage_path
from .Currency import currency_symbol, default_currency, format_money
from .Number import clamp
from .Pipeline import Pipeline
from .Str import (
    after, before, between, camel_case, contains, email_mask, ends_with,
    format_money_cents, kebab_case, mask, normalize_email, pluralize,
    sanitize_text, slugify, snake_case, starts_with, strip_tags, studly_case,
    title_case, truncate, ulid, uuid,
)
from .SupportProvider import SupportProvider
from .Time import format_duration, humanize_seconds, to_pendulum

__all__ = [
    "Arr",
    "Collection",
    "Conditionable",
    "Macroable",
    "Tappable",
    "Stringable",
    "Once",
    "once",
    "Fluent",
    "HtmlString",
    "Lottery",
    "Sleep",
    "Benchmark",
    "Uri",
    "Reflector",
    "Timebox",
    "Date",
    "Process",
    "ProcessResult",
    "ProcessFailedException",
    "Defer",
    "defer",
    "Manager",
    "HigherOrderTapProxy",
    "collect",
    "flatten",
    "SupportProvider",
    "Pipeline",
    "PathManager",
    "ModuleManager",
    "Image",

    # Dynamic API (main functions)
    "modules", "paths",

    # Utility functions
    "module_exists", "import_module", "get_classes", "get_functions",
    "storage_path", "config_path", "public_path", "app_path", "base_path",

    # String utilities
    "slugify", "normalize_email", "email_mask", "format_money_cents",
    "truncate", "title_case", "snake_case", "kebab_case", "camel_case", "studly_case",
    "pluralize", "strip_tags", "sanitize_text",
    # Laravel-parity Str helpers
    "uuid", "ulid",
    "starts_with", "ends_with", "contains",
    "before", "after", "between", "mask",

    # Currency utilities
    "default_currency", "currency_symbol", "format_money",

    # Number utilities
    "clamp",

    # Time utilities
    "humanize_seconds", "format_duration", "to_pendulum",

    # Short aliases
    "m", "p",
    "workflow",

    # Dynamic helpers
    "app", "routes", "config", "storage", "config_paths", "app_paths",
]

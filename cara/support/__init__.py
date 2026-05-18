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
from .modules import (
    app,
    config,
    get_classes,
    get_functions,
    import_module,
    m,
    module_exists,
    modules,
    routes,
)
from .PathManager import PathManager
from .paths import app as app_paths
from .paths import app_path, base_path
from .paths import config as config_paths
from .paths import config_path, p, paths, public_path, storage, storage_path
from .Currency import currency_symbol, default_currency, format_money
from .Number import clamp
from .Pipeline import Pipeline
from .Str import (
    after,
    before,
    between,
    camel_case,
    contains,
    email_mask,
    ends_with,
    format_money_cents,
    kebab_case,
    mask,
    normalize_email,
    pluralize,
    sanitize_text,
    slugify,
    snake_case,
    starts_with,
    strip_tags,
    studly_case,
    title_case,
    truncate,
    ulid,
    uuid,
)
from .SupportProvider import SupportProvider
from .Time import format_duration, humanize_seconds, to_pendulum

__all__ = [
    "after",
    "app",
    "app_path",
    "app_paths",
    "Arr",
    "base_path",
    "before",
    "Benchmark",
    "between",
    "camel_case",
    "clamp",
    "collect",
    "Collection",
    "Conditionable",
    "config",
    "config_path",
    "config_paths",
    "contains",
    "currency_symbol",
    "Date",
    "default_currency",
    "Defer",
    "defer",
    "email_mask",
    "ends_with",
    "flatten",
    "Fluent",
    "format_duration",
    "format_money",
    "format_money_cents",
    "get_classes",
    "get_functions",
    "HigherOrderTapProxy",
    "HtmlString",
    "humanize_seconds",
    "Image",
    "import_module",
    "kebab_case",
    "Lottery",
    "m",
    "Macroable",
    "Manager",
    "mask",
    "module_exists",
    "ModuleManager",
    "modules",
    "normalize_email",
    "Once",
    "once",
    "p",
    "PathManager",
    "paths",
    "Pipeline",
    "pluralize",
    "Process",
    "ProcessFailedException",
    "ProcessResult",
    "public_path",
    "Reflector",
    "routes",
    "sanitize_text",
    "Sleep",
    "slugify",
    "snake_case",
    "starts_with",
    "storage",
    "storage_path",
    "Stringable",
    "strip_tags",
    "studly_case",
    "SupportProvider",
    "Tappable",
    "Timebox",
    "title_case",
    "to_pendulum",
    "truncate",
    "ulid",
    "Uri",
    "uuid",
    "workflow",
]

"""
Support Module Initialization.

This module provides core support classes and utilities for the Cara framework. It includes the
Collection class which offers a fluent, Laravel-style interface for working with arrays and data
collections.
"""

from .Arr import Arr
from .Collection import Collection, collect, flatten
from .Conditionable import Conditionable
from .Date import Date
from .Defer import Defer, defer
from .Fluent import Fluent
from .HigherOrderTapProxy import HigherOrderTapProxy
from .HtmlString import HtmlString
from .Image import Image
from .Macroable import Macroable
from .Manager import Manager
from .ModuleManager import ModuleManager
from .Once import Once, once
from .Process import Process, ProcessFailedException, ProcessResult
from .Reflector import Reflector
from .Sleep import Sleep
from .Stringable import Stringable
from .Tappable import Tappable
from .Uri import Uri

from .Currency import currency_symbol, default_currency, format_money
from .Number import clamp
from .PathManager import PathManager
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
from .paths import app as app_paths
from .paths import app_path, base_path
from .paths import config as config_paths
from .paths import config_path, p, paths, public_path, storage, storage_path

__all__ = [
    "Arr",
    "Collection",
    "Conditionable",
    "Date",
    "Defer",
    "Fluent",
    "HigherOrderTapProxy",
    "HtmlString",
    "Image",
    "Macroable",
    "Manager",
    "ModuleManager",
    "Once",
    "PathManager",
    "Pipeline",
    "Process",
    "ProcessFailedException",
    "ProcessResult",
    "Reflector",
    "Sleep",
    "Stringable",
    "SupportProvider",
    "Tappable",
    "Uri",
    "after",
    "app",
    "app_path",
    "app_paths",
    "base_path",
    "before",
    "between",
    "camel_case",
    "clamp",
    "collect",
    "config",
    "config_path",
    "config_paths",
    "contains",
    "currency_symbol",
    "default_currency",
    "defer",
    "email_mask",
    "ends_with",
    "flatten",
    "format_duration",
    "format_money",
    "format_money_cents",
    "get_classes",
    "get_functions",
    "humanize_seconds",
    "import_module",
    "kebab_case",
    "m",
    "mask",
    "module_exists",
    "modules",
    "normalize_email",
    "once",
    "p",
    "paths",
    "pluralize",
    "public_path",
    "routes",
    "sanitize_text",
    "slugify",
    "snake_case",
    "starts_with",
    "storage",
    "storage_path",
    "strip_tags",
    "studly_case",
    "title_case",
    "to_pendulum",
    "truncate",
    "ulid",
    "uuid",
]

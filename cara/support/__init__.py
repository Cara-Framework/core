"""
Support Module Initialization.

This module provides core support classes and utilities for the Cara framework. It includes the
Collection class which offers a fluent, Laravel-style interface for working with arrays and data
collections.
"""

from .Auth import optional_user_id, resolve_user
from .BestEffort import best_effort, best_effort_ctx
from .Coercion import safe_float, safe_int
from .Collection import Collection, collect, flatten
from .Console import HasColoredOutput
from .MailSafety import DEFAULT_SMTP_PORT, SMTP_TIMEOUT_SECONDS, strip_header_crlf
from .Currency import currency_symbol, default_currency, format_money
from .Date import Date
from .HtmlString import HtmlString
from .Image import Image
from .LogColors import LogColors
from .Macroable import Macroable
from .Manager import Manager
from .ModuleLoader import load
from .ModuleManager import ModuleManager
from .Number import safe_divide_decimal, to_decimal
from .PathManager import PathManager
from .Pipeline import Pipeline
from .Process import Process, ProcessFailedException, ProcessResult
from .Retry import Retry
from .Sleep import Sleep
from .Str import (
    as_filepath,
    email_mask,
    mask_ip,
    mask_proxy_url,
    mask_token,
    modularize,
    redact_log_secrets,
    sanitize_text,
    slugify,
    strip_tags,
)
from .Structures import data, data_get, data_set
from .SupportProvider import SupportProvider
from .Time import parse_human_time, to_pendulum
from .Modules import get_classes, modules
from .Paths import base_path, paths, public_path, storage_path


__all__ = [
    "Collection",
    "DEFAULT_SMTP_PORT",
    "Date",
    "HasColoredOutput",
    "HtmlString",
    "Image",
    "LogColors",
    "Macroable",
    "Manager",
    "ModuleManager",
    "PathManager",
    "Pipeline",
    "Process",
    "ProcessFailedException",
    "ProcessResult",
    "Retry",
    "SMTP_TIMEOUT_SECONDS",
    "Sleep",
    "SupportProvider",
    "as_filepath",
    "base_path",
    "best_effort",
    "best_effort_ctx",
    "collect",
    "currency_symbol",
    "data",
    "data_get",
    "data_set",
    "default_currency",
    "email_mask",
    "flatten",
    "format_money",
    "get_classes",
    "load",
    "mask_ip",
    "mask_proxy_url",
    "mask_token",
    "modularize",
    "modules",
    "optional_user_id",
    "parse_human_time",
    "paths",
    "public_path",
    "redact_log_secrets",
    "resolve_user",
    "safe_divide_decimal",
    "safe_float",
    "safe_int",
    "sanitize_text",
    "slugify",
    "storage_path",
    "strip_header_crlf",
    "strip_tags",
    "to_decimal",
    "to_pendulum",
]

"""
Support Module Initialization.

This module provides core support classes and utilities for the Cara framework. It includes the
Collection class which offers a fluent, Laravel-style interface for working with arrays and data
collections.
"""

from .Collection import Collection, collect, flatten
from .CommandPipeline import workflow
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
from .Pipeline import Pipeline
from .SupportProvider import SupportProvider

__all__ = [
    "Collection",
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

    # Short aliases
    "m", "p",
    "workflow",

    # Dynamic helpers
    "app", "routes", "config", "storage", "config_paths", "app_paths",
]

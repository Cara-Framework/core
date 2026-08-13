"""
Configuration Manager for the Cara framework.

This module provides the Configuration class, responsible for loading, merging, and managing
application configuration settings from various sources.
"""

from __future__ import annotations

import threading

from cara.exceptions import (
    InvalidConfigurationLocationException,
    InvalidConfigurationSetupException,
)
from cara.loader import Loader
from cara.support import data


class Configuration:
    _instance = None
    _lock = threading.Lock()

    # Foundation configuration keys that cannot be overwritten
    reserved_keys = [
        "app",
        "auth",
        "broadcast",
        "cache",
        "database",
        "filesystem",
        "mail",
        "notification",
        "providers",
        "queue",
        "session",
    ]

    def __init__(self, application):
        if application is None:
            raise InvalidConfigurationSetupException(
                "Configuration requires an application; tests must use Configuration.empty()"
            )
        with Configuration._lock:
            self.application = application
            self._config = data()
            Configuration._instance = self
        self._loader = application.make("loader")

    @classmethod
    def empty(cls) -> Configuration:
        """Install an explicit empty authority for isolated framework tests."""
        with cls._lock:
            instance = object.__new__(cls)
            instance.application = None
            instance._config = data()
            instance._loader = Loader()
            cls._instance = instance
            return instance

    def load(self):
        """
        At boot, load all configuration modules under the directory returned by
        application.make("config.location").

        Each file yields a mapping of settings.
        """
        config_root = self.application.make("config.location")
        for module_name, module in self._loader.get_modules(
            config_root, raise_exception=True
        ).items():
            params = self._loader.get_parameters(module)
            for name, value in params.items():
                # store under "<filename>.<lowercase_key>"
                self._config[f"{module_name}.{name.lower()}"] = value

        # Ensure at least "app" section exists
        if not self._config.get("app"):
            raise InvalidConfigurationLocationException(
                f"Config directory {config_root} does not contain required configuration files."
            )

    def merge_with(self, path, external_config):
        """
        Merge external config into existing config under `path`.

        Similar to Laravel's merge.
        """
        if path in self.reserved_keys:
            raise InvalidConfigurationSetupException(
                f"{path} is a reserved configuration key name. Please use another key."
            )
        if isinstance(external_config, str):
            params = self._loader.get_parameters(external_config)
        else:
            params = external_config

        base_config = {name.lower(): value for name, value in params.items()}
        merged_config = {
            **base_config,
            **self.get(path, {}),
        }
        self.set(path, merged_config)

    def set(self, path, value):
        self._config[path] = value

    def has(self, path):
        return path in self._config

    def all(self):
        return self._config

    def get(self, path, default=None):
        try:
            return self._config[path]
        except KeyError:
            return default

"""
Configuration Manager for the Cara framework.

This module provides the Configuration class, responsible for loading, merging, and managing
application configuration settings from various sources.
"""

from cara.exceptions import (
    InvalidConfigurationLocationException,
    InvalidConfigurationSetupException,
)
from cara.facades import Loader
from cara.support.Structures import data


class Configuration:
    _instance = None

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

    def __init__(self, application=None):
        if application:
            self.application = application
            self._config = data()
            Configuration._instance = self
        else:
            if not Configuration._instance:
                self._config = data()
                Configuration._instance = self
            else:
                self._config = Configuration._instance._config

    def load(self):
        """
        At boot, load all configuration modules under the directory returned by
        application.make("config.location").

        Each file yields a mapping of settings.
        """
        config_root = self.application.make("config.location")
        for module_name, module in Loader.get_modules(
            config_root, raise_exception=True
        ).items():
            params = Loader.get_parameters(module)
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
            params = Loader.get_parameters(external_config)
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
            config_at_path = self._config[path]
            if isinstance(config_at_path, dict):
                return data(config_at_path)
            return config_at_path
        except KeyError:
            return default

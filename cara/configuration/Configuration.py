"""
Configuration Manager for the Cara framework.

This module provides the Configuration class, responsible for loading, merging, and managing
application configuration settings from various sources.
"""

from __future__ import annotations

import threading

from dotty_dict import Dotty

from cara.exceptions import (
    InvalidConfigurationLocationException,
    InvalidConfigurationSetupException,
)
from cara.loader import Loader
from cara.support import data

#: Dotty's own dot-notation walk, without the ``lru_cache`` in front of it.
_READ_PATH = Dotty.__getitem__.__wrapped__


class Configuration:
    _instance = None
    _lock = threading.Lock()

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

    def set(self, path, value):
        self._config[path] = value

    def has(self, path):
        return path in self._config

    def all(self):
        return self._config

    def get(self, path, default=None):
        """A dot-notation read, O(depth of ``path``).

        Never ``self._config[path]``: ``Dotty.__getitem__`` is an
        ``lru_cache`` method and ``Dotty.__hash__`` is ``hash(str(self))``, so
        that spelling renders the whole configuration tree on every read, hit
        or miss. The ORM once read ``app.timezone`` per datetime attribute, and
        a 30-day sales export spent 29 s on it (187,534 reads, 156 us each).
        """
        # The common read — string keys down plain dicts — is walked here:
        # Dotty re-parses the path (escape stamps, a fresh closure, a list
        # ``pop(0)`` per level) on every call, ~1 us for a value read ten
        # times per request. Nothing is memoised, so a section mutated in
        # place (``config("mail")["x"] = y``, a test's ``setitem`` on the
        # store) is read live exactly as before. Anything else — a list
        # index, a non-string key, an escaped dot, a miss — is left to Dotty.
        if type(path) is str and "\\" not in path:
            node = getattr(self._config, "_data", None)
            for part in path.split("."):
                if type(node) is not dict or part not in node:
                    break
                node = node[part]
                if node is None:
                    # Dotty stops at a None and answers it, even mid-path.
                    return None
            else:
                return node
        try:
            return _READ_PATH(self._config, path)
        except KeyError:
            return default

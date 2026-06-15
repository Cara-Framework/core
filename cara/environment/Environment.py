"""
Environment Configuration System for the Cara framework.

This module provides a robust environment management system that handles loading and accessing
environment variables from various sources. It supports multiple environment files, environment-
specific overrides, and type casting of environment values.

The environment system follows the 12-factor app methodology for configuration management through
environment variables.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from cara.exceptions import ConfigurationException


class LoadEnvironment:
    """This class is used for loading the environment from .env and .env.* files."""

    # Track loaded environment files
    loaded_files = []

    def __init__(self, environment=None, override=True, only=None):
        """
        LoadEnvironment constructor.

        Keyword Arguments:
            env {string} -- An additional environment file that you want to load. (default: {None})
            override {bool} -- Whether or not the environment variables found should overwrite existing ones. (default: {False})
            only {string} -- If this is set then it will only load that environment. (default: {None})
        """
        from dotenv import load_dotenv

        self.env = load_dotenv

        if only:
            self._load_environment(only, override=override)
            return

        env_path = str(Path(".") / ".env")
        if Path(env_path).exists():
            self.env(env_path, override=override)
            LoadEnvironment.loaded_files.append(env_path)

        if os.environ.get("APP_ENV"):
            self._load_environment(os.environ.get("APP_ENV"), override=override)
        if environment:
            self._load_environment(environment, override=override)

        if "PYTEST_CURRENT_TEST" in os.environ:
            self._load_environment("testing", override=override)

    def _load_environment(self, environment, override=False):
        """
        Load the environment depending on the env file.

        Arguments:
            environment {string} -- Name of the environment file to load from

        Keyword Arguments:
            override {bool} -- Whether the environment file should overwrite existing environment keys. (default: {False})
        """
        env_path = str(Path(".") / f".env.{environment}")
        if Path(env_path).exists():
            self.env(dotenv_path=env_path, override=override)
            LoadEnvironment.loaded_files.append(env_path)


_BOOL_TRUTHY = ("true", "yes", "on", "1")
_BOOL_FALSY = ("false", "no", "off", "0")


def _cast_bool(name, raw, default):
    """Strict bool cast for env strings. Raises ValueError on unknown values."""
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return default if isinstance(default, bool) else None
    if isinstance(raw, str):
        stripped = raw.strip()
        if stripped == "":
            return default if isinstance(default, bool) else None
        lower = stripped.lower()
        if lower in _BOOL_TRUTHY:
            return True
        if lower in _BOOL_FALSY:
            return False
        raise ConfigurationException(
            f"Environment variable {name}={raw!r} cannot be cast to bool. "
            f"Allowed values: true/false, yes/no, on/off, 1/0."
        )
    # Non-str, non-bool (e.g. caller passed an int default). Fall back to
    # Python truthiness — explicit types passed by callers always pre-empt
    # the surprise of returning ``int`` 0/1 where a real bool was wanted.
    return bool(raw)


def _cast_typed(name, raw, target_type, default):
    """Apply an explicit type cast to a raw env value.

    Backs the type-annotation form ``env("X", default, float)``.
    Raises a ``ValueError`` that names the offending variable so a
    misconfigured deploy fails fast at startup with an actionable
    message instead of bubbling a cryptic ``invalid literal for int()``
    error from some downstream call site.
    """
    if target_type is bool:
        return _cast_bool(name, raw, default)

    if raw is None:
        return default if isinstance(default, target_type) else None

    if isinstance(raw, target_type) and not isinstance(raw, bool):
        return raw

    if isinstance(raw, str):
        stripped = raw.strip()
        if stripped == "":
            # Empty string → fall back to the (already-typed) default.
            return default if isinstance(default, target_type) else target_type()
        try:
            return target_type(stripped)
        except (TypeError, ValueError) as exc:
            raise ConfigurationException(
                f"Environment variable {name}={raw!r} cannot be cast to "
                f"{target_type.__name__}: {exc}"
            ) from exc

    # Numeric default passed through (e.g. ``env("X", 5.0, float)`` when
    # the env var is unset and the helper already substituted the default).
    try:
        return target_type(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigurationException(
            f"Environment variable {name}={raw!r} cannot be cast to "
            f"{target_type.__name__}: {exc}"
        ) from exc


def env(value, default="", cast=True):
    env_var = os.getenv(value, None)
    if env_var is None:
        env_var = default

    # Explicit type-cast form: ``env("X", default, int|float|bool)``.
    # The third arg historically documents as a boolean flag but every
    # config site that needs a typed value passes a type object. Honour
    # that intent here so e.g. ``REDIS_SOCKET_TIMEOUT=5.5`` returns a
    # ``float`` instead of the raw string ``"5.5"`` (which silently
    # broke redis-py's socket-timeout argument validation).
    if isinstance(cast, type):
        return _cast_typed(value, env_var, cast, default)

    if not cast:
        return env_var

    # If not a str, return as-is (int/bool/etc)
    if not isinstance(env_var, str):
        return env_var

    # Now env_var is str
    stripped = env_var.strip()
    if stripped == "":
        return default

    # Auto-coerce integer-looking values. Use an explicit ``[+-]?\d+`` match
    # rather than ``str.isnumeric()``: the latter is True for non-parseable
    # characters like "½"/"²" (so ``X=²`` crashed startup with ValueError)
    # and False for "-5" (so a negative int env var leaked through as a raw
    # string and broke numeric comparisons downstream).
    if re.fullmatch(r"[+-]?\d+", stripped):
        try:
            return int(stripped)
        except ValueError:
            pass
    # Robust boolean coercion. The previous version only matched
    # ``"true"`` / ``"True"`` (and lowercase ``"false"``) literally —
    # everything else (``"TRUE"``, ``"yes"``, ``"on"``, padded ``" true "``)
    # fell through and returned the raw string, which silently evaluates
    # as truthy in ``if env(...):`` checks. The asymmetry meant
    # ``X=true`` and ``X=TRUE`` produced different downstream behaviour
    # depending on whether the consumer normalised the value. Match
    # the conventions docker-compose / k8s / .env loaders use.
    lower = stripped.lower()
    if lower in ("true", "yes", "on"):
        return True
    if lower in ("false", "no", "off"):
        return False
    return env_var

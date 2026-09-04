"""
Command Decorator for the Cara framework.

This module provides a decorator for registering command-line commands in the application.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any

# Registry of decorated command classes
_command_registry: list[type[Any]] = []


def command(
    name: str,
    help: str = "",
    options: list[dict[str, Any]] | None = None,
) -> Callable[[type[Any]], type[Any]]:
    """
    Decorator to mark a class as a CLI command.

    name: command name
    help: description text
    options: explicit option metadata. Value options declare a real Python
             ``type``; switches declare ``is_flag=True``. Defaults are
             carried in ``default`` rather than encoded in the option name.
    """

    def decorator(cls: type[Any]) -> type[Any]:
        cls.name = name
        cls.help = help
        cls._cli_options = list(options or [])
        _command_registry.append(cls)
        _wrap_init(cls)
        _wrap_handle(cls)
        return cls

    return decorator


def _wrap_init(cls: type[Any]) -> None:
    orig = cls.__init__

    @wraps(orig)
    def wrapped(self, application, *args, **kwargs):
        if hasattr(application, "call"):
            application.call(orig, self, application, *args, **kwargs)
        else:
            orig(self, application, *args, **kwargs)

    cls.__init__ = wrapped


def _wrap_handle(cls: type[Any]) -> None:
    if not hasattr(cls, "handle"):
        return
    orig = cls.handle

    @wraps(orig)
    def wrapped(self, *args, **kwargs):
        if hasattr(self, "application") and hasattr(self.application, "call"):
            return self.application.call(orig, self, *args, **kwargs)
        return orig(self, *args, **kwargs)

    cls.handle = wrapped


def get_registered_commands() -> list[type[Any]]:
    return _command_registry

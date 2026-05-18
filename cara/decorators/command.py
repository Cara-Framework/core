"""
Command Decorator for the Cara framework.

This module provides a decorator for registering command-line commands in the application.
"""

from collections.abc import Callable
from functools import wraps
from typing import Any

# Registry of decorated command classes
_command_registry: list[type[Any]] = []

# Hook lists
_before_hooks: list[Callable[[str], None]] = []
_after_hooks: list[Callable[[str], None]] = []
_on_error_hooks: list[Callable[[str, Exception], None]] = []


def command(
    name: str, help: str = "", options: dict[str, str] | None = None
) -> Callable[[type[Any]], type[Any]]:
    """
    Decorator to mark a class as a CLI command.

    name: command name
    help: description text
    options: mapping of option definitions to their help text.
             Key examples:
               "--m|migration=all": "Migration name to run"
               "--f|force": "Force migrations without prompt"
               "--schema=?": "Schema to migrate, optional value"
             Value: help description string.
    """

    def decorator(cls: type[Any]) -> type[Any]:
        cls.name = name
        cls.help = help
        cls._cli_options = options or {}
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


def before_command(fn: Callable[[str], None]) -> Callable[[str], None]:
    _before_hooks.append(fn)
    return fn


def after_command(fn: Callable[[str], None]) -> Callable[[str], None]:
    _after_hooks.append(fn)
    return fn


def on_error(fn: Callable[[str, Exception], None]) -> Callable[[str, Exception], None]:
    _on_error_hooks.append(fn)
    return fn


def _run_before(name: str) -> None:
    for fn in _before_hooks:
        fn(name)


def _run_after(name: str) -> None:
    for fn in _after_hooks:
        fn(name)


def _run_on_error(name: str, err: Exception) -> None:
    for fn in _on_error_hooks:
        fn(name, err)

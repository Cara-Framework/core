"""
Command Decorator for the Cara framework.

This module provides a decorator for registering command-line commands in the application.
"""

import inspect
from functools import wraps
from typing import Any, Callable, List, Type

# Registry of decorated command classes
_command_registry: List[Type[Any]] = []

# Hook lists
_before_hooks: List[Callable[[str], None]] = []
_after_hooks: List[Callable[[str], None]] = []
_on_error_hooks: List[Callable[[str, Exception], None]] = []


def command(
    name: str, help: str = "", options: dict[str, str] = None
) -> Callable[[Type[Any]], Type[Any]]:
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

    def decorator(cls: Type[Any]) -> Type[Any]:
        setattr(cls, "name", name)
        setattr(cls, "help", help)
        setattr(cls, "_cli_options", options or {})
        _command_registry.append(cls)
        _wrap_init(cls)
        _wrap_handle(cls)
        return cls

    return decorator


def _wrap_init(cls: Type[Any]) -> None:
    orig = cls.__init__
    sig = inspect.signature(orig)

    @wraps(orig)
    def wrapped(self, application, *args, **kwargs):
        # Resolve dependencies from application.make for __init__
        kwargs_di = {}
        for pname, param in list(sig.parameters.items())[1:]:
            if pname == "application":
                continue
            dep = _resolve_dep(application, pname, param.annotation, param.default)
            kwargs_di[pname] = dep
        orig(self, application, **kwargs_di)

    cls.__init__ = wrapped


def _wrap_handle(cls: Type[Any]) -> None:
    if not hasattr(cls, "handle"):
        return
    orig = cls.handle
    sig = inspect.signature(orig)

    @wraps(orig)
    def wrapped(self, *args, **kwargs):
        # Resolve DI parameters for handle
        bound = {}
        for pname, param in sig.parameters.items():
            if pname == "self":
                continue
            if pname in kwargs:
                bound[pname] = kwargs[pname]
                continue
            dep = _resolve_dep(self.application, pname, param.annotation, param.default)
            bound[pname] = dep
        return orig(self, **bound)

    cls.handle = wrapped


def _resolve_dep(app, name: str, annotation: Any, default: Any) -> Any:
    # Try resolving by annotation via application.make
    if annotation is not inspect._empty:
        try:
            return app.make(annotation)
        except Exception:
            pass
    # Try resolving by name
    try:
        return app.make(name)
    except Exception:
        pass
    # Use default if available
    if default is not inspect._empty:
        return default
    raise RuntimeError(f"Cannot resolve dependency '{name}'")


def get_registered_commands() -> List[Type[Any]]:
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

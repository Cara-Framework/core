"""Reflector — class / callable introspection helper.

Laravel's ``Illuminate\\Support\\Reflector`` parity. Provides a
small set of cached lookups used internally by the container,
event dispatcher, and pipeline when they need to know what a
callable expects::

    Reflector.is_callable(target)
    Reflector.parameter_class_name(callback, "user")  # → "User" or None
    Reflector.is_parameter_subclass_of(callback, "model", BaseModel)

Centralising these helpers keeps reflection logic out of the
hot-path call sites (queue worker, container resolution) and
makes them straightforward to mock in tests.
"""

from __future__ import annotations

import inspect
from typing import Any, Callable, Optional, Type


class Reflector:
    """Static reflection helpers for callables and classes."""

    @staticmethod
    def is_callable(target: Any) -> bool:
        """True if ``target`` is callable (incl. classes, methods, lambdas)."""
        return callable(target)

    @staticmethod
    def signature(target: Callable) -> Optional[inspect.Signature]:
        """Return the signature, or ``None`` if not introspectable.

        Builtins and C-extension callables sometimes don't expose
        signatures — return ``None`` instead of raising so callers
        can fall back to ``*args, **kwargs`` dispatch.
        """
        try:
            return inspect.signature(target)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def parameters(target: Callable) -> list:
        """Return the list of :class:`inspect.Parameter`s, or ``[]``."""
        sig = Reflector.signature(target)
        return list(sig.parameters.values()) if sig else []

    @staticmethod
    def parameter_class(target: Callable, name: str) -> Optional[type]:
        """Return the annotated class for parameter ``name``, or ``None``.

        Only resolves when the annotation is an actual class object —
        forward-reference strings and unions return ``None`` to keep
        the caller's branching logic simple.
        """
        sig = Reflector.signature(target)
        if sig is None or name not in sig.parameters:
            return None
        annotation = sig.parameters[name].annotation
        if annotation is inspect.Parameter.empty:
            return None
        return annotation if isinstance(annotation, type) else None

    @staticmethod
    def parameter_class_name(target: Callable, name: str) -> Optional[str]:
        """Return ``parameter_class(...).__name__`` or the raw annotation if string."""
        sig = Reflector.signature(target)
        if sig is None or name not in sig.parameters:
            return None
        annotation = sig.parameters[name].annotation
        if annotation is inspect.Parameter.empty:
            return None
        if isinstance(annotation, type):
            return annotation.__name__
        if isinstance(annotation, str):
            return annotation
        return None

    @staticmethod
    def is_parameter_subclass_of(
        target: Callable, name: str, base: Type
    ) -> bool:
        """True if parameter ``name`` is annotated as a subclass of ``base``."""
        cls = Reflector.parameter_class(target, name)
        if cls is None:
            return False
        try:
            return issubclass(cls, base)
        except TypeError:
            return False

    @staticmethod
    def get_class_method_owner(cls: Type, method_name: str) -> Optional[Type]:
        """Return the class in the MRO that defined ``method_name``."""
        for klass in cls.__mro__:
            if method_name in klass.__dict__:
                return klass
        return None


__all__ = ["Reflector"]

"""Metaclass powering the deliberate static query surface of models."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ..query import QueryBuilder


def _is_delegated(cls: type, attribute: str) -> bool:
    """Whether ``cls`` advertises ``attribute`` for delegation."""
    for source in ("__passthrough__", "_class_scopes"):
        try:
            names = type.__getattribute__(cls, source)
        except AttributeError:
            continue
        if names and attribute in names:
            return True
    return False


class ModelMeta(type):
    """Register scopes and delegate only the advertised query API."""

    def __new__(
        mcs: type[ModelMeta],
        name: str,
        bases: tuple,
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> ModelMeta:
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        cls._class_scopes: dict[str, Callable] = {}

        for attr_name in dir(cls):
            if not attr_name.startswith("scope_") or not callable(
                getattr(cls, attr_name, None)
            ):
                continue
            scope_method = getattr(cls, attr_name)
            scope_name = attr_name[6:]

            def create_scope_method(
                scope_func: Callable, registered_name: str
            ) -> classmethod:
                def scope_wrapper(
                    cls_inner: type, *args: Any, **scope_kwargs: Any
                ) -> QueryBuilder:
                    instance = cls_inner()
                    builder = instance.get_builder()
                    return scope_func(instance, builder, *args, **scope_kwargs)

                scope_wrapper.__name__ = registered_name
                scope_wrapper.__doc__ = f"Query scope: {registered_name}"
                return classmethod(scope_wrapper)

            setattr(
                cls,
                scope_name,
                create_scope_method(scope_method, scope_name),
            )
            cls._class_scopes[scope_name] = scope_method

        if cls._class_scopes and hasattr(cls, "_scopes"):
            cls._scopes[cls] = cls._class_scopes
        return cls

    def __getattribute__(cls: type, attribute: str) -> Any:
        try:
            return super().__getattribute__(attribute)
        except AttributeError:
            pass

        if not attribute.startswith("_") and _is_delegated(cls, attribute):
            try:
                return getattr(cls(), attribute)
            except AttributeError:
                pass
        raise AttributeError(
            f"'{cls.__name__}' object has no attribute '{attribute}'"
        ) from None

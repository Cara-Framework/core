"""
Core IoC (Inversion of Control) Container for the Cara framework.

This module provides a powerful dependency injection container that manages object creation and
dependency resolution throughout the application. It supports automatic dependency injection,
singleton bindings, and contextual binding.

The container follows the PSR-11 container interface standard and provides Laravel-style service
container functionality.
"""

from __future__ import annotations

import inspect
import threading
import types
import typing as _typing
from collections.abc import Callable
from contextvars import ContextVar
from typing import Any, Union

from . import _ContainerResolution

# Lazy import exceptions to avoid circular imports


_resolving_stack_var: ContextVar[list[Any]] = ContextVar("cara.container.resolving_stack")


class Container:
    """
    Core of the Service Container.

    - Manages bindings: key (string or class) → class / factory (callable) / instance
    - Supports deferred providers: stored in _deferred, registered when first requested
    - Performs automatic dependency injection by inspecting constructor signatures
    """

    def __init__(self) -> None:
        # (1) Direct bindings: name → value (could be a class, callable, or instance)
        self.objects: dict[Any, Any] = {}

        # (2) If strict=True, existing bindings cannot be overridden
        self.strict: bool = False

        # (3) If override=False, do not override existing bindings
        self.override: bool = True

        # (4) If remember=True, cache constructor arguments for repeated resolutions
        self.remember: bool = False

        # (5) Lock guarding deferred-provider resolution. Without this,
        # two concurrent ``make("cache")`` calls race: Thread A pops the
        # entry out of ``_deferred`` and enters ``register()``; Thread B
        # arrives in the narrow window after A's pop and before A's
        # ``bind("cache", ...)`` completes, sees the key absent from both
        # ``_deferred`` (A popped it) and ``objects`` (A hasn't bound
        # yet), and raises ``MissingContainerBindingException``. Under
        # the queue worker's threaded consumer this shows up as sporadic
        # "'cache' key was not found" errors that vanish on retry. A
        # lock around the pop+register+bind block is the minimum-footprint
        # fix; the path runs once per service, so contention is nil.
        self._deferred_lock = threading.RLock()

        # (6) Hooks: callback lists for bind / make / resolve events
        self._hooks: dict[str, dict[Any, list[Callable]]] = {
            "bind": {},
            "make": {},
            "resolve": {},
        }

        # (6) Temporary swap bindings for testing or mocking
        self.swaps: dict[Any, Any] = {}

        # (7) Cached constructor arguments when remember=True
        self._remembered: dict[Any, list[Any]] = {}

        # (8) Deferred providers: key (string or class) → provider class
        self._deferred: dict[Any, Any] = {}

        # (9) List of instantiated provider objects (for optional tracking)
        self.providers: list[Any] = []

    # -------------------------------------
    # Public Binding and Resolving Methods
    # -------------------------------------

    def bind(self, name: Any, class_obj: Any) -> Container:
        """
        Bind a key (string or class) to a class, factory (callable), or instance.

        Raises an exception if strict=True and the key already exists.
        """
        if inspect.ismodule(class_obj):
            (
                GenericContainerException,
                MissingContainerBindingException,
                StrictContainerException,
            ) = _ContainerResolution._get_container_exceptions()
            raise StrictContainerException(
                f"Cannot bind module '{class_obj}' with key '{name}'."
            )
        if self.strict and name in self.objects:
            (
                GenericContainerException,
                MissingContainerBindingException,
                StrictContainerException,
            ) = _ContainerResolution._get_container_exceptions()
            raise StrictContainerException(
                f"Cannot override '{name}' in strict container."
            )

        if self.override or name not in self.objects:
            self.fire_hook("bind", name, class_obj)
            self.objects[name] = class_obj

        return self

    def singleton(self, name: Any, class_obj: Any) -> None:
        """
        Register a singleton binding (Laravel-style).

        If class_obj is a factory (callable), it's wrapped so the result is cached.
        First make() calls the factory and caches. Subsequent make() return cached instance.
        """
        if inspect.isfunction(class_obj) or inspect.ismethod(class_obj):
            # Lazy singleton: wrap factory to cache result. The lock
            # closes a check-then-act race — without it, two threads
            # arriving at a fresh ``make(name)`` both observe
            # ``cached["instance"] is None``, both invoke ``class_obj()``,
            # and the loser's instance is dropped on the floor while
            # the caller already holds a reference to it. Costly when
            # the factory opens a DB pool, mounts a Playwright browser,
            # etc. — that orphan resource leaks for the process lifetime.
            cached = {"instance": None}
            init_lock = threading.Lock()

            def singleton_factory():
                if cached["instance"] is None:
                    with init_lock:
                        if cached["instance"] is None:
                            cached["instance"] = class_obj()
                return cached["instance"]

            self.bind(name, singleton_factory)
        else:
            # Immediate singleton: resolve now and bind instance
            instance = self.resolve(class_obj)
            self.bind(name, instance)

    def unbind(self, name: Any) -> bool:
        """
        Unbind a previously bound name.

        Returns False if the key did not exist.
        """
        if name not in self.objects:
            return False
        del self.objects[name]
        return True

    def simple(self, obj: Any) -> Container:
        """Bind an object or class under its own class as the key."""
        key = obj if inspect.isclass(obj) else obj.__class__
        self.bind(key, obj)
        return self

    def has(self, name: Any) -> bool:
        """Check if a given key (string) or class exists in the container (either as a direct
        binding or as a deferred provider)."""
        if isinstance(name, str):
            return name in self.objects or name in self._deferred
        # Lazy-import the exception class — the same circular-import
        # workaround the rest of this file uses. The bare reference
        # ``MissingContainerBindingException`` would NameError at
        # runtime because the symbol is only defined inside
        # ``_get_container_exceptions``.
        _, MissingContainerBindingException, _ = (
            _ContainerResolution._get_container_exceptions()
        )
        try:
            self._find_obj(name)
            return True
        except MissingContainerBindingException:
            return name in self._deferred

    def resolve(self, obj: Any, *resolving_arguments: Any) -> Any:
        """
        Instantiate a class or call a function, performing dependency injection based on constructor
        parameter annotations.

        - Primitive types (str, int, etc.) are taken from *resolving_arguments or defaults.
        - Class-annotated parameters are resolved from the container or triggered as deferred.
        - "self" parameters receive the class itself.
        """
        # Circular dependency guard
        resolving_stack = _resolving_stack_var.get([])
        if obj in resolving_stack:
            chain = " -> ".join(getattr(c, "__name__", str(c)) for c in resolving_stack)
            name = getattr(obj, "__name__", str(obj))
            (
                GenericContainerException,
                _,
                _,
            ) = _ContainerResolution._get_container_exceptions()
            raise GenericContainerException(
                f"Circular dependency detected: {chain} -> {name}"
            )
        _resolving_stack_var.set([*resolving_stack, obj])

        try:
            return self._do_resolve(obj, *resolving_arguments)
        finally:
            stack = _resolving_stack_var.get([])
            if stack:
                _resolving_stack_var.set(stack[:-1])
            else:
                _resolving_stack_var.set([])

    # ---------------------------------------
    def _accepts_container(self, func):
        """Check if callable accepts a container parameter."""
        try:
            sig = inspect.signature(func)
            params = list(sig.parameters.keys())
            return len(params) > 0 and params[0] in ("app", "container", "self")
        except TypeError, ValueError:
            return False

    def fire_hook(self, action: str, key: Any, obj: Any) -> None:
        """Fire hooks for bind/make/resolve actions."""
        # If bound object is a class, invoke class-based hooks
        if inspect.isclass(obj) and obj in self._hooks[action]:
            for fn in self._hooks[action][obj]:
                fn(obj, self)

        # If bound object is an instance, check hooks on its class
        if hasattr(obj, "__class__") and obj.__class__ in self._hooks[action]:
            for fn in self._hooks[action][obj.__class__]:
                fn(obj, self)

    def on_bind(self, key: Any, fn: Callable) -> Container:
        return self._bind_hook("bind", key, fn)

    def on_make(self, key: Any, fn: Callable) -> Container:
        return self._bind_hook("make", key, fn)

    def on_resolve(self, key: Any, fn: Callable) -> Container:
        return self._bind_hook("resolve", key, fn)

    def _bind_hook(self, hook: str, key: Any, fn: Callable) -> Container:
        """Add a callback to the specified hook (bind/make/resolve) for the given key."""
        self._hooks[hook].setdefault(key, []).append(fn)
        return self

    # ----------------------------
    # Internal Binding Lookup Method
    # ----------------------------

    def _find_obj(self, obj: Any) -> Any:
        """
        Locate a bound object with multi-strategy resolution:

        Strategy 1: Direct key lookup in self.objects
        Strategy 2: Try full module path (e.g., "app.contracts.CategoryContract.CategoryContract")
        Strategy 3: Try simple class name (e.g., "CategoryContract")
        Strategy 4: Match by type/instance/subclass
        """
        # Strategy 1: Direct lookup
        if obj in self.objects:
            provider_obj = self.objects[obj]
            self.fire_hook("resolve", obj, provider_obj)
            return provider_obj

        # Strategy 2: Try full module path
        if (
            inspect.isclass(obj)
            and hasattr(obj, "__module__")
            and hasattr(obj, "__name__")
        ):
            full_path = f"{obj.__module__}.{obj.__name__}"
            if full_path in self.objects:
                provider_obj = self.objects[full_path]
                self.fire_hook("resolve", obj, provider_obj)
                return provider_obj

        # Strategy 3: Try simple class name
        if (
            inspect.isclass(obj)
            and hasattr(obj, "__name__")
            and obj.__name__ in self.objects
        ):
            provider_obj = self.objects[obj.__name__]
            self.fire_hook("resolve", obj, provider_obj)
            return provider_obj

        # Strategy 4: Match by type/instance/subclass (original logic)
        for provider_obj in self.objects.values():
            # (1) Class–instance match
            try:
                if inspect.isclass(obj) and isinstance(provider_obj, obj):
                    self.fire_hook("resolve", obj, provider_obj)
                    return provider_obj
            except TypeError:
                pass

            # (2) Exact match: key bound as instance or class
            try:
                if obj in (
                    provider_obj,
                    provider_obj.__class__,
                ):
                    self.fire_hook("resolve", obj, provider_obj)
                    return provider_obj
            except TypeError:
                pass

            # (3) Subclass match
            try:
                if (
                    inspect.isclass(provider_obj)
                    and not inspect.isabstract(provider_obj)
                    and issubclass(provider_obj, obj)
                ) or (
                    issubclass(provider_obj.__class__, obj)
                    and not inspect.isabstract(provider_obj.__class__)
                ):
                    self.fire_hook("resolve", obj, provider_obj)
                    return provider_obj
            except TypeError:
                pass

        (
            GenericContainerException,
            MissingContainerBindingException,
            StrictContainerException,
        ) = _ContainerResolution._get_container_exceptions()
        raise MissingContainerBindingException(
            f"The dependency with the '{obj}' annotation could not be resolved by the container"
        )

    def _find_concrete_binding(self, abstract_obj: Any) -> Any:
        """Find a concrete subclass for an abstract binding.

        When a binding or direct lookup resolves to an abstract class, this
        fallback searches all bound values for a concrete class that subclasses
        the requested abstraction.
        """
        if not inspect.isclass(abstract_obj):
            return None

        for provider_obj in self.objects.values():
            if inspect.isclass(provider_obj):
                try:
                    if issubclass(provider_obj, abstract_obj) and not inspect.isabstract(
                        provider_obj
                    ):
                        return provider_obj
                except TypeError:
                    pass

            try:
                if isinstance(provider_obj, abstract_obj) and not inspect.isabstract(
                    provider_obj.__class__
                ):
                    return provider_obj.__class__
            except TypeError, AttributeError:
                pass

        return None

    def get_parameters(self, obj: Any) -> Any:
        """Return inspect.signature(obj).parameters.items() for parameter inspection."""
        return inspect.signature(obj).parameters.items()

    @staticmethod
    def _unwrap_annotation(ann: Any) -> Any:
        """Extract the concrete class from union types like ``X | None`` or ``Optional[X]``."""

        origin = getattr(ann, "__origin__", None)
        _union_type = getattr(types, "UnionType", None)
        is_union = origin is Union or (
            _union_type is not None and isinstance(ann, _union_type)
        )
        if is_union:
            type_args = [a for a in ann.__args__ if a is not type(None)]
            if len(type_args) == 1:
                return type_args[0]
            return None
        return ann

    def call(self, callable_or_method: Any, *args: Any, **kwargs: Any) -> Any:
        """Invoke any callable, auto-resolving class-annotated params from the container.

        Works with sync/async functions, bound methods, and closures.
        Primitive-typed and un-annotated params are left to the caller.
        Returns whatever the callable returns (including a coroutine for
        async functions — the caller is responsible for awaiting it).

        Positional ``args`` are matched to the leading parameters by
        position; those parameters are skipped during IoC resolution so
        we don't try to instantiate the same type twice (or fail because
        the annotation is a domain class with required init args).
        """
        sig = inspect.signature(callable_or_method)
        resolved: dict[str, Any] = {}

        # Names occupied by positional args — those must NOT be resolved.
        positional_names: set[str] = set()
        if args:
            param_iter = list(sig.parameters.items())
            pos_idx = 0
            for name, param in param_iter:
                if name in ("self", "cls"):
                    continue
                if param.kind in (
                    inspect.Parameter.VAR_POSITIONAL,
                    inspect.Parameter.VAR_KEYWORD,
                ):
                    break
                positional_names.add(name)
                pos_idx += 1
                if pos_idx >= len(args):
                    break

        for name, param in sig.parameters.items():
            if name in ("self", "cls") or name in kwargs or name in positional_names:
                continue
            if param.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                continue

            ann = param.annotation
            if ann is inspect.Parameter.empty or ann is Any:
                continue

            if isinstance(ann, str):
                # Resolve PEP 563 / `from __future__ import annotations`
                # string forms by evaluating them in the callable's
                # namespace. For closures/nested functions, __globals__
                # captures the enclosing scope's locals at definition
                # time, which is more accurate than module-level globals.
                func_globals = getattr(callable_or_method, "__globals__", None)
                if func_globals is None:
                    module = inspect.getmodule(callable_or_method)
                    func_globals = module.__dict__ if module is not None else {}
                try:
                    eval_globals = {"typing": _typing, **vars(_typing), **func_globals}
                    ann = eval(ann, eval_globals, {})
                except Exception:
                    # Fallback: look up by class name in container bindings.
                    # This handles closures whose annotations reference
                    # locally-imported contracts not in __globals__.
                    raw_name = ann.split("|", 1)[0].strip()
                    found = func_globals.get(raw_name)
                    if found is None:
                        # The container's binding map is ``self.objects``;
                        # the previous ``self._bindings`` attribute does
                        # not exist on this class and would raise
                        # AttributeError the moment an annotation failed
                        # to eval (e.g. a closure with a string-form type
                        # under ``from __future__ import annotations``).
                        for bound_key in self.objects:
                            if (
                                inspect.isclass(bound_key)
                                and bound_key.__name__ == raw_name
                            ):
                                found = bound_key
                                break
                    ann = found if found is not None else ann

            ann = self._unwrap_annotation(ann)
            if ann is None:
                continue
            if ann in (str, int, float, bool, dict, list, tuple, type(None)):
                continue
            if not inspect.isclass(ann):
                continue

            try:
                resolved[name] = self.make(ann)
            except Exception as e:
                if param.default is not inspect.Parameter.empty:
                    continue
                (
                    GenericContainerException,
                    MissingContainerBindingException,
                    StrictContainerException,
                ) = _ContainerResolution._get_container_exceptions()
                raise GenericContainerException(
                    f"Could not resolve required parameter {name!r} "
                    f"(annotation {ann!r}) for {callable_or_method!r}: {e}"
                ) from e

        merged = {**resolved, **kwargs}
        return callable_or_method(*args, **merged)

    # ----------------------------
    # Wildcard Binding Search (collect)
    # ----------------------------

    def collect(self, search: Any) -> dict[Any, Any]:
        """
        Collect bindings by wildcard (e.g., '*Service') or by class type.

        - If search is a string containing '*', return any key that matches prefix/suffix.
        - If search is a class, return any bound value that is instance or subclass.
        """
        results: dict[Any, Any] = {}
        if isinstance(search, str):
            if "*" not in search:
                raise AttributeError(
                    "Search string must contain '*' for wildcard matching."
                )
            prefix, suffix = search.split("*", 1)
            for key, val in self.objects.items():
                if (
                    isinstance(key, str)
                    and key.startswith(prefix)
                    and key.endswith(suffix)
                ):
                    results[key] = val
            return results

        # If search is a class, find all bound values that match or subclass
        for key, val in self.objects.items():
            if (inspect.isclass(val) and issubclass(val, search)) or isinstance(
                val, search
            ):
                results[key] = val
        return results

    # ----------------------------
    # Testing / Mocking Support (swap)
    # ----------------------------

    def swap(self, obj: Any, callback: Any) -> Container:
        """Temporarily override a binding for testing or mocking."""
        self.swaps[obj] = callback
        return self

    def __contains__(self, obj: Any) -> bool:
        return self.has(obj)

    make = _ContainerResolution._container_make
    _do_resolve = _ContainerResolution._container_do_resolve

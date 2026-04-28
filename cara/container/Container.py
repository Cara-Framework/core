"""
Core IoC (Inversion of Control) Container for the Cara framework.

This module provides a powerful dependency injection container that manages object creation and
dependency resolution throughout the application. It supports automatic dependency injection,
singleton bindings, and contextual binding.

The container follows the PSR-11 container interface standard and provides Laravel-style service
container functionality.
"""

import inspect
import threading
from typing import Any, Callable, Dict, List, Union

# Lazy import exceptions to avoid circular imports


def _get_container_exceptions():
    """Lazy import container exceptions to avoid circular imports."""
    from cara.exceptions import (
        GenericContainerException,
        MissingContainerBindingException,
        StrictContainerException,
    )

    return (
        GenericContainerException,
        MissingContainerBindingException,
        StrictContainerException,
    )


class Container:
    """
    Core of the Service Container.

    - Manages bindings: key (string or class) → class / factory (callable) / instance
    - Supports deferred providers: stored in _deferred, registered when first requested
    - Performs automatic dependency injection by inspecting constructor signatures
    """

    def __init__(self) -> None:
        # (1) Direct bindings: name → value (could be a class, callable, or instance)
        self.objects: Dict[Any, Any] = {}

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

        # (6) Resolution stack for circular dependency detection
        self._resolving_stack: List[Any] = []

        # (5) Hooks: callback lists for bind / make / resolve events
        self._hooks: Dict[str, Dict[Any, List[Callable]]] = {
            "bind": {},
            "make": {},
            "resolve": {},
        }

        # (6) Temporary swap bindings for testing or mocking
        self.swaps: Dict[Any, Any] = {}

        # (7) Cached constructor arguments when remember=True
        self._remembered: Dict[Any, List[Any]] = {}

        # (8) Deferred providers: key (string or class) → provider class
        self._deferred: Dict[Any, Any] = {}

        # (9) List of instantiated provider objects (for optional tracking)
        self.providers: List[Any] = []

    # -------------------------------------
    # Public Binding and Resolving Methods
    # -------------------------------------

    def bind(self, name: Any, class_obj: Any) -> "Container":
        """
        Bind a key (string or class) to a class, factory (callable), or instance.

        Raises an exception if strict=True and the key already exists.
        """
        if inspect.ismodule(class_obj):
            (
                GenericContainerException,
                MissingContainerBindingException,
                StrictContainerException,
            ) = _get_container_exceptions()
            raise StrictContainerException(
                f"Cannot bind module '{class_obj}' with key '{name}'."
            )
        if self.strict and name in self.objects:
            (
                GenericContainerException,
                MissingContainerBindingException,
                StrictContainerException,
            ) = _get_container_exceptions()
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

    def simple(self, obj: Any) -> "Container":
        """Bind an object or class under its own class as the key."""
        key = obj if inspect.isclass(obj) else obj.__class__
        self.bind(key, obj)
        return self

    def make(self, name: Any, *arguments: Any) -> Any:
        """
        Resolve an object from the container.

        1) If `name` is a class:
           - First check deferred providers by lowercased class name; if found, register and boot it.
           - Next, try to find an already-bound instance via _find_obj(name). If found, return it.
           - Otherwise, call resolve(name, *arguments) to perform constructor injection.

        2) If `name` is a string:
           - If a deferred provider exists under that key, register and boot it.
           - If an object is bound under that string:
               a) If it is a class, call resolve() to construct it.
               b) If it is a function or method (factory), call it and return the result.
               c) Otherwise assume it's already an instance and return it.

        3) If a swap exists for `name`, return the swapped object (used for testing).

        4) Otherwise, raise MissingContainerBindingException.

        ROOT CAUSE (2026-04-24): the lookup used to be split — the
        deferred check + register was inside the lock, but the final
        ``self.objects[name]`` lookup and DI fallthrough ran OUTSIDE
        it. Under concurrent workers, Thread A could pop the key and
        be mid-``register()`` (still computing the config, building
        drivers) while Thread B's lock-free outer check saw the key
        absent and dropped through to the lookup, which hit an empty
        ``self.objects`` and raised MissingContainerBindingException.
        Wrapping the whole resolution path in the lock forces B to
        wait until A's ``bind()`` completes. Application.make() has
        the same pattern — both must be under their respective locks
        for the serialization to hold end-to-end.
        """
        # (1) If name is a class type — lock covers deferred fire-once
        # + object lookup + DI fallthrough so a racing make() can't see
        # the window between pop() and bind().
        if inspect.isclass(name):
            with self._deferred_lock:
                (
                    GenericContainerException,
                    MissingContainerBindingException,
                    StrictContainerException,
                ) = _get_container_exceptions()
                if name in self._deferred or self._attempt_load_deferred(name):
                    provider_class = self._deferred.pop(name)
                    for k, cls in list(self._deferred.items()):
                        if cls is provider_class:
                            self._deferred.pop(k, None)
                    provider = provider_class(self)
                    provider.register()
                    self.providers.append(provider)
                    if hasattr(provider, "boot"):
                        provider.boot()

                # Try to find a previously bound value matching this class
                try:
                    found = self._find_obj(name)
                    if inspect.isclass(found) and inspect.isabstract(found):
                        concrete = self._find_concrete_binding(name)
                        if concrete:
                            found = concrete
                        else:
                            raise TypeError(
                                f"No concrete implementation found for '{name}'"
                            )
                    self.fire_hook("make", name, found)

                    # If found is a class, resolve it (instantiate with DI)
                    if inspect.isclass(found):
                        instance = self.resolve(found, *arguments)
                        return instance

                    # If found is a callable factory, call it
                    if callable(found):
                        result = found(self) if self._accepts_container(found) else found()
                        return result

                    # Otherwise return the bound value (already an instance)
                    return found

                except MissingContainerBindingException:
                    # If not found in bindings, try to instantiate directly
                    return self.resolve(name, *arguments)

        # (2) String path — serialize deferred register + objects lookup.
        if isinstance(name, str):
            with self._deferred_lock:
                if name in self._deferred:
                    provider_class = self._deferred.pop(name)
                    for k, cls in list(self._deferred.items()):
                        if cls is provider_class:
                            self._deferred.pop(k, None)
                    provider = provider_class(self)
                    provider.register()
                    self.providers.append(provider)
                    if hasattr(provider, "boot"):
                        provider.boot()

                if name in self.objects:
                    bound = self.objects[name]
                    self.fire_hook("make", name, bound)

                    # a) If the bound value is a class, resolve its constructor
                    if inspect.isclass(bound) and inspect.isabstract(bound):
                        concrete = self._find_concrete_binding(name)
                        if concrete:
                            bound = concrete
                        else:
                            raise TypeError(
                                f"No concrete implementation found for '{name}'"
                            )
                    if inspect.isclass(bound):
                        return self.resolve(bound, *arguments)

                    # b) If the bound value is a function or method (factory), call it
                    if inspect.isfunction(bound) or inspect.ismethod(bound):
                        return bound()

                    # c) Otherwise, assume it's already an instance
                    return bound

                # (3) If a swap (test/mock) exists, return that
                if name in self.swaps:
                    return self.swaps[name]

                # (4) No binding found → raise an error
                (
                    GenericContainerException,
                    MissingContainerBindingException,
                    StrictContainerException,
                ) = _get_container_exceptions()
                raise MissingContainerBindingException(
                    f"'{name}' key was not found in the container"
                )

        # Else, fallback: resolve by constructor injection
        return self.resolve(name, *arguments)

    def has(self, name: Any) -> bool:
        """Check if a given key (string) or class exists in the container (either as a direct
        binding or as a deferred provider)."""
        if isinstance(name, str):
            return name in self.objects or name in self._deferred
        try:
            self._find_obj(name)
            return True
        except MissingContainerBindingException:
            if name in self._deferred:
                return True
            return False

    def resolve(self, obj: Any, *resolving_arguments: Any) -> Any:
        """
        Instantiate a class or call a function, performing dependency injection based on constructor
        parameter annotations.

        - Primitive types (str, int, etc.) are taken from *resolving_arguments or defaults.
        - Class-annotated parameters are resolved from the container or triggered as deferred.
        - "self" parameters receive the class itself.
        """
        # Circular dependency guard
        if obj in self._resolving_stack:
            chain = " -> ".join(
                getattr(c, "__name__", str(c)) for c in self._resolving_stack
            )
            name = getattr(obj, "__name__", str(obj))
            (
                GenericContainerException,
                _,
                _,
            ) = _get_container_exceptions()
            raise GenericContainerException(
                f"Circular dependency detected: {chain} -> {name}"
            )
        self._resolving_stack.append(obj)

        try:
            return self._do_resolve(obj, *resolving_arguments)
        finally:
            self._resolving_stack.pop()

    def _do_resolve(self, obj: Any, *resolving_arguments: Any) -> Any:
        """Internal resolve implementation."""
        objects: List[Any] = []
        keyword_objects: Dict[str, Any] = {}
        passing_args = list(resolving_arguments)
        (
            GenericContainerException,
            MissingContainerBindingException,
            StrictContainerException,
        ) = _get_container_exceptions()

        # If remember=True and arguments were cached, use them directly
        if self.remember:
            if obj in self._remembered:
                cached = self._remembered[obj]
                try:
                    return obj(*cached)
                except TypeError as e:
                    (
                        GenericContainerException,
                        MissingContainerBindingException,
                        StrictContainerException,
                    ) = _get_container_exceptions()
                    raise GenericContainerException(str(e)) from e
            if inspect.ismethod(obj):
                signature = (
                    f"{obj.__module__}.{obj.__self__.__class__.__name__}.{obj.__name__}"
                )
                if signature in self._remembered:
                    cached = self._remembered[signature]
                    try:
                        return obj(*cached)
                    except TypeError as e:
                        (
                            GenericContainerException,
                            MissingContainerBindingException,
                            StrictContainerException,
                        ) = _get_container_exceptions()
                        raise GenericContainerException(str(e)) from e

        # Inspect constructor parameters
        for _, param in self.get_parameters(obj):
            if param.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                continue
            ann = param.annotation
            is_keyword_only = param.kind == inspect.Parameter.KEYWORD_ONLY

            # Resolve postponed annotations (`from __future__ import annotations`)
            # so contract strings become real classes for DI.
            if isinstance(ann, str):
                module = inspect.getmodule(obj)
                if module is not None:
                    ann = module.__dict__.get(ann, ann)

            # Treat typing.Any as an untyped slot: pull from passed args
            # or fall back to None/default handling below.
            if ann is Any:
                value = passing_args.pop(0) if passing_args else (
                    param.default if param.default is not inspect._empty else None
                )
                if is_keyword_only:
                    keyword_objects[param.name] = value
                else:
                    objects.append(value)
                continue

            # (1) Primitive types: expect passed argument or default
            if ann in (
                str,
                int,
                float,
                bool,
                dict,
                list,
                tuple,
            ) or (isinstance(ann, type) and ann.__module__ == "builtins"):
                if passing_args:
                    value = passing_args.pop(0)
                else:
                    value = param.default if param.default is not inspect._empty else None
                if is_keyword_only:
                    keyword_objects[param.name] = value
                else:
                    objects.append(value)
                continue

            # (2) If annotation is a class, attempt to resolve it
            if ann is not inspect._empty and inspect.isclass(ann):
                try:
                    dep = self._find_obj(ann)
                    if inspect.isclass(dep):
                        dep = self.resolve(dep)
                    if is_keyword_only:
                        keyword_objects[param.name] = dep
                    else:
                        objects.append(dep)
                    continue
                except MissingContainerBindingException:
                    # Maybe a deferred provider can supply this class
                    if self._attempt_load_deferred(ann):
                        dep = self._find_obj(ann)
                        if inspect.isclass(dep):
                            dep = self.resolve(dep)
                        if is_keyword_only:
                            keyword_objects[param.name] = dep
                        else:
                            objects.append(dep)
                        continue
                    # Otherwise, if user passed an extra argument, use it
                    if passing_args:
                        value = passing_args.pop(0)
                        if is_keyword_only:
                            keyword_objects[param.name] = value
                        else:
                            objects.append(value)
                        continue
                    (
                        GenericContainerException,
                        MissingContainerBindingException,
                        StrictContainerException,
                    ) = _get_container_exceptions()
                    raise GenericContainerException(
                        f"Cannot resolve dependency '{param.name}' of {obj}"
                    )

            # (3) Skip explicit "self" params when present in inspected signatures.
            if param.name == "self":
                continue

            # (4) If a default value is specified in signature, use it
            if param.default is not inspect._empty:
                if is_keyword_only:
                    keyword_objects[param.name] = param.default
                else:
                    objects.append(param.default)
                continue

            # (5) Last resort: use a passed argument if available
            if passing_args:
                value = passing_args.pop(0)
                if is_keyword_only:
                    keyword_objects[param.name] = value
                else:
                    objects.append(value)
                continue

            (
                GenericContainerException,
                MissingContainerBindingException,
                StrictContainerException,
            ) = _get_container_exceptions()
            raise GenericContainerException(
                f"Not enough dependencies passed. Resolving '{obj}' needs parameter '{param.name}'."
            )

        # Cache constructor arguments if remember=True
        if self.remember:
            key = (
                obj
                if not inspect.ismethod(obj)
                else f"{obj.__module__}.{obj.__self__.__class__.__name__}.{obj.__name__}"
            )
            self._remembered[key] = objects.copy()

        return obj(*objects, **keyword_objects)

    # ---------------------------------------
    # Deferred Provider Support Methods
    # ---------------------------------------

    def add_deferred_provider(self, provider: Any) -> None:
        """
        Register a DeferredProvider instance.

        provider.provides() returns a list of service keys (e.g. ["queue", "logger"]).
        """
        provides_list = provider.provides()
        for svc in provides_list:
            self._deferred[svc] = provider

    def _attempt_load_deferred(self, key_or_class: Any) -> bool:
        """
        If key_or_class is in _deferred, pop the provider, register and boot it, then remove any
        other keys pointing to the same provider.

        Return True. Otherwise return False.
        """
        if key_or_class in self._deferred:
            provider = self._deferred.pop(key_or_class)
            provider.register()
            self.providers.append(provider)
            if hasattr(provider, "boot"):
                provider.boot()

            # Remove any other keys that reference the same provider class
            for k, cls in list(self._deferred.items()):
                if cls is provider:
                    self._deferred.pop(k, None)

            return True

        return False

    def _accepts_container(self, func):
        """Check if callable accepts a container parameter."""
        try:
            sig = inspect.signature(func)
            params = list(sig.parameters.keys())
            return len(params) > 0 and params[0] in ("app", "container", "self")
        except (TypeError, ValueError):
            return False

    def fire_hook(self, action: str, key: Any, obj: Any):
        """Fire hooks for bind/make/resolve actions."""
        # If bound object is a class, invoke class-based hooks
        if inspect.isclass(obj) and obj in self._hooks[action]:
            for fn in self._hooks[action][obj]:
                fn(obj, self)

        # If bound object is an instance, check hooks on its class
        if hasattr(obj, "__class__") and obj.__class__ in self._hooks[action]:
            for fn in self._hooks[action][obj.__class__]:
                fn(obj, self)

    def on_bind(self, key: Any, fn: Callable) -> "Container":
        return self._bind_hook("bind", key, fn)

    def on_make(self, key: Any, fn: Callable) -> "Container":
        return self._bind_hook("make", key, fn)

    def on_resolve(self, key: Any, fn: Callable) -> "Container":
        return self._bind_hook("resolve", key, fn)

    def _bind_hook(self, hook: str, key: Any, fn: Callable) -> "Container":
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
        if inspect.isclass(obj) and hasattr(obj, '__module__') and hasattr(obj, '__name__'):
            full_path = f"{obj.__module__}.{obj.__name__}"
            if full_path in self.objects:
                provider_obj = self.objects[full_path]
                self.fire_hook("resolve", obj, provider_obj)
                return provider_obj
        
        # Strategy 3: Try simple class name
        if inspect.isclass(obj) and hasattr(obj, '__name__'):
            if obj.__name__ in self.objects:
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
        ) = _get_container_exceptions()
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
                    if (
                        issubclass(provider_obj, abstract_obj)
                        and not inspect.isabstract(provider_obj)
                    ):
                        return provider_obj
                except TypeError:
                    pass

            try:
                if isinstance(provider_obj, abstract_obj) and not inspect.isabstract(
                    provider_obj.__class__
                ):
                    return provider_obj.__class__
            except (TypeError, AttributeError):
                pass

        return None

    def get_parameters(self, obj: Any):
        """Return inspect.signature(obj).parameters.items() for parameter inspection."""
        return inspect.signature(obj).parameters.items()

    @staticmethod
    def _unwrap_annotation(ann: Any) -> Any:
        """Extract the concrete class from union types like ``X | None`` or ``Optional[X]``."""
        import types

        origin = getattr(ann, "__origin__", None)
        if origin is Union or isinstance(ann, types.UnionType):
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
        """
        sig = inspect.signature(callable_or_method)
        resolved: Dict[str, Any] = {}

        for name, param in sig.parameters.items():
            if name in ("self", "cls") or name in kwargs:
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
                module = inspect.getmodule(callable_or_method)
                if module is not None:
                    ann = module.__dict__.get(ann, ann)

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
                ) = _get_container_exceptions()
                raise GenericContainerException(
                    f"Could not resolve required parameter {name!r} "
                    f"(annotation {ann!r}) for {callable_or_method!r}: {e}"
                ) from e

        merged = {**resolved, **kwargs}
        return callable_or_method(*args, **merged)

    # ----------------------------
    # Wildcard Binding Search (collect)
    # ----------------------------

    def collect(self, search: Any) -> Dict[Any, Any]:
        """
        Collect bindings by wildcard (e.g., '*Service') or by class type.

        - If search is a string containing '*', return any key that matches prefix/suffix.
        - If search is a class, return any bound value that is instance or subclass.
        """
        results: Dict[Any, Any] = {}
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

    def swap(self, obj: Any, callback: Any) -> "Container":
        """Temporarily override a binding for testing or mocking."""
        self.swaps[obj] = callback
        return self

    def __contains__(self, obj: Any) -> bool:
        return self.has(obj)

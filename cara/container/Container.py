"""
Core IoC (Inversion of Control) Container for the Cara framework.

This module provides a powerful dependency injection container that manages object creation and
dependency resolution throughout the application. It supports automatic dependency injection,
singleton bindings, and contextual binding.

The container follows the PSR-11 container interface standard and provides Laravel-style service
container functionality.
"""

import inspect
from typing import Any, Callable, Dict, List

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
            # Lazy singleton: wrap factory to cache result
            cached = {"instance": None}

            def singleton_factory():
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
        """
        # (1) If name is a class (e.g., type-hinted dependency)
        if inspect.isclass(name):
            key_str = name.__name__.lower()

            # If a deferred provider is registered for this class-key, register it now
            if key_str in self._deferred:
                provider_class = self._deferred.pop(key_str)
                # Remove any other keys pointing to the same provider class
                for k, cls in list(self._deferred.items()):
                    if cls is provider_class:
                        self._deferred.pop(k, None)
                provider = provider_class(self)
                provider.register()
                self.providers.append(provider)
                if hasattr(provider, "boot"):
                    provider.boot()

            # Try to find a previously bound instance matching this class
            try:
                found = self._find_obj(name)
                return found
            except MissingContainerBindingException:
                # If not found, perform constructor injection
                return self.resolve(name, *arguments)

        # (2) If name is a string
        if isinstance(name, str) and name in self._deferred:
            provider_class = self._deferred.pop(name)
            for k, cls in list(self._deferred.items()):
                if cls is provider_class:
                    self._deferred.pop(k, None)
            provider = provider_class(self)
            provider.register()
            self.providers.append(provider)
            if hasattr(provider, "boot"):
                provider.boot()

        if isinstance(name, str) and name in self.objects:
            bound = self.objects[name]
            self.fire_hook("make", name, bound)

            # a) If the bound value is a class, resolve its constructor
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
        if isinstance(name, str):
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
        objects: List[Any] = []
        passing_args = list(resolving_arguments)

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
                    raise GenericContainerException(str(e))
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
                        raise GenericContainerException(str(e))

        # Inspect constructor parameters
        for _, param in self.get_parameters(obj):
            ann = param.annotation

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
                    objects.append(passing_args.pop(0))
                else:
                    objects.append(
                        param.default if param.default is not inspect._empty else None
                    )
                continue

            # (2) If annotation is a class, attempt to resolve it
            if inspect.isclass(ann):
                try:
                    dep = self._find_obj(ann)
                    if inspect.isclass(dep):
                        dep = self.resolve(dep)
                    objects.append(dep)
                    continue
                except MissingContainerBindingException:
                    # Maybe a deferred provider can supply this class
                    if self._attempt_load_deferred(ann):
                        dep = self._find_obj(ann)
                        if inspect.isclass(dep):
                            dep = self.resolve(dep)
                        objects.append(dep)
                        continue
                    # Otherwise, if user passed an extra argument, use it
                    if passing_args:
                        objects.append(passing_args.pop(0))
                        continue
                    (
                        GenericContainerException,
                        MissingContainerBindingException,
                        StrictContainerException,
                    ) = _get_container_exceptions()
                    raise GenericContainerException(
                        f"Cannot resolve dependency '{param.name}' of {obj}"
                    )

            # (3) If parameter name is "self", pass the class/function itself
            if param.name == "self":
                objects.append(obj)
                continue

            # (4) If a default value is specified in signature, use it
            if param.default is not inspect._empty:
                objects.append(param.default)
                continue

            # (5) Last resort: use a passed argument if available
            if passing_args:
                objects.append(passing_args.pop(0))
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

        return obj(*objects)

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

    # ----------------------------
    # Hook System (bind / make / resolve)
    # ----------------------------

    def fire_hook(self, action: str, key: Any, obj: Any) -> None:
        """Invoke any callbacks registered for the given hook name and key or class."""
        # Exact key matches
        if key in self._hooks[action]:
            for fn in self._hooks[action][key]:
                fn(obj, self)

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
        Locate a bound object by class or instance:

        1) If obj is a class and provider_obj is an instance of obj, return provider_obj.
        2) If obj exactly matches a bound instance or its class, return provider_obj.
        3) If provider_obj is a class and issubclass(provider_obj, obj), or provider_obj.__class__ issubclass(obj), return provider_obj.
        4) Otherwise raise MissingContainerBindingException.
        """
        for provider_obj in self.objects.values():
            # (1) Class–instance match
            if inspect.isclass(obj) and isinstance(provider_obj, obj):
                self.fire_hook("resolve", obj, provider_obj)
                return provider_obj

            # (2) Exact match: key bound as instance or class
            if obj in (
                provider_obj,
                provider_obj.__class__,
            ):
                self.fire_hook("resolve", obj, provider_obj)
                return provider_obj

            # (3) Subclass match
            if (
                inspect.isclass(provider_obj) and issubclass(provider_obj, obj)
            ) or issubclass(provider_obj.__class__, obj):
                self.fire_hook("resolve", obj, provider_obj)
                return provider_obj

        (
            GenericContainerException,
            MissingContainerBindingException,
            StrictContainerException,
        ) = _get_container_exceptions()
        raise MissingContainerBindingException(
            f"The dependency with the '{obj}' annotation could not be resolved by the container"
        )

    def get_parameters(self, obj: Any):
        """Return inspect.signature(obj).parameters.items() for parameter inspection."""
        return inspect.signature(obj).parameters.items()

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

"""Container binding lookup and constructor dependency resolution."""

from __future__ import annotations

import inspect
import types as _types
from typing import Any, Union


def _get_container_exceptions():
    """Lazy import container exceptions to avoid circular imports."""
    import cara.exceptions as exceptions  # local: cycle with cara.exceptions

    return (
        exceptions.GenericContainerException,
        exceptions.MissingContainerBindingException,
        exceptions.StrictContainerException,
    )


def _extract_optional_class(ann: Any, module: Any) -> type | None:
    """Return the single class wrapped by an ``Optional[X]`` / ``X | None``
    annotation, or ``None`` for anything else.

    Auto-injection only fired for params annotated with a *plain* class
    (``inspect.isclass(ann)``); an ``X | None`` annotation — extremely common
    for "inject in prod, default for unit tests" services — is a ``Union`` /
    ``types.UnionType`` (or, under ``from __future__ import annotations``, the
    raw string ``"X | None"``), neither of which is a class, so the container
    silently fell through to the ``None`` default and the dependency never got
    injected. This recognises the single-class Optional shape in both runtime
    and postponed-string forms so the caller can resolve it like a plain class.
    Multi-arg unions and unresolvable names return ``None`` (unchanged
    behaviour).
    """

    # Runtime union objects (annotations not postponed): ``X | None`` (PEP 604)
    # or ``typing.Optional[X]`` / ``typing.Union[X, None]``.
    if isinstance(ann, _types.UnionType) or getattr(ann, "__origin__", None) is Union:
        args = [a for a in getattr(ann, "__args__", ()) if a is not type(None)]
        return args[0] if len(args) == 1 and inspect.isclass(args[0]) else None

    # Postponed string forms: ``"X | None"``, ``"None | X"``, ``"Optional[X]"``.
    if isinstance(ann, str):
        s = ann.strip()
        name: str | None = None
        if s.startswith("Optional[") and s.endswith("]"):
            name = s[len("Optional[") : -1].strip()
        elif "|" in s:
            non_none = [
                p.strip() for p in s.split("|") if p.strip() not in ("None", "NoneType")
            ]
            if len(non_none) == 1:
                name = non_none[0]
        if name and module is not None:
            cand = module.__dict__.get(name.rsplit(".", 1)[-1])
            if inspect.isclass(cand):
                return cand
    return None


def _container_make(self, name: Any, *arguments: Any) -> Any:
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

    4) Otherwise, raise missing_container_binding_exception.

    ROOT CAUSE (2026-04-24): the lookup used to be split — the
    deferred check + register was inside the lock, but the final
    ``self.objects[name]`` lookup and DI fallthrough ran OUTSIDE
    it. Under concurrent workers, Thread A could pop the key and
    be mid-``register()`` (still computing the config, building
    drivers) while Thread B's lock-free outer check saw the key
    absent and dropped through to the lookup, which hit an empty
    ``self.objects`` and raised missing_container_binding_exception.
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
                generic_container_exception,
                missing_container_binding_exception,
                strict_container_exception,
            ) = _get_container_exceptions()
            # NOTE: no ``or _attempt_load_deferred(name)`` here — that
            # helper POPPED and registered the provider itself, so the
            # pop() below KeyError'd whenever the OR's second arm won.
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

            # Try to find a previously bound value matching this class
            try:
                found = self._find_obj(name)
                if inspect.isclass(found) and inspect.isabstract(found):
                    concrete = self._find_concrete_binding(name)
                    if concrete:
                        found = concrete
                    else:
                        raise TypeError(f"No concrete implementation found for '{name}'")
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

            except missing_container_binding_exception:
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
                        raise TypeError(f"No concrete implementation found for '{name}'")
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
                generic_container_exception,
                missing_container_binding_exception,
                strict_container_exception,
            ) = _get_container_exceptions()
            raise missing_container_binding_exception(
                f"'{name}' key was not found in the container"
            )

    # Else, fallback: resolve by constructor injection
    return self.resolve(name, *arguments)


def _container_do_resolve(self, obj: Any, *resolving_arguments: Any) -> Any:
    """Internal resolve implementation."""
    objects: list[Any] = []
    keyword_objects: dict[str, Any] = {}
    passing_args = list(resolving_arguments)
    (
        generic_container_exception,
        missing_container_binding_exception,
        strict_container_exception,
    ) = _get_container_exceptions()

    # If remember=True and arguments were cached, use them directly
    if self.remember:
        if obj in self._remembered:
            cached = self._remembered[obj]
            try:
                return obj(*cached)
            except TypeError as e:
                (
                    generic_container_exception,
                    missing_container_binding_exception,
                    strict_container_exception,
                ) = _get_container_exceptions()
                raise generic_container_exception(str(e)) from e
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
                        generic_container_exception,
                        missing_container_binding_exception,
                        strict_container_exception,
                    ) = _get_container_exceptions()
                    raise generic_container_exception(str(e)) from e

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

        # Optional[class] / `class | None`: resolve the wrapped class like a
        # plain class, but fail soft — if it isn't bindable, fall back to the
        # signature default (usually None) instead of raising, preserving the
        # "inject in prod, default in tests" contract these params encode.
        optional_cls = _extract_optional_class(ann, inspect.getmodule(obj))
        if optional_cls is not None:
            try:
                dep = self.make(optional_cls)
            except Exception:
                dep = param.default if param.default is not inspect._empty else None
            if is_keyword_only:
                keyword_objects[param.name] = dep
            else:
                objects.append(dep)
            continue

        # Treat typing.Any as an untyped slot: pull from passed args
        # or fall back to None/default handling below.
        if ann is Any:
            value = (
                passing_args.pop(0)
                if passing_args
                else (param.default if param.default is not inspect._empty else None)
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

        # (2) If annotation is a class, resolve via make() which handles
        # deferred providers, lock serialization, and the full lookup
        # chain. Previous code used _find_obj() + manual deferred
        # fallback — a weaker path that missed edge cases (e.g. nested
        # deps whose providers hadn't fired yet).
        if ann is not inspect._empty and inspect.isclass(ann):
            try:
                dep = self.make(ann)
                if is_keyword_only:
                    keyword_objects[param.name] = dep
                else:
                    objects.append(dep)
                continue
            except Exception:
                # If make() fails, try caller-supplied positional args
                if passing_args:
                    value = passing_args.pop(0)
                    if is_keyword_only:
                        keyword_objects[param.name] = value
                    else:
                        objects.append(value)
                    continue
                (
                    generic_container_exception,
                    missing_container_binding_exception,
                    strict_container_exception,
                ) = _get_container_exceptions()
                raise generic_container_exception(
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
            generic_container_exception,
            missing_container_binding_exception,
            strict_container_exception,
        ) = _get_container_exceptions()
        raise generic_container_exception(
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

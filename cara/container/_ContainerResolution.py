"""Container binding lookup and constructor dependency resolution."""

from __future__ import annotations

import inspect
import sys
import threading
import types as _types
from typing import Any, Union

_container_exceptions: tuple[type, type, type] | None = None

# "No binding under this key" — ``None`` is a value a key may be bound to.
_UNBOUND = object()


def _get_container_exceptions():
    """Lazy import container exceptions to avoid circular imports.

    Resolved once. The cycle only exists while the packages are importing;
    after that the answer never changes, yet every ``make`` and ``resolve``
    re-ran the import statement and three lazy-barrel attribute lookups —
    ten times for one controller with three injected dependencies, about a
    third of that resolution's cost.
    """
    global _container_exceptions
    if _container_exceptions is None:
        import cara.exceptions as exceptions  # local: cycle with cara.exceptions

        _container_exceptions = (
            exceptions.GenericContainerException,
            exceptions.MissingContainerBindingException,
            exceptions.StrictContainerException,
        )
    return _container_exceptions


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


def _annotation_module(obj: Any) -> Any:
    """The module whose names a postponed annotation of ``obj``'s signature
    reads: the module that DEFINES the signature's function. A class that
    inherits its constructor has it from the base's module — its own module
    may never import the base constructor's parameter types, so reading the
    strings there left them unresolved and the subclass could not be built.
    """
    target = obj.__init__ if inspect.isclass(obj) else obj
    target = getattr(target, "__func__", target)
    defining = getattr(target, "__module__", None)
    module = sys.modules.get(defining) if isinstance(defining, str) else None
    return module if module is not None else inspect.getmodule(obj)


class _DeferredLoad:
    """One deferred provider being registered: who runs it, and when it is done."""

    __slots__ = ("done", "owner")

    def __init__(self) -> None:
        self.owner = threading.get_ident()
        self.done = threading.Event()


def _container_load_deferred(self, registry: dict[Any, Any], key: Any) -> None:
    """Register and boot the deferred provider behind ``key`` exactly once.

    ``registry`` maps binding keys to provider classes (``Container._deferred``;
    ``Application`` keeps its own). The container lock is held only to move
    the provider's keys from ``registry`` to ``_loading``; ``register()`` and
    ``boot()`` — which build managers, open drivers, read config — run with
    no lock at all. What used to be guaranteed by holding the lock across
    them is kept without it:

    * a second thread asking for one of the provider's keys WAITS for this
      registration instead of finding the key in neither table (the race of
      2026-04-24);
    * the registering thread itself is never made to wait: a ``make`` of the
      provider's own key from inside ``register()``/``boot()`` goes straight
      to the bindings, as it did under the re-entrant lock;
    * two providers that resolve each other's keys while registering on two
      threads do not wait on each other for ever: a wait that would close
      such a circle is skipped, which is the outcome one thread gets;
    * a ``register()`` that raises puts the keys back, so the next ``make``
      (a thread that was waiting included) runs the provider again instead
      of reporting a missing binding for the rest of the process.
    """
    me = threading.get_ident()
    while True:
        with self._deferred_lock:
            provider_class = registry.get(key)
            if provider_class is not None:
                keys = [k for k, cls in registry.items() if cls is provider_class]
                load = _DeferredLoad()
                for k in keys:
                    self._loading[k] = load
                    del registry[k]
                break
            load = self._loading.get(key)
            if load is None or load.owner == me or _closes_wait_circle(self, load, me):
                return
            self._waiting[me] = load
        try:
            load.done.wait()
        finally:
            with self._deferred_lock:
                self._waiting.pop(me, None)
        # Registered by now — or its ``register()`` raised and the key is
        # deferred again, in which case this thread runs it: look once more.

    registered = False
    try:
        provider = provider_class(self)
        provider.register()
        registered = True
        self.providers.append(provider)
        if hasattr(provider, "boot"):
            provider.boot()
    finally:
        with self._deferred_lock:
            for k in keys:
                if self._loading.get(k) is load:
                    del self._loading[k]
                if not registered:
                    registry.setdefault(k, provider_class)
        load.done.set()


def _closes_wait_circle(self, load: _DeferredLoad, me: int) -> bool:
    """Whether waiting for ``load`` would wait on this very thread: its owner
    is itself waiting, directly or through others, for a registration that
    ``me`` is running. Called with the container lock held."""
    seen: set[int] = set()
    owner = load.owner
    while owner not in seen:
        seen.add(owner)
        awaited = self._waiting.get(owner)
        if awaited is None:
            return False
        if awaited.owner == me:
            return True
        owner = awaited.owner
    return False


def _container_make(self, name: Any, *arguments: Any) -> Any:
    """
    Resolve an object from the container.

    1) If `name` is a class:
       - First check deferred providers by class; if found, register and boot it.
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

    **What the lock covers.** Only the registry: the deferred-provider
    hand-off (``load_deferred``) and the binding lookup. The whole resolution
    used to run inside it — the constructor call included — so one slow or
    blocking constructor (a client that connects in ``__init__``, a factory
    that reads a file) stalled every ``make`` of every other thread, for
    bindings that had nothing to do with it. The race that lock was widened
    for (2026-04-24: a second thread finding a deferred key in neither
    table while the first was still registering it) is closed by
    ``load_deferred`` itself, which makes that second thread wait for THAT
    provider and for nothing else.

    Construction runs unlocked. A lazy singleton is still built exactly once:
    its factory carries its own lock (``Container.singleton``), held for that
    binding alone, released when its factory raises, and re-entrant
    resolution from inside a constructor takes no lock this thread could be
    waiting on.
    """
    (
        generic_container_exception,
        missing_container_binding_exception,
        strict_container_exception,
    ) = _get_container_exceptions()

    # (1) If name is a class type
    if inspect.isclass(name):
        # NOTE: no ``or _attempt_load_deferred(name)`` here — that
        # helper POPPED and registered the provider itself, so the
        # pop() KeyError'd whenever the OR's second arm won.
        self.load_deferred(self._deferred, name)

        # Try to find a previously bound value matching this class
        try:
            with self._deferred_lock:
                found = self._find_obj(name)
                if inspect.isclass(found) and inspect.isabstract(found):
                    concrete = self._find_concrete_binding(name)
                    if concrete:
                        found = concrete
                    else:
                        raise TypeError(f"No concrete implementation found for '{name}'")

            # If found is a class, resolve it (instantiate with DI)
            if inspect.isclass(found):
                return self.resolve(found, *arguments)

            # If found is a callable factory, call it
            if callable(found):
                return found(self) if self._accepts_container(found) else found()

            # Otherwise return the bound value (already an instance)
            return found

        except missing_container_binding_exception:
            # If not found in bindings, try to instantiate directly
            return self.resolve(name, *arguments)

    # (2) String path
    if isinstance(name, str):
        self.load_deferred(self._deferred, name)

        with self._deferred_lock:
            bound = self.objects.get(name, _UNBOUND)
            if bound is _UNBOUND:
                # (3) If a swap (test/mock) exists, return that
                if name in self.swaps:
                    return self.swaps[name]

                # (4) No binding found → raise an error
                raise missing_container_binding_exception(
                    f"'{name}' key was not found in the container"
                )

            if inspect.isclass(bound) and inspect.isabstract(bound):
                concrete = self._find_concrete_binding(name)
                if concrete:
                    bound = concrete
                else:
                    raise TypeError(f"No concrete implementation found for '{name}'")

        # a) If the bound value is a class, resolve its constructor
        if inspect.isclass(bound):
            return self.resolve(bound, *arguments)

        # b) If the bound value is a function or method (factory), call it
        if inspect.isfunction(bound) or inspect.ismethod(bound):
            return bound()

        # c) Otherwise, assume it's already an instance
        return bound

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
            module = _annotation_module(obj)
            if module is not None:
                ann = module.__dict__.get(ann, ann)

        # Optional[class] / `class | None`: resolve the wrapped class like a
        # plain class, but fail soft — if it isn't bindable, fall back to the
        # signature default (usually None) instead of raising, preserving the
        # "inject in prod, default in tests" contract these params encode.
        optional_cls = _extract_optional_class(ann, _annotation_module(obj))
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

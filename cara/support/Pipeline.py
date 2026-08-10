"""Pipeline system for sequential data processing.

Provides a flexible pipeline implementation for processing data through a series of
handlers. Each handler can modify the input before passing it to the next stage.

Similar to Laravel's Pipeline implementation - enables middleware chains and other
sequential processing patterns.

The Laravel-canonical fluent API is the only surface:
``Pipeline.send(payload).through([...]).via("handle").then(destination)``.
Both sync and async ``then()`` callables are supported; sync chains stay
fully synchronous so callers can use the pipeline outside an event loop.
Calling a pipeline — ``await Pipeline(passable, app).through([...])(handler)``,
the shape the middleware capsules use — is an awaitable alias for ``then()``
and nothing more.

Pre-fix ``__call__`` was a second, independently written dispatch loop
advertised as a "legacy async API". It had drifted: it hardcoded
``pipe.handle`` so a caller who set ``via("process")`` had it silently
discarded, it could not run a plain-callable pipe at all, and it appended to
``executed_instances`` itself instead of going through ``_resolve_pipe`` — so
every change to instance tracking had to be made twice. One behaviour, two
implementations, is the failure the doctrine's no-shims rule exists to
prevent.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from functools import reduce
from typing import Any


class Pipeline:
    """Pipeline class for sequential data processing.

    Implements a pipeline pattern where data can be processed through a series of pipes
    (middleware/handlers). Each pipe can transform the data and control the flow to the
    next pipe in the sequence.

    Supports both synchronous and asynchronous pipes. Each pipe should have a handle()
    method that accepts the request and a next callable.
    """

    # Maximum pipeline depth to prevent stack overflow from misconfigured
    # middleware chains. Python default recursion limit is ~1000; keep well
    # below that to leave room for application code.
    MAX_DEPTH = 200

    def __init__(self, passable: Any = None, application: Any | None = None) -> None:
        """Initialize the pipeline.

        Args:
            passable: The object to pass through the pipeline. May be set
                here or via :meth:`send` for the Laravel-canonical fluent
                construction.
            application: Optional application instance for dependency injection
                when pipes are registered as classes.
        """
        self.passable = passable
        self.application = application
        self.pipes: list[type | Any] = []
        self.method = "handle"
        # Track instantiated pipe instances so terminate() can reuse them
        # instead of creating fresh objects that lack request-time state.
        self.executed_instances: list[Any] = []

    # ── Laravel-canonical fluent API ────────────────────────────────

    @classmethod
    def send(cls, passable: Any, application: Any | None = None) -> Pipeline:
        """Set the object that gets passed through the pipeline.

        Mirrors Laravel's ``Pipeline::send($payload)`` static entry point.
        Returns a new pipeline instance for method chaining::

            result = (
                Pipeline.send(payload)
                .through([NormalizeFields, ApplyDefaults, Validate])
                .then(lambda x: x)
            )
        """
        instance = cls(passable, application)
        return instance

    def via(self, method: str) -> Pipeline:
        """Override the method name pipes are called with.

        Mirrors Laravel's ``Pipeline::via('method')``. Default is
        ``"handle"`` to match every existing cara pipe; override only
        when integrating a third-party class that uses a different
        method name.
        """
        self.method = method
        return self

    def then(self, destination: Callable[[Any], Any] | None = None) -> Any:
        """Run the pipeline and pass the result to ``destination``.

        Mirrors Laravel's ``Pipeline::then(fn)``. Auto-detects whether
        the chain is sync (every pipe + destination is sync) or async
        (any one is a coroutine function) and dispatches accordingly:

        * Fully sync chain → returns the destination's return value.
        * Any async pipe / async destination → returns an awaitable;
          callers ``await pipeline.then(...)``.

        ``destination`` defaults to identity (``lambda payload: payload``)
        so callers who want a "transform-only" chain don't have to
        pass one.
        """
        if destination is None:
            destination = lambda payload: payload  # noqa: E731

        if len(self.pipes) > self.MAX_DEPTH:
            raise RuntimeError(
                f"Pipeline depth ({len(self.pipes)}) exceeds maximum "
                f"({self.MAX_DEPTH}). Check for circular middleware."
            )

        # Decide sync vs async based on the destination + every pipe.
        if self._chain_is_async(destination):
            return self._then_async(destination)
        return self._then_sync(destination)

    def then_return(self) -> Any:
        """Run the pipeline and return the (possibly transformed) payload.

        Mirrors Laravel's ``Pipeline::thenReturn()`` — equivalent to
        ``then(lambda x: x)``, with the same sync/async auto-detection.
        """
        return self.then()

    # ── Internal sync / async dispatch ──────────────────────────────

    def _chain_is_async(self, destination: Callable) -> bool:
        """Detect whether any pipe or the destination is async."""
        if inspect.iscoroutinefunction(destination):
            return True
        for pipe in self.pipes:
            target = pipe
            if isinstance(pipe, type):
                # Look up the bound ``method`` on the class itself.
                target = getattr(pipe, self.method, None)
            elif callable(pipe) and not hasattr(pipe, self.method):
                target = pipe
            else:
                target = getattr(pipe, self.method, None)
            if target is None:
                continue
            if inspect.iscoroutinefunction(target):
                return True
        return False

    def _resolve_pipe(self, pipe: Any) -> Any:
        """Materialise a pipe — instantiate classes, leave callables alone.

        Every pipe the chain actually reaches is recorded in
        ``executed_instances``, whether the caller handed us a class or an
        already-built instance. The conductors terminate exactly this list, so
        a pre-instantiated middleware that was skipped here never got its
        ``terminate()`` called.
        """
        if isinstance(pipe, type):
            pipe = pipe(self.application) if self.application else pipe()
        self.executed_instances.append(pipe)
        return pipe

    def _invoke(self, pipe: Any, request: Any, next_callable: Callable) -> Any:
        """Call ``pipe`` with ``(request, next_callable)`` using the right shape.

        Pipes can be:

        * Plain callable: ``pipe(request, next)`` — Laravel-style closures.
        * Object with ``self.method`` (default ``handle``):
          ``pipe.handle(request, next)``.
        """
        method_attr = (
            getattr(pipe, self.method, None) if not callable_only(pipe) else None
        )
        if method_attr is not None and not isinstance(pipe, type):
            return method_attr(request, next_callable)
        # Plain callable (function / lambda / class without bound method)
        return pipe(request, next_callable)

    def _then_sync(self, destination: Callable[[Any], Any]) -> Any:
        """Synchronous run-through using ``functools.reduce``."""

        # Build the pipeline backwards: each closure wraps the next.
        def carry(stack, pipe):
            def closure(payload):
                resolved = self._resolve_pipe(pipe)
                return self._invoke(resolved, payload, stack)

            return closure

        # ``reduce(carry, reversed(pipes), destination)`` collapses the
        # list of pipes into one callable that, when invoked with the
        # initial payload, walks the chain and finally hits destination.
        composed = reduce(carry, reversed(self.pipes), destination)
        return composed(self.passable)

    async def _then_async(self, destination: Callable[[Any], Any]) -> Any:
        """Async run-through — mirrors the sync version with awaits."""

        async def call(idx: int, payload: Any) -> Any:
            if idx >= len(self.pipes):
                result = destination(payload)
                if inspect.isawaitable(result):
                    result = await result
                return result
            pipe = self._resolve_pipe(self.pipes[idx])
            next_callable = lambda p: call(idx + 1, p)  # noqa: E731
            result = self._invoke(pipe, payload, next_callable)
            if inspect.isawaitable(result):
                result = await result
            return result

        return await call(0, self.passable)

    # ── Chain construction and the callable alias ───────────────────

    def through(self, pipes: list[type | Any]) -> Pipeline:
        """Set the objects to send through the pipeline.

        Args:
            pipes: List of pipe objects or pipe classes to process

        Returns:
            Self for method chaining
        """
        self.pipes = pipes
        return self

    async def __call__(
        self, final_handler: Callable[[Any], Awaitable[Any]] | None = None
    ) -> Any:
        """Run the pipeline — an awaitable alias for :meth:`then`.

        This is the shape the HTTP and WebSocket conductors use
        (``await pipeline.through(middleware)(handler)``). It owns no dispatch
        logic of its own: everything goes through ``then()`` so ``via()``, the
        depth ceiling, plain-callable pipes and ``executed_instances`` behave
        identically no matter which entry point a caller reaches for.

        A fully synchronous chain makes ``then()`` return a plain value rather
        than an awaitable, so the result is awaited only when it is awaitable —
        a bare ``await self.then(...)`` would raise ``TypeError`` on a sync
        chain reached through this ``async def``.

        Args:
            final_handler: Optional final handler to call at the end.
                Defaults to identity, matching ``then()``.

        Returns:
            The processed passable after going through all pipes

        Raises:
            RuntimeError: If the pipeline exceeds MAX_DEPTH pipes
        """
        result = self.then(final_handler)
        if inspect.isawaitable(result):
            result = await result
        return result


def callable_only(value: Any) -> bool:
    """Return True for plain callables (function / lambda / class) WITHOUT
    a Laravel-style ``handle`` method bound on them.

    Used by :meth:`Pipeline._invoke` to decide between
    ``pipe.handle(request, next)`` (object-with-method dispatch) and
    ``pipe(request, next)`` (plain-callable dispatch).
    """
    if isinstance(value, type):
        return False
    if not callable(value):
        return False
    # An instance with a ``handle`` method is dispatched via ``handle``;
    # plain functions/lambdas do not expose that method.
    return not hasattr(value, "handle")


__all__ = ["Pipeline", "callable_only"]

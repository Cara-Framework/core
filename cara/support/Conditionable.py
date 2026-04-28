"""Conditionable mixin — Laravel's ``Conditionable`` trait equivalent.

Adds ``when(value, callback)`` and ``unless(value, callback)`` methods
to any class so callers can branch fluently inside a builder chain
without breaking the chain::

    query = (
        repo.query()
        .when(user_id, lambda q, uid: q.where("user_id", uid))
        .unless(include_archived, lambda q: q.where_null("archived_at"))
        .order_by("created_at", "desc")
    )

Mirrors Laravel's ``Illuminate\\Support\\Traits\\Conditionable`` —
the ``$value`` argument is forwarded to the callback so closures
don't need to capture it again.

Usage:

    class MyBuilder(Conditionable):
        def where(self, ...): ...

    builder.when(filters.get("brand"), lambda b, brand: b.where("brand", brand))
"""

from __future__ import annotations

from typing import Any, Callable, Optional


class Conditionable:
    """Mixin that adds ``when()`` / ``unless()`` to any builder.

    Both methods evaluate ``value`` (resolving callables via
    :func:`cara.helpers.value`) and invoke the callback with the
    builder + the resolved value when truthy. The mixin returns
    ``self`` to keep the fluent chain unbroken.
    """

    def when(
        self,
        value: Any,
        callback: Optional[Callable[..., Any]] = None,
        default: Optional[Callable[..., Any]] = None,
    ) -> "Conditionable":
        """Run ``callback(self, value)`` if ``value`` is truthy.

        ``callback`` and ``default`` may be plain callables; if a
        callable returns a non-``None`` value it replaces ``self``
        for the rest of the chain (Laravel parity for the rare
        "swap the builder mid-chain" pattern).

        Args:
            value: The condition. Truthy → ``callback`` runs;
                falsy → optional ``default`` runs (Laravel parity).
                When ``value`` itself is a callable, it's invoked
                with ``self`` to derive the actual condition (matches
                Laravel's ``$value = $value($this)`` shorthand).
            callback: ``(builder, resolved_value) -> builder | None``.
                Called when condition is truthy. Receiving ``None``
                from the callback keeps ``self`` as the chain target.
            default: Optional ``(builder, resolved_value) -> builder | None``.
                Called when condition is falsy. Skipped when omitted.
        """
        if callable(value) and not isinstance(value, type):
            resolved = value(self)
        else:
            resolved = value

        if resolved:
            if callback is not None:
                returned = callback(self, resolved)
                if returned is not None:
                    return returned
        elif default is not None:
            returned = default(self, resolved)
            if returned is not None:
                return returned
        return self

    def unless(
        self,
        value: Any,
        callback: Optional[Callable[..., Any]] = None,
        default: Optional[Callable[..., Any]] = None,
    ) -> "Conditionable":
        """Inverse of :meth:`when` — runs ``callback`` when ``value`` is falsy.

        Same semantics as :meth:`when` but with the truthiness check
        flipped. ``default`` runs when ``value`` is truthy.
        """
        if callable(value) and not isinstance(value, type):
            resolved = value(self)
        else:
            resolved = value

        if not resolved:
            if callback is not None:
                returned = callback(self, resolved)
                if returned is not None:
                    return returned
        elif default is not None:
            returned = default(self, resolved)
            if returned is not None:
                return returned
        return self


__all__ = ["Conditionable"]

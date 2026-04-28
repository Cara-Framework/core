"""Tappable mixin — Laravel's ``Tappable`` trait equivalent.

Adds a fluent ``.tap(callback)`` method to any class so callers can
peek at / mutate the object mid-chain without breaking the chain::

    user = (
        repo.find(id)
        .tap(lambda u: Log.info(f"loaded {u.email}"))
        .tap(lambda u: u.touch())
    )

Different from :func:`cara.helpers.tap` which is a plain function;
this mixin gives the same semantics as a method bound to the
instance, matching Laravel's trait shape.

Mirrors ``Illuminate\\Support\\Traits\\Tappable``.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Union

from .HigherOrderTapProxy import HigherOrderTapProxy


class Tappable:
    """Mixin that adds ``self.tap(callback)`` for fluent peeking.

    The callback is called with ``self`` and its return value is
    discarded — ``tap`` always returns ``self`` so the chain
    continues against the original object. To swap the chain target
    use a regular method that returns a new instance instead.

    When called with no callback, returns a :class:`HigherOrderTapProxy`
    so the next method call on the proxy runs on ``self`` but yields
    ``self`` back to the chain — Laravel's "higher-order tap" feature.
    """

    def tap(
        self, callback: Optional[Callable[["Tappable"], Any]] = None
    ) -> Union["Tappable", HigherOrderTapProxy]:
        """Pass ``self`` through ``callback`` and return ``self``.

        Mirrors Laravel's ``$obj->tap(fn ($x) => ...)``. With no
        callback, returns a higher-order proxy that runs the next
        method call on ``self`` but returns ``self`` (single-shot,
        matching Laravel)::

            user.tap().save()         # save() runs on user, chain → user
            user.tap().save().reload() # reload() runs on user (proxy is one-shot)
        """
        if callback is None:
            return HigherOrderTapProxy(self)
        callback(self)
        return self


__all__ = ["Tappable"]

"""HigherOrderTapProxy — call methods on a tapped object, return original.

Laravel's higher-order tap is one of those features that reads as
magic until you've used it once. ``Tappable.tap()`` without a
callback returns a proxy that forwards the **next** method call
to the underlying object but returns the underlying object —
single-shot, matching Laravel's parity::

    # Save the user, but throw away whatever save() returns —
    # downstream code keeps using ``user`` directly.
    user = User.create(payload).tap().save()

    # For multi-step chains, tap() each step:
    user = (
        User.create(payload)
        .tap().save()
        .tap().send_welcome_email()
    )

Without higher-order tap, every chainable side-effect method has
to remember to ``return self`` — which is the kind of discipline
that breaks the moment someone adds a method that genuinely needs
to return something else. The proxy makes "tap, then ignore the
return" a one-keystroke pattern.

Used by :class:`cara.support.Tappable.Tappable.tap` when called
with no callback.
"""

from __future__ import annotations

from typing import Any, Generic, TypeVar

T = TypeVar("T")


class HigherOrderTapProxy(Generic[T]):
    """Forward method calls to ``target`` but return ``target``."""

    __slots__ = ("_target",)

    def __init__(self, target: T) -> None:
        # ``object.__setattr__`` bypasses our overridden ``__setattr__``
        # so we can stash the wrapped target without infinite recursion.
        object.__setattr__(self, "_target", target)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._target, name)
        if not callable(attr):
            # Non-method attribute access just returns the value —
            # mirrors Laravel where ``$user->tap->name`` returns the
            # name string (no tap semantics for plain reads).
            return attr

        target = self._target

        def proxy(*args: Any, **kwargs: Any) -> T:
            attr(*args, **kwargs)
            # The whole point: discard whatever the method returned
            # and hand the original target back to the chain.
            return target

        return proxy

    def __setattr__(self, name: str, value: Any) -> None:
        # Writes pass straight through to the target — useful for
        # ``obj.tap().some_field = value`` without breaking the chain.
        setattr(self._target, name, value)

    def __repr__(self) -> str:  # pragma: no cover — debug aid
        return f"HigherOrderTapProxy({self._target!r})"


__all__ = ["HigherOrderTapProxy"]

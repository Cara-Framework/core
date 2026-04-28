"""Lottery — probabilistic execution helper.

Laravel's ``Illuminate\\Support\\Lottery`` parity. Run a callback
with a given probability — useful for sampling expensive
diagnostics, A/B routing, jittered telemetry::

    Lottery.odds(1, 100).winner(lambda: log_slow_query()).choose()

    # Shorthand: 1% chance of running, otherwise no-op.
    Lottery.odds(1, 100).winner(slow_path).loser(fast_path).choose()

The :meth:`force` mode pins the outcome for tests so you don't get
flaky probabilistic assertions::

    Lottery.force(True)  # every odds() always wins
    try:
        run_traced_request()
    finally:
        Lottery.force(None)  # reset to real RNG

Stateless / threadsafe — pure-function over ``random.random``.
"""

from __future__ import annotations

import random
from typing import Any, Callable, ClassVar, Optional


class Lottery:
    """Run a callback with ``chances/total`` probability."""

    __slots__ = ("_chances", "_total", "_winner", "_loser")

    # When set, every ``choose()`` returns this outcome regardless of
    # the actual roll — for deterministic tests.
    _forced: ClassVar[Optional[bool]] = None

    def __init__(self, chances: int, total: int) -> None:
        if total <= 0:
            raise ValueError("total must be positive")
        if chances < 0:
            raise ValueError("chances must be non-negative")
        self._chances = chances
        self._total = total
        self._winner: Optional[Callable[[], Any]] = None
        self._loser: Optional[Callable[[], Any]] = None

    @classmethod
    def odds(cls, chances: int, total: int = 100) -> "Lottery":
        """Construct a lottery with ``chances`` in ``total`` odds."""
        return cls(chances, total)

    def winner(self, callback: Callable[[], Any]) -> "Lottery":
        """Set the callback to run when the lottery wins — chainable."""
        self._winner = callback
        return self

    def loser(self, callback: Callable[[], Any]) -> "Lottery":
        """Set the callback to run when the lottery loses — chainable."""
        self._loser = callback
        return self

    def choose(self) -> Any:
        """Roll the dice and run winner/loser. Returns the chosen callable's result."""
        if self.wins():
            return self._winner() if self._winner is not None else None
        return self._loser() if self._loser is not None else None

    def wins(self) -> bool:
        """Return True if this roll wins. Honours :meth:`force`."""
        if Lottery._forced is not None:
            return Lottery._forced
        return random.random() < (self._chances / self._total)

    def __call__(self) -> Any:
        """Allow ``Lottery.odds(1, 10).winner(fn)()`` shorthand."""
        return self.choose()

    @classmethod
    def force(cls, outcome: Optional[bool]) -> None:
        """Pin every future roll to ``True``, ``False``, or unset (``None``).

        Lifetime is process-wide — call from test setup/teardown to
        bracket assertions. ``None`` returns to real-RNG behaviour.
        """
        cls._forced = outcome


__all__ = ["Lottery"]

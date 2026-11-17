"""The seconds one outbound call may still sleep across all its retries.

Split out of :mod:`cara.support.CallRetry` so that module owns behaviour only
(the retry loop) and this one owns the one class it hands around. A caller
that waits before sending (pacing, a token bucket) spends from the same
budget the retry loop draws on.
"""

from __future__ import annotations

#: The most one call sleeps across all its retries unless the caller widens it.
CALL_WAIT_BUDGET_S = 20.0


class CallWaitBudget:
    """Seconds one call may still sleep. A caller that waits before sending
    (pacing, a token bucket) spends from the same budget the loop draws on."""

    __slots__ = ("left",)

    def __init__(self, seconds: float = CALL_WAIT_BUDGET_S) -> None:
        self.left = float(seconds)

    def affords(self, seconds: float) -> bool:
        return float(seconds) <= self.left

    def spend(self, seconds: float) -> None:
        self.left -= max(0.0, float(seconds))


__all__ = ["CALL_WAIT_BUDGET_S", "CallWaitBudget"]

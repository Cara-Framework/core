"""An established socket's frames spend a budget, not the whole server.

``ws.throttle`` meters the upgrade; once connected, a client could send
subscribe frames — each an authorization query — as fast as the network
carried them. ``MessageBudget`` is the per-connection allowance a socket
controller spends on every inbound frame.
"""

from __future__ import annotations

import pytest

from cara.websocket import MessageBudget


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_a_fresh_connection_may_spend_its_whole_burst() -> None:
    budget = MessageBudget(5, 60, clock=_Clock())

    assert [budget.spend() for _ in range(5)] == [True] * 5
    assert budget.spend() is False


def test_the_budget_refills_at_its_pace() -> None:
    clock = _Clock()
    budget = MessageBudget(60, 60, clock=clock)  # one frame a second
    for _ in range(60):
        assert budget.spend()
    assert budget.spend() is False

    clock.now += 1.0
    assert budget.spend() is True
    assert budget.spend() is False

    clock.now += 3600  # never refills past its capacity
    assert [budget.spend() for _ in range(61)].count(True) == 60


@pytest.mark.parametrize(("limit", "window"), [(0, 60), (-1, 60), (True, 60), (5, 0)])
def test_an_ambiguous_budget_is_refused(limit, window) -> None:
    with pytest.raises(ValueError):
        MessageBudget(limit, window)

"""Pin ``cara.support.drain_batches`` — the bounded retention drain."""

from __future__ import annotations

import pytest

from cara.support import drain_batches


def _steps(*counts):
    it = iter(counts)
    calls = []

    def step():
        calls.append(1)
        return next(it)

    return step, calls


def test_stops_on_a_partial_batch() -> None:
    step, calls = _steps(10, 10, 3, 10)
    assert drain_batches(step, batch_size=10, max_batches=50) == 23
    assert len(calls) == 3


def test_caps_a_backlog_at_max_batches() -> None:
    step, calls = _steps(*([10] * 100))
    assert drain_batches(step, batch_size=10, max_batches=4) == 40
    assert len(calls) == 4


def test_none_reads_as_zero() -> None:
    step, calls = _steps(None)
    assert drain_batches(step, batch_size=10, max_batches=5) == 0
    assert len(calls) == 1


def test_step_error_propagates() -> None:
    def step():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        drain_batches(step, batch_size=1, max_batches=1)


@pytest.mark.parametrize(("batch", "cap"), [(0, 1), (1, 0)])
def test_rejects_non_positive_bounds(batch, cap) -> None:
    with pytest.raises(ValueError):
        drain_batches(lambda: 0, batch_size=batch, max_batches=cap)

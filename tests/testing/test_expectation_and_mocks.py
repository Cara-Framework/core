"""Assertions in the toolkit that could pass without asserting anything."""

from __future__ import annotations

import pytest

from cara.testing import ExpectationFailed, Mock, expect

# ── expect(...).not_ ──────────────────────────────────────────────────────


def test_not_negates_only_the_next_assertion() -> None:
    """``not_`` stayed set for the rest of the chain: the correct second link
    failed ("Expected NOT value to equal 5") and a WRONG one passed."""
    expect(5).not_.to_be_none().to_equal(5)
    with pytest.raises(ExpectationFailed, match=r"Expected value to equal 6, got 5"):
        expect(5).not_.to_be_none().to_equal(6)


def test_not_still_inverts_the_assertion_it_precedes() -> None:
    expect(1).not_.to_equal(2)
    with pytest.raises(ExpectationFailed, match="NOT"):
        expect(1).not_.to_equal(1)
    # Each ``not_`` is spent by the link after it.
    expect([1, 2]).not_.to_contain(3).not_.to_be_empty().to_have_length(2)


def test_a_negated_to_throw_says_what_was_raised() -> None:
    with pytest.raises(ExpectationFailed, match=r"NOT to raise ValueError, got 'bad'"):
        expect(lambda: (_ for _ in ()).throw(ValueError("bad"))).not_.to_throw(ValueError)


# ── to_be_close_to ────────────────────────────────────────────────────────


def test_a_stated_tolerance_is_an_absolute_bound_at_any_magnitude() -> None:
    """``math.isclose``'s default RELATIVE tolerance (1e-9) rode along with
    the stated one, so near 1e9 "within 0.01" let 0.4 through and two
    epoch-millisecond stamps 1.5 s apart were "within 1"."""
    with pytest.raises(ExpectationFailed, match="within 0.01"):
        expect(1_000_000_000.4).to_be_close_to(1_000_000_000.0, tolerance=0.01)
    with pytest.raises(ExpectationFailed, match="within 1 of"):
        expect(1_700_000_001_500).to_be_close_to(1_700_000_000_000, tolerance=1)
    expect(1_000_000_000.4).to_be_close_to(1_000_000_000.0, tolerance=0.5)
    expect(1_700_000_000_001).to_be_close_to(1_700_000_000_000, tolerance=1)


def test_without_a_tolerance_it_is_still_float_noise_equality() -> None:
    expect(0.1 + 0.2).to_be_close_to(0.3)
    expect(1e10 * 1.1).to_be_close_to(11_000_000_000.0)  # one ulp apart
    with pytest.raises(ExpectationFailed):
        expect(0.5).to_be_close_to(0.5001)


# ── strict Mock ───────────────────────────────────────────────────────────


class _Gateway:
    def charge(self, amount: int) -> bool: ...

    def refund(self, amount: int) -> bool: ...


def _swallowing_caller(gateway: _Gateway) -> str:
    """Code under test that reports ANY failure as a soft error."""
    try:
        gateway.refund(5)
    except Exception:
        return "soft-failed"
    return "ok"


def test_an_unexpected_call_is_recorded_even_when_the_caller_swallows_it() -> None:
    """The strict mock's ``AssertionError`` is an ``Exception``: swallowed by
    the code under test, it left no trace, and ``assert_not_called`` passed
    for a method that had just been called."""
    gateway = Mock(_Gateway)
    gateway.expects("charge").returns(True)

    assert _swallowing_caller(gateway) == "soft-failed"

    assert gateway.call_count("refund") == 1
    assert gateway.calls_to("refund")[0].args == (5,)
    with pytest.raises(AssertionError, match=r"refund\(\) to NOT be called, got 1x"):
        gateway.assert_not_called("refund")


def test_an_undeclared_method_still_raises_at_the_call_site() -> None:
    gateway = Mock(_Gateway)
    with pytest.raises(AssertionError, match="Unexpected call to refund"):
        gateway.refund(5)
    gateway.assert_not_called("charge")

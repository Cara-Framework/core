"""``FacadeSwap.register`` / ``swap`` refuse a key no facade resolves through.

The live defect: the fake was stored under whatever key the test gave, and
``_patched_getattr`` serves a fake only when a facade's ``key`` matches it.
``register("event", fake)`` (the facade's key is ``"events"``) therefore
swapped NOTHING: the test believed it had faked the facade while every call
went to the real service.
"""

from __future__ import annotations

import importlib

import pytest

from cara.exceptions import InvalidArgumentException
from cara.facades import Event, Facade

facade_swap = importlib.import_module("cara.testing.FacadeSwap")


class Shipping(metaclass=Facade):
    """An application's own facade, known because its module is imported."""

    key = "shipping"


class _Fake:
    def ping(self) -> str:
        return "fake"


@pytest.fixture(autouse=True)
def _isolated_fakes():
    before = dict(facade_swap._FAKES)
    yield
    facade_swap._FAKES.clear()
    facade_swap._FAKES.update(before)


def test_register_refuses_a_misspelt_key_and_lists_the_valid_ones():
    with pytest.raises(InvalidArgumentException) as raised:
        facade_swap.register("event", _Fake())

    message = str(raised.value)
    assert "'event'" in message
    assert "events" in message and "cache" in message
    assert "event" not in facade_swap._FAKES


def test_swap_refuses_an_unknown_key_before_binding_anything():
    entered = []
    with (
        pytest.raises(InvalidArgumentException, match="valid keys"),
        facade_swap.swap("not-a-facade", _Fake()),
    ):
        entered.append(True)

    assert entered == []

    assert "not-a-facade" not in facade_swap._FAKES


def test_a_real_facade_key_is_swapped_and_served():
    with facade_swap.swap("events", _Fake()):
        assert Event.ping() == "fake"


def test_an_application_facade_key_is_accepted():
    facade_swap.register("shipping", _Fake())

    assert Shipping.ping() == "fake"

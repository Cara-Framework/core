"""Canonical definition of ``FakeExhaustedError``."""

from __future__ import annotations

from cara.exceptions import CaraException


class FakeExhaustedError(CaraException, AssertionError):
    """A response sequence ran out of entries."""

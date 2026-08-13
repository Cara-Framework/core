"""Canonical definition of ``StrayHttpRequestError``."""

from __future__ import annotations

from cara.exceptions import CaraException


class StrayHttpRequestError(CaraException, AssertionError):
    """A faked test made an HTTP request no fake pattern covers."""

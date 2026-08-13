"""ExpectationFailed."""

from __future__ import annotations

from cara.exceptions import CaraException


class ExpectationFailed(CaraException, AssertionError):
    """Raised when an expectation fails. Subclasses ``AssertionError``
    so pytest displays it the same way as ``assert`` failures."""

"""Canonical definition of ``StepFailed``."""

from __future__ import annotations

from cara.exceptions import CaraException


class StepFailed(CaraException):
    """A pipeline step signalled failure via a non-zero exit code.

    Raised internally so a step whose ``handle()`` RETURNS a non-zero
    craft exit code flows into the same failure path as one that raised —
    it must not be counted as a completed step.

    Inside the taxonomy (§9) rather than rooted at bare ``Exception``:
    an orphan carries no ``status_code``, so if one ever escapes a
    command into the ASGI handler ``get_status_code`` takes its
    "default to 500 for unknown exceptions" branch and the framework
    reports somebody else's step failure as an unclassified server
    fault. One taxonomy means one mapping.
    """

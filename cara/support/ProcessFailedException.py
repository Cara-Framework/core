"""Canonical definition of ``ProcessFailedException``."""

from __future__ import annotations

from cara.exceptions import CaraException

from .ProcessResult import ProcessResult


class ProcessFailedException(CaraException, RuntimeError):
    """Raised by :meth:`ProcessResult.throw_on_failure` for non-zero exits.

    Inside the taxonomy (§9), with ``RuntimeError`` kept as a SECOND base:
    craft commands and jobs that shell out already write
    ``except RuntimeError`` around ``throw_on_failure``, and dropping that
    base would convert their handled failure into an unhandled one. Rooted
    only at ``RuntimeError`` it was invisible to ``except CaraException``,
    which is how a subprocess failure reached the handler as an
    unclassified 500 instead of a framework error with a mapping.
    """

    def __init__(self, result: ProcessResult) -> None:
        self.result = result
        super().__init__(
            f"Process {result.command()!r} exited with code {result.exit_code()}: "
            f"{result.error_output().strip() or '<no stderr>'}"
        )

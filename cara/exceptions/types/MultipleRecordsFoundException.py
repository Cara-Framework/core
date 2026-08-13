"""MultipleRecordsFoundException."""

from __future__ import annotations

from .ModelException import ModelException


class MultipleRecordsFoundException(ModelException):
    """Thrown when a "firstOrFail" style query unexpectedly returns many records."""

    pass

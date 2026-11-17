"""IdempotencyKeyConflictException."""

from __future__ import annotations

from .HttpException import HttpException


class IdempotencyKeyConflictException(HttpException):
    """409 — an ``Idempotency-Key`` whose first request is still running, or
    whose first run's outcome was not recorded.

    Never a re-run: the caller retries shortly (still running) or checks the
    result and sends a new key (unknown outcome). Envelope ``type`` is
    ``conflict``, the discriminator every 409 answers with.
    """

    status_code = 409
    error_type = "conflict"

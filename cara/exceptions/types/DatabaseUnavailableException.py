"""DatabaseUnavailableException."""

from __future__ import annotations

from .ORMException import ORMException


class DatabaseUnavailableException(ORMException):
    """Postgres is unreachable, connection was refused, or the pool was
    exhausted before a slot could be acquired.

    Distinct from ``QueryException`` (a bad query) — this is the
    "the database isn't answering" path. Maps to HTTP 503 so callers
    (and load balancers) can distinguish it from a 500 application
    fault and retry without alarming oncall.
    """

    is_http_exception = True
    status_code = 503

    def __init__(
        self,
        message: str = "Database temporarily unavailable",
        retry_after: int | None = None,
    ):
        super().__init__(message)
        if retry_after is not None:
            self.retry_after = retry_after

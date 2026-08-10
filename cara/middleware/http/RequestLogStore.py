"""Persistence port for the durable HTTP request log.

The framework owns the *policy* (what a request-log row contains, when it is
written, when old rows are swept). It deliberately owns no table, no model and
no SQL: storage belongs to the application. ``PersistRequestLog`` resolves this
port from the container, so an application binds its own implementation::

    from cara.middleware.http import RequestLogStore


    class AppServiceProvider(Provider):
        def register(self) -> None:
            self.application.bind(RequestLogStore, HttpRequestLogRepository())

Both methods run on a worker thread off the request path and are allowed to
raise — ``PersistRequestLog`` converts a failure into a single warning and,
for a missing destination relation, a process-wide kill switch.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class RequestLogStore(ABC):
    """Write side of the durable request log."""

    @abstractmethod
    def insert(self, payload: dict[str, Any]) -> None:
        """Persist one request-log row."""

    @abstractmethod
    def prune_old(self, retention_days: int) -> None:
        """Delete rows older than ``retention_days``.

        ``retention_days <= 0`` must be a no-op: it means 'retention
        disabled', not 'retain nothing'. Without that guard a misconfigured
        zero truncates the whole log.
        """

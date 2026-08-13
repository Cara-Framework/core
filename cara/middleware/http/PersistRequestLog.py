"""
Persist Request Log Middleware

Writes one durable row per served request through the application's
:class:`RequestLogStore` binding. Sits at the *end* of the middleware chain
so status and duration reflect the final response (including downstream
middleware mutations). Every write goes through a fire-and-forget thread —
a slow log INSERT must never extend the user-visible latency of a request.

Probe endpoints are skipped so the log does not fill with orchestrator noise.
The framework default is ``("/health", "/metrics")``; an application widens it
through ``logging.http_request_log_skip_prefixes``.

Row shape handed to the store::

    (
        created_at,
        method,
        path,
        route,
        status_code,
        duration_ms,
    )
    client_ip, user_agent, user_id, response_bytes

Anything beyond that is application schema: override :meth:`extra_columns`
to stamp additional columns onto the row.

Configuration (all read at call time, never snapshotted at import):

``logging.persist_http_requests``
    Kill switch. Default ``True``.
``logging.http_request_log_skip_prefixes``
    Path prefixes that are never logged. Default :attr:`SKIP_PREFIXES`.
``logging.http_request_log_retention_days``
    Sweep horizon. Default :attr:`DEFAULT_RETENTION_DAYS`. ``<= 0`` disables
    the sweep instead of truncating the log.
``logging.http_request_log_cleanup_every``
    Sampled sweep cadence. Default :attr:`DEFAULT_CLEANUP_EVERY`. ``<= 0``
    disables the sweep entirely so operators can flip it off without a deploy.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from cara.configuration import config
from cara.context import ExecutionContext
from cara.facades import Log
from cara.http import Request, Response
from cara.middleware.http.RequestLogStore import RequestLogStore
from cara.middleware.Middleware import Middleware
from cara.support import optional_user_id


class PersistRequestLog(Middleware):
    """Append a structured row per HTTP request to the request-log store."""

    #: Destination relation name. Only used to phrase the operator-facing
    #: warnings and to recognise a "relation does not exist" driver message;
    #: the actual write target is the bound :class:`RequestLogStore`.
    TABLE = "http_request_log"

    #: Log category for this middleware's own diagnostics. Deliberately not a
    #: category applications configure by default, so the missing-relation
    #: warning is visible out of the box.
    LOG_CATEGORY = "cara.http.request_log"

    SKIP_PREFIXES = ("/health", "/metrics")
    MAX_PATH_LEN = 500
    MAX_UA_LEN = 500

    DEFAULT_RETENTION_DAYS = 30
    DEFAULT_CLEANUP_EVERY = 1000

    # Process-wide kill switch flipped the first time the destination
    # relation is missing. Without this, every served request retries the
    # INSERT and spams the same error into the worker log. One warning is
    # enough — the operator runs the migration and the next process boot
    # rehydrates the switch.
    _table_missing = False

    # Sampled-cleanup counter — every Nth request the worker thread also
    # asks the store to prune rows past the retention horizon, so the log
    # doesn't grow without bound.
    _request_counter = 0

    # Strong references to the in-flight write/sweep tasks. ``asyncio``
    # registers a task with the running loop only WEAKLY, so a task whose
    # sole remaining referent was a local in ``_persist`` can be collected
    # before it ever runs — the row silently never lands, and it happens
    # under exactly the memory pressure that makes an access log most
    # useful. Shared across subclasses by design (mutated, never rebound):
    # one process, one set of pending writes.
    _inflight_tasks: set[asyncio.Task] = set()

    @classmethod
    def _spawn(cls, coro: Any) -> None:
        """Schedule ``coro`` and hold it until it finishes.

        The reference is dropped by the done-callback, so the set tracks
        exactly the writes still owed to the store.
        """
        task = asyncio.create_task(coro)
        cls._inflight_tasks.add(task)
        task.add_done_callback(cls._inflight_tasks.discard)
        task.add_done_callback(cls._warn_on_task_failure)

    @classmethod
    def _warn_on_task_failure(cls, task: asyncio.Task) -> None:
        """Retrieve a failed task's exception so the loop cannot eat it.

        ``_insert`` and ``_cleanup_old_rows`` handle their own errors, so
        anything surfacing here failed before them — the thread hop itself.
        Nobody awaits these tasks, so without this callback asyncio only
        emits an unretrieved-exception message at GC time, detached from
        the request that caused it.
        """
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            Log.warning(
                f"{cls.TABLE} background write could not be dispatched: {exc}",
                category=cls.LOG_CATEGORY,
            )

    def __init__(self, application: Any, **kwargs: Any) -> None:
        """Resolve the persistence port once and fail fast on bad wiring."""
        super().__init__(application, **kwargs)
        try:
            self._request_logs = application.make(RequestLogStore)
        except Exception as exc:
            raise RuntimeError(
                "PersistRequestLog is registered but no RequestLogStore is "
                "bound. Bind one in a service provider: "
                "application.bind(RequestLogStore, <your store>)."
            ) from exc

    def extra_columns(self, request: Request) -> dict[str, Any]:
        """Application-owned columns merged onto the row.

        The framework row carries only agnostic request facts. Anything that
        belongs to the application's own schema is stamped here.
        """
        return {}

    async def handle(self, request: Request, get_response: Callable) -> Response:
        path = request.path or ""
        if any(path.startswith(prefix) for prefix in self._skip_prefixes()):
            return await get_response(request)
        if not self._enabled():
            return await get_response(request)
        if PersistRequestLog._table_missing:
            return await get_response(request)

        start = time.time()
        response: Response | None = None
        # Default 500: when ``get_response`` raises, the HTTP conductor renders
        # the exception into a response OUTSIDE this pipeline (status =
        # ``exc.status_code``, 500 when it carries none), so the raise is the
        # only signal available here. Logging on that path is what lets a
        # request-log reader surface 404 / 422 / 5xx at all — otherwise only
        # responses that flow back normally would ever be recorded.
        status_code = 500
        try:
            response = await get_response(request)
            status_code = self._response_status(response)
            return response
        except Exception as exc:
            status_code = int(getattr(exc, "status_code", 500) or 500)
            raise
        finally:
            duration_ms = int((time.time() - start) * 1000)
            self._persist(request, path, status_code, duration_ms, response)

    def _persist(
        self,
        request: Request,
        path: str,
        status_code: int,
        duration_ms: int,
        response: Response | None,
    ) -> None:
        """Build the log row and fire the (thread-off) insert + sampled sweep.

        Never raises: a logging failure here runs inside the caller's
        ``finally`` and must not mask the response — or the exception — the
        request is actually returning.
        """
        try:
            payload: dict[str, Any] = {
                "created_at": datetime.now(UTC),
                "method": (request.method or "")[:10],
                "path": path[: self.MAX_PATH_LEN],
                "route": self._resolve_route(request),
                "status_code": status_code,
                "duration_ms": duration_ms,
                "client_ip": self._client_ip(request),
                "user_agent": self._user_agent(request),
                "user_id": optional_user_id(request),
                "response_bytes": self._response_bytes(response)
                if response is not None
                else None,
            }
            payload.update(self.extra_columns(request))
            self._spawn(ExecutionContext.run_in_thread(self._insert, payload))
            cadence = self._cleanup_cadence()
            if self._should_run_cleanup(cleanup_every=cadence):
                retention = self._retention_days()
                self._spawn(
                    ExecutionContext.run_in_thread(self._cleanup_old_rows, retention),
                )
        except Exception as exc:
            Log.debug(
                f"PersistRequestLog skipped row: {exc}",
                category=self.LOG_CATEGORY,
            )

    @staticmethod
    def _response_status(response: Response) -> int:
        try:
            value = response.get_status_code()
            return int(value) if value is not None else 0
        except Exception:
            return 0

    @staticmethod
    def _enabled() -> bool:
        return bool(config("logging.persist_http_requests", True))

    @classmethod
    def _skip_prefixes(cls) -> tuple[str, ...]:
        """Resolve the skip list at call time so config can widen it."""
        configured = config("logging.http_request_log_skip_prefixes", None)
        if configured is None:
            return cls.SKIP_PREFIXES
        if isinstance(configured, str):
            return (configured,)
        try:
            return tuple(str(prefix) for prefix in configured)
        except TypeError:
            return cls.SKIP_PREFIXES

    def _insert(self, payload: dict[str, Any]) -> None:
        try:
            self._request_logs.insert(payload)
        except Exception as exc:
            # Relation missing? Trip the kill switch and emit ONE warning
            # with the fix instruction so the worker log doesn't drown in a
            # per-request stream of the same error.
            if self._is_missing_table_error(exc):
                if not PersistRequestLog._table_missing:
                    PersistRequestLog._table_missing = True
                    Log.warning(
                        f"{self.TABLE} relation is missing — disabling "
                        "PersistRequestLog for this process. Run "
                        "`python craft migrate` to create it, then restart "
                        "the application.",
                        category=self.LOG_CATEGORY,
                    )
                return
            Log.warning(
                f"{self.TABLE} insert failed: {exc}",
                category=self.LOG_CATEGORY,
            )

    @classmethod
    def _is_missing_table_error(cls, exc: Exception) -> bool:
        """Identify only an absent relation, never a missing column.

        PostgreSQL exposes UndefinedTable as SQLSTATE 42P01. ORM layers may
        wrap it, so walk the causal chain and retain an exact-message fallback
        for drivers that do not expose SQLSTATE. The fallback is anchored:
        the marker must start the message (or follow a ``:`` prefix), so an
        unrelated error that merely quotes the phrase cannot trip the switch.
        """
        current: BaseException | None = exc
        seen: set[int] = set()
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            sqlstate = getattr(current, "sqlstate", None) or getattr(
                current, "pgcode", None
            )
            if str(sqlstate or "") == "42P01":
                return True
            current = current.__cause__ or current.__context__

        message = str(exc).casefold()
        table = cls.TABLE.casefold()
        for marker in (
            f'relation "{table}" does not exist',
            f'relation "public.{table}" does not exist',
        ):
            position = message.find(marker)
            if position < 0:
                continue
            prefix = message[:position].rstrip()
            if not prefix or prefix.endswith(":"):
                return True
        return False

    @classmethod
    def _should_run_cleanup(cls, cleanup_every: int) -> bool:
        """Sampled gate — fires once every ``cleanup_every`` requests.

        ``cleanup_every <= 0`` disables the sweep entirely so operators
        can flip it off without redeploying.
        """
        if cleanup_every <= 0:
            return False
        cls._request_counter += 1
        return cls._request_counter % cleanup_every == 0

    @classmethod
    def _retention_days(cls) -> int:
        try:
            return int(
                config(
                    "logging.http_request_log_retention_days",
                    cls.DEFAULT_RETENTION_DAYS,
                )
            )
        except TypeError, ValueError:
            return cls.DEFAULT_RETENTION_DAYS

    @classmethod
    def _cleanup_cadence(cls) -> int:
        try:
            return int(
                config(
                    "logging.http_request_log_cleanup_every",
                    cls.DEFAULT_CLEANUP_EVERY,
                )
            )
        except TypeError, ValueError:
            return cls.DEFAULT_CLEANUP_EVERY

    def _cleanup_old_rows(self, retention_days: int) -> None:
        """Ask the store to drop rows older than ``retention_days``.

        ``retention_days <= 0`` is a deliberate no-op on the store side: it
        means 'retention disabled', not 'retain nothing'. This wrapper only
        preserves the fire-and-forget warning-logging contract on the hot
        request path.
        """
        try:
            self._request_logs.prune_old(retention_days)
        except Exception as exc:
            Log.warning(
                f"{self.TABLE} cleanup failed: {exc}",
                category=self.LOG_CATEGORY,
            )

    @staticmethod
    def _resolve_route(request: Request) -> str | None:
        route = getattr(request, "route", None)
        if route is None:
            return None
        name = route.get_name() if hasattr(route, "get_name") else None
        if name:
            return str(name)[:200]
        for attr in ("uri", "path"):
            value = getattr(route, attr, None)
            if value and not callable(value):
                return str(value)[:200]
        return None

    @classmethod
    def _client_ip(cls, request: Request) -> str | None:
        # ``Request.ip`` is a method, not a property — reading it without
        # calling captures the bound method object (always truthy), so
        # ``str(...)`` writes "<bound method Request.ip of ...>" into the
        # client_ip column and the trusted-proxy walk never runs. Invoke the
        # accessor explicitly so TRUSTED_PROXIES + XFF resolution happens.
        for attr in ("ip", "client_ip", "remote_addr"):
            value = getattr(request, attr, None)
            if value is None:
                continue
            try:
                resolved = value() if callable(value) else value
            except Exception as exc:
                Log.warning(
                    f"{cls.TABLE} client IP accessor {attr!r} failed: {exc}",
                    category=cls.LOG_CATEGORY,
                )
                continue
            if resolved:
                return str(resolved)[:45]
        return None

    def _user_agent(self, request: Request) -> str | None:
        try:
            headers = getattr(request, "headers", {}) or {}
            agent = headers.get("User-Agent") or headers.get("user-agent")
            return str(agent)[: self.MAX_UA_LEN] if agent else None
        except Exception as exc:
            Log.warning(
                f"{self.TABLE} User-Agent parse failed: {exc}",
                category=self.LOG_CATEGORY,
            )
            return None

    @classmethod
    def _response_bytes(cls, response: Response) -> int | None:
        # Cara's Response carries its payload on ``content`` (BaseResponse),
        # not ``body`` — reading ``body`` returns None for every request and
        # leaves the response_bytes column permanently NULL.
        body = getattr(response, "content", None)
        if body is None:
            return None
        try:
            return len(body) if isinstance(body, (bytes, str)) else None
        except Exception as exc:
            Log.warning(
                f"{cls.TABLE} response body length failed: {exc}",
                category=cls.LOG_CATEGORY,
            )
            return None

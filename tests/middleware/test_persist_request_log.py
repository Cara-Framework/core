"""Behaviour pins for the durable request-log middleware.

The middleware owns policy only — skip rules, the fire-and-forget write, the
missing-relation kill switch, the sampled retention sweep and the row shape.
Storage lives behind the ``RequestLogStore`` port, so every test here drives a
stub store instead of a database.
"""

from __future__ import annotations

import asyncio
import gc
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

from cara.middleware.http import PersistRequestLog, RequestLogStore
from cara.middleware.http import PersistRequestLog as _module_probe

middleware_module = __import__(
    "cara.middleware.http.PersistRequestLog", fromlist=["PersistRequestLog"]
)

assert _module_probe is middleware_module.PersistRequestLog


# ── Doubles ──────────────────────────────────────────────────────────


class _Store(RequestLogStore):
    def __init__(self) -> None:
        self.rows: list[dict] = []
        self.pruned: list[int] = []

    def insert(self, payload: dict) -> None:
        self.rows.append(payload)

    def prune_old(self, retention_days: int) -> None:
        self.pruned.append(retention_days)


def _application(store: RequestLogStore | None = None) -> MagicMock:
    app = MagicMock()
    app.make.return_value = store if store is not None else _Store()
    return app


def _subject(insert=None, prune=None) -> PersistRequestLog:
    """Bare instance with only the store wired — no container involved."""
    middleware = object.__new__(PersistRequestLog)
    middleware._request_logs = SimpleNamespace(
        insert=insert or (lambda _payload: None),
        prune_old=prune or (lambda _days: None),
    )
    return middleware


def _request(path: str = "/api/products", method: str = "GET") -> SimpleNamespace:
    return SimpleNamespace(path=path, method=method, headers={"User-Agent": "probe/1"})


def _stub_config(monkeypatch, values: dict) -> None:
    monkeypatch.setattr(
        middleware_module,
        "config",
        lambda key, default=None: values.get(key, default),
    )


class _SettledTask:
    """Stand-in for a task that finished the moment it was scheduled.

    ``_spawn`` registers the strong reference *before* attaching callbacks,
    so running each callback on registration exercises the real bookkeeping
    (add, then discard) without needing a live event loop.
    """

    def add_done_callback(self, callback) -> None:
        callback(self)

    @staticmethod
    def cancelled() -> bool:
        return False

    @staticmethod
    def exception():
        return None


def _capture_dispatch(monkeypatch) -> list[tuple]:
    """Intercept the fire-and-forget hop and record (callable, *args)."""
    calls: list[tuple] = []

    class _Context:
        @staticmethod
        def run_in_thread(fn, *args):
            calls.append((fn, *args))
            return None

    monkeypatch.setattr(middleware_module, "ExecutionContext", _Context)
    monkeypatch.setattr(
        middleware_module,
        "asyncio",
        SimpleNamespace(create_task=lambda _coro: _SettledTask()),
    )
    return calls


# ── Container wiring ─────────────────────────────────────────────────


def test_store_is_resolved_from_the_container_at_construction():
    store = _Store()
    app = _application(store)
    middleware = PersistRequestLog(app)

    assert middleware._request_logs is store
    app.make.assert_called_once_with(RequestLogStore)


def test_unbound_store_fails_loudly_at_construction():
    """A product that registers the middleware without binding the port must
    die at boot, not silently drop every request row."""
    app = MagicMock()
    app.make.side_effect = RuntimeError("nothing bound")

    with pytest.raises(RuntimeError) as excinfo:
        PersistRequestLog(app)

    assert "RequestLogStore" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, RuntimeError)


# ── Skip rules and kill switch ───────────────────────────────────────


def test_framework_skip_prefixes_cover_probe_endpoints(monkeypatch):
    _stub_config(monkeypatch, {})

    assert PersistRequestLog._skip_prefixes() == ("/health", "/metrics")


def test_config_replaces_the_skip_prefix_list(monkeypatch):
    _stub_config(
        monkeypatch,
        {"logging.http_request_log_skip_prefixes": ["/health", "/admin/access-log"]},
    )

    assert PersistRequestLog._skip_prefixes() == ("/health", "/admin/access-log")


def test_non_iterable_skip_prefix_config_falls_back_to_the_default(monkeypatch):
    _stub_config(monkeypatch, {"logging.http_request_log_skip_prefixes": 7})

    assert PersistRequestLog._skip_prefixes() == PersistRequestLog.SKIP_PREFIXES


@pytest.mark.asyncio
async def test_skipped_path_writes_no_row(monkeypatch):
    _stub_config(monkeypatch, {})
    calls = _capture_dispatch(monkeypatch)
    subject = _subject()

    response = await subject.handle(_request("/health/live"), lambda _req: _respond(200))

    assert response.get_status_code() == 200
    assert calls == []


@pytest.mark.asyncio
async def test_kill_switch_config_disables_the_write(monkeypatch):
    _stub_config(monkeypatch, {"logging.persist_http_requests": False})
    calls = _capture_dispatch(monkeypatch)
    subject = _subject()

    await subject.handle(_request(), lambda _req: _respond(200))

    assert calls == []


# ── Row shape ────────────────────────────────────────────────────────


async def _respond(status: int, content: bytes = b"hello"):
    return SimpleNamespace(get_status_code=lambda: status, content=content)


@pytest.mark.asyncio
async def test_successful_request_persists_the_canonical_row(monkeypatch):
    _stub_config(monkeypatch, {})
    calls = _capture_dispatch(monkeypatch)
    subject = _subject()

    await subject.handle(_request("/api/products"), lambda _req: _respond(201))

    (_fn, payload) = calls[0]
    assert payload["method"] == "GET"
    assert payload["path"] == "/api/products"
    assert payload["status_code"] == 201
    assert payload["user_agent"] == "probe/1"
    assert payload["user_id"] is None
    assert payload["response_bytes"] == 5


@pytest.mark.asyncio
async def test_response_bytes_read_from_content_not_body(monkeypatch):
    """``BaseResponse`` stores the payload on ``content``; reading ``body``
    leaves the column permanently NULL."""
    _stub_config(monkeypatch, {})
    calls = _capture_dispatch(monkeypatch)
    subject = _subject()

    async def _weird(_req):
        return SimpleNamespace(
            get_status_code=lambda: 200, content=b"1234567890", body=None
        )

    await subject.handle(_request(), _weird)

    assert calls[0][1]["response_bytes"] == 10


@pytest.mark.asyncio
async def test_extra_columns_hook_is_merged_onto_the_row(monkeypatch):
    _stub_config(monkeypatch, {})
    calls = _capture_dispatch(monkeypatch)

    class _Stamped(PersistRequestLog):
        def extra_columns(self, request):
            return {"workspace_id": 42}

    subject = object.__new__(_Stamped)
    subject._request_logs = SimpleNamespace(
        insert=lambda _p: None, prune_old=lambda _d: None
    )

    await subject.handle(_request(), lambda _req: _respond(200))

    assert calls[0][1]["workspace_id"] == 42


@pytest.mark.asyncio
async def test_exception_path_records_the_exception_status_and_reraises(monkeypatch):
    _stub_config(monkeypatch, {})
    calls = _capture_dispatch(monkeypatch)
    subject = _subject()

    class _Boom(Exception):
        status_code = 422

    async def _raise(_req):
        raise _Boom("nope")

    with pytest.raises(_Boom):
        await subject.handle(_request(), _raise)

    payload = calls[0][1]
    assert payload["status_code"] == 422
    assert payload["response_bytes"] is None


def test_client_ip_invokes_the_accessor_instead_of_stringifying_it(monkeypatch):
    request = SimpleNamespace(ip=lambda: "203.0.113.9")

    assert PersistRequestLog._client_ip(request) == "203.0.113.9"


def test_client_ip_survives_a_raising_accessor(monkeypatch):
    monkeypatch.setattr(middleware_module, "Log", MagicMock())

    def _explode():
        raise RuntimeError("no peer")

    assert PersistRequestLog._client_ip(SimpleNamespace(ip=_explode)) is None


# ── Missing-relation kill switch ─────────────────────────────────────


def test_missing_relation_trips_the_kill_switch_once(monkeypatch):
    PersistRequestLog._table_missing = False
    warning = Mock()
    monkeypatch.setattr(
        middleware_module, "Log", SimpleNamespace(warning=warning, debug=Mock())
    )
    subject = _subject(
        insert=Mock(
            side_effect=RuntimeError('relation "http_request_log" does not exist')
        )
    )

    subject._insert({})

    assert PersistRequestLog._table_missing is True
    assert "relation is missing" in warning.call_args.args[0]
    PersistRequestLog._table_missing = False


def test_missing_column_does_not_trip_the_kill_switch(monkeypatch):
    """Schema drift must stay loud — only an absent relation disables logging."""
    PersistRequestLog._table_missing = False
    warning = Mock()
    monkeypatch.setattr(
        middleware_module, "Log", SimpleNamespace(warning=warning, debug=Mock())
    )
    subject = _subject(
        insert=Mock(
            side_effect=RuntimeError(
                'column "workspace_id" of relation "http_request_log" does not exist'
            )
        )
    )

    subject._insert({})

    assert PersistRequestLog._table_missing is False
    assert "insert failed" in warning.call_args.args[0]


def test_sqlstate_42p01_trips_the_switch_through_the_cause_chain(monkeypatch):
    PersistRequestLog._table_missing = False
    monkeypatch.setattr(
        middleware_module, "Log", SimpleNamespace(warning=Mock(), debug=Mock())
    )
    driver_error = RuntimeError("undefined table")
    driver_error.sqlstate = "42P01"
    wrapper = RuntimeError("ORM wrapper")
    wrapper.__cause__ = driver_error

    subject = _subject(insert=Mock(side_effect=wrapper))
    subject._insert({})

    assert PersistRequestLog._table_missing is True
    PersistRequestLog._table_missing = False


# ── Retention sweep ──────────────────────────────────────────────────


def test_retention_defaults_to_thirty_days(monkeypatch):
    _stub_config(monkeypatch, {})

    assert PersistRequestLog._retention_days() == 30


def test_retention_honours_config(monkeypatch):
    _stub_config(monkeypatch, {"logging.http_request_log_retention_days": 400})

    assert PersistRequestLog._retention_days() == 400


def test_cleanup_cadence_defaults_to_one_thousand(monkeypatch):
    _stub_config(monkeypatch, {})

    assert PersistRequestLog._cleanup_cadence() == 1000


def test_cleanup_fires_every_nth_request():
    PersistRequestLog._request_counter = 0

    fired = [PersistRequestLog._should_run_cleanup(cleanup_every=3) for _ in range(6)]

    assert fired == [False, False, True, False, False, True]


def test_cleanup_disabled_when_cadence_non_positive():
    PersistRequestLog._request_counter = 0

    assert PersistRequestLog._should_run_cleanup(cleanup_every=0) is False
    assert PersistRequestLog._should_run_cleanup(cleanup_every=-1) is False


def test_cleanup_delegates_the_retention_window_to_the_store(monkeypatch):
    monkeypatch.setattr(
        middleware_module, "Log", SimpleNamespace(warning=Mock(), debug=Mock())
    )
    pruned: list[int] = []
    subject = _subject(prune=pruned.append)

    subject._cleanup_old_rows(90)

    assert pruned == [90]


def test_cleanup_failure_is_warned_not_raised(monkeypatch):
    warning = Mock()
    monkeypatch.setattr(
        middleware_module, "Log", SimpleNamespace(warning=warning, debug=Mock())
    )
    subject = _subject(prune=Mock(side_effect=RuntimeError("locked")))

    subject._cleanup_old_rows(30)

    assert "cleanup failed" in warning.call_args.args[0]


# ── Fire-and-forget task ownership ───────────────────────────────────


def _thread_hop(monkeypatch, body):
    """Replace the thread hop with ``body``, keeping real ``asyncio``."""

    class _Context:
        @staticmethod
        async def run_in_thread(fn, *args):
            return await body(fn, *args)

    monkeypatch.setattr(middleware_module, "ExecutionContext", _Context)


@pytest.mark.asyncio
async def test_pending_write_is_strongly_referenced_until_it_completes(monkeypatch):
    """``asyncio`` tracks tasks weakly.

    A write whose only referent was a local in ``_persist`` can be collected
    before it runs, and the row is lost with no error anywhere — so the
    middleware holds the task itself and releases it on completion.
    """
    _stub_config(monkeypatch, {})
    PersistRequestLog._table_missing = False
    PersistRequestLog._request_counter = 0
    PersistRequestLog._inflight_tasks.clear()

    gate = asyncio.Event()

    async def _blocked(fn, *args):
        await gate.wait()
        return fn(*args)

    _thread_hop(monkeypatch, _blocked)

    rows: list[dict] = []
    subject = _subject(insert=rows.append)

    await subject.handle(_request(), lambda _req: _respond(200))

    assert len(PersistRequestLog._inflight_tasks) == 1
    task = next(iter(PersistRequestLog._inflight_tasks))

    # The handler has returned; the local that held the task is gone. Only
    # the middleware's own reference stands between this write and the GC.
    gc.collect()
    assert not task.done()

    gate.set()
    await task

    assert rows and rows[0]["path"] == "/api/products"
    assert PersistRequestLog._inflight_tasks == set()


@pytest.mark.asyncio
async def test_undispatchable_write_is_reported_not_swallowed(monkeypatch):
    """Nobody awaits these tasks, so a failure before ``_insert``'s own
    handler would otherwise surface only as asyncio GC noise."""
    warning = Mock()
    monkeypatch.setattr(
        middleware_module, "Log", SimpleNamespace(warning=warning, debug=Mock())
    )
    _stub_config(monkeypatch, {})
    PersistRequestLog._table_missing = False
    PersistRequestLog._request_counter = 0
    PersistRequestLog._inflight_tasks.clear()

    async def _explode(_fn, *_args):
        raise RuntimeError("thread pool exhausted")

    _thread_hop(monkeypatch, _explode)

    await _subject().handle(_request(), lambda _req: _respond(200))
    for _ in range(4):
        await asyncio.sleep(0)

    assert warning.called
    assert "could not be dispatched" in warning.call_args.args[0]
    assert warning.call_args.kwargs["category"] == PersistRequestLog.LOG_CATEGORY
    assert PersistRequestLog._inflight_tasks == set()

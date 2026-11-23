"""``PersistRequestLog`` buffers rows and writes them in batches.

Live defect: every served request paid one thread hop and one INSERT for its
own log row — on a busy worker the bookkeeping cost more than the request it
recorded. Rows are now buffered and leave as one batch when the buffer holds
``BATCH_SIZE`` rows or ``FLUSH_INTERVAL_SECONDS`` after its first row; the
shutdown callback writes whatever is left, so a deploy loses nothing; a store
that implements only ``insert`` still receives every row.
"""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from cara.middleware.http import PersistRequestLog, RequestLogStore
from cara.routing import Router

middleware_module = importlib.import_module("cara.middleware.http.PersistRequestLog")
redaction_module = importlib.import_module("cara.middleware.http.PathRedaction")


class _InsertOnlyStore(RequestLogStore):
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def insert(self, payload: dict) -> None:
        self.rows.append(payload)

    def prune_old(self, retention_days: int) -> None:
        return None


class _BatchStore(_InsertOnlyStore):
    def __init__(self) -> None:
        super().__init__()
        self.batches: list[list[dict]] = []

    def insert_many(self, payloads: list[dict]) -> None:
        self.batches.append(list(payloads))


class _Application:
    """Container stand-in: resolves the store and the router."""

    def __init__(self, store: RequestLogStore) -> None:
        self._store = store
        self._router = Router(None)

    def make(self, key):
        return self._store if key is RequestLogStore else self._router


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    for module in (middleware_module, redaction_module):
        monkeypatch.setattr(module, "config", lambda _key, default=None: default)
    monkeypatch.setattr(PersistRequestLog, "_table_missing", False)
    monkeypatch.setattr(PersistRequestLog, "_request_counter", 0)

    class _DirectHop:
        @staticmethod
        async def run_in_thread(fn, *args):
            return fn(*args)

    monkeypatch.setattr(middleware_module, "ExecutionContext", _DirectHop)

    def _reset() -> None:
        timer = PersistRequestLog._buffer_state.get("timer")
        if timer is not None:
            timer.cancel()
        PersistRequestLog._buffer.clear()
        PersistRequestLog._buffer_state.update(writer=None, timer=None, loop=None)
        PersistRequestLog._inflight_tasks.clear()

    _reset()
    yield
    _reset()


async def _ok(_request):
    return SimpleNamespace(get_status_code=lambda: 200, content=b"ok")


def _request(index: int) -> SimpleNamespace:
    return SimpleNamespace(path=f"/api/items/{index}", method="GET", headers={})


async def _serve(application: _Application, count: int, first: int = 0) -> None:
    for index in range(first, first + count):
        await PersistRequestLog(application).handle(_request(index), _ok)


async def _drain() -> None:
    pending = list(PersistRequestLog._inflight_tasks)
    for task in pending:
        await task


@pytest.mark.asyncio
async def test_a_full_buffer_leaves_as_one_batch(monkeypatch):
    monkeypatch.setattr(PersistRequestLog, "BATCH_SIZE", 3)
    store = _BatchStore()
    application = _Application(store)

    await _serve(application, 2)
    await _drain()
    assert store.batches == []

    await _serve(application, 1, first=2)
    await _drain()

    assert [[row["path"] for row in batch] for batch in store.batches] == [
        ["/api/items/0", "/api/items/1", "/api/items/2"]
    ]
    assert PersistRequestLog._buffer == []


@pytest.mark.asyncio
async def test_a_partial_buffer_leaves_when_the_interval_timer_fires(monkeypatch):
    """The first buffered row arms one timer for ``FLUSH_INTERVAL_SECONDS``;
    firing it writes the partial batch (the timer is driven by hand)."""
    armed: list[tuple] = []

    class _Loop:
        def call_later(self, delay, callback):
            armed.append((delay, callback))
            return SimpleNamespace(cancel=lambda: None)

    loop = _Loop()
    real_asyncio = middleware_module.asyncio
    monkeypatch.setattr(
        middleware_module,
        "asyncio",
        SimpleNamespace(
            get_running_loop=lambda: loop,
            create_task=real_asyncio.create_task,
            gather=real_asyncio.gather,
        ),
    )
    store = _BatchStore()

    await _serve(_Application(store), 2)
    await _drain()

    assert len(armed) == 1
    assert armed[0][0] == PersistRequestLog.FLUSH_INTERVAL_SECONDS
    assert store.batches == []

    armed[0][1]()
    await _drain()

    assert [len(batch) for batch in store.batches] == [2]


@pytest.mark.asyncio
async def test_shutdown_writes_every_buffered_row():
    store = _BatchStore()
    application = _Application(store)

    await _serve(application, 5)

    assert application._shutdown_callbacks == [PersistRequestLog.flush_pending]
    assert store.batches == []

    for callback in application._shutdown_callbacks:
        await callback()

    assert [row["path"] for batch in store.batches for row in batch] == [
        f"/api/items/{index}" for index in range(5)
    ]
    assert PersistRequestLog._inflight_tasks == set()


@pytest.mark.asyncio
async def test_a_store_without_insert_many_still_receives_every_row():
    store = _InsertOnlyStore()
    application = _Application(store)

    await _serve(application, 4)
    await PersistRequestLog.flush_pending()

    assert [row["path"] for row in store.rows] == [
        f"/api/items/{index}" for index in range(4)
    ]

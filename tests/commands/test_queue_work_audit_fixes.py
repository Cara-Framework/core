"""Queue worker defects the 2026-11-23 audit reproduced.

Each of these told the broker, the ledger or the operator something the job
had not done:

  * a broker-closed CHANNEL under a running job (pika >= 1.5 raises it from
    ``process_data_events``) set the process-wide shutdown flag, so a job that
    outlived the broker's delivery-ack timeout was drained, cancelled and
    started over — forever. Only a lost CONNECTION ends the worker.
  * a broker ``Basic.Cancel`` left a slot servicing a channel nothing would
    deliver on, still counted as ready.
  * a ``CancelledError`` the job let escape by itself was read as a worker
    shutdown: handed back unpenalised, and it ended the slot thread for good.
  * the ``retry_after`` floor was applied BEFORE the +/-25 % jitter, so about
    half of the retries came back earlier than the failure asked.
  * a ``TimeoutError`` raised by the job's own code was relabelled as the
    execution budget's and rebuilt without its ``retry_after``.

The broker, ledger and tracker are fakes; no service is contacted.
"""

from __future__ import annotations

import asyncio
import builtins
import importlib
import threading
import time
from types import SimpleNamespace

import pendulum
import pytest

from cara.commands.core.ActiveJobCancellationRegistry import (
    ActiveJobCancellationRegistry,
)
from cara.commands.core.JobProcessor import JobProcessor
from cara.commands.core.QueueWorkCommand import QueueWorkCommand
from cara.exceptions import QueueException
from cara.queues.delivery import DeliveryClaim

# ── the consumer stream ─────────────────────────────────────────────────────


class _Channel:
    is_open = True

    def __init__(self) -> None:
        self.callbacks: list = []
        self.cancel_callbacks: list = []

    def basic_qos(self, **_kwargs) -> None:
        return None

    def basic_consume(self, *, queue, on_message_callback, auto_ack) -> str:
        self.callbacks.append(on_message_callback)
        return "ctag"

    def basic_cancel(self, _tag) -> None:
        return None

    def add_on_cancel_callback(self, callback) -> None:
        self.cancel_callbacks.append(callback)

    def close(self) -> None:
        self.is_open = False


class _ChannelClosedUnderAJob:
    """Delivers one job, then raises once — the way pika reports the broker
    closing the channel — and is serviced normally afterwards."""

    def __init__(self, channel: _Channel, *, connection_lost: bool) -> None:
        self.channel = channel
        self.connection_lost = connection_lost
        self.is_closed = False
        self.calls = 0
        self.serviced_after_error = 0

    def process_data_events(self, *, time_limit) -> None:
        self.calls += 1
        if self.calls == 1:
            self.channel.callbacks[0](
                self.channel, SimpleNamespace(delivery_tag=1), SimpleNamespace(), b"x"
            )
            return
        if self.calls == 2:
            self.is_closed = self.connection_lost
            raise RuntimeError("ChannelClosedByBroker: (406, 'PRECONDITION_FAILED')")
        self.serviced_after_error += 1
        time.sleep(0.005)

    def add_callback_threadsafe(self, callback) -> None:
        callback()


class _SlowProcessor:
    """One job that is still running when the channel goes away."""

    cancellation_registry = None

    def __init__(self, command) -> None:
        self.command = command
        self.shutdown_seen_at_finish: bool | None = None

    def process_message(self, _channel, _method, _body, **_kwargs) -> str:
        time.sleep(0.2)
        self.shutdown_seen_at_finish = self.command.shutdown_requested
        return "success"


def _stream_command() -> QueueWorkCommand:
    command = QueueWorkCommand.__new__(QueueWorkCommand)
    command.shutdown_requested = False
    command._record_worker_outcome = lambda _outcome, _config: None
    command._verify_consumer_queue = lambda _manager, _queue: None
    command._should_stop = lambda _config: False
    return command


def _consume(command, connection, channel, processor) -> None:
    command._consume_queue_stream(
        queue_names=["sync"],
        connection_manager=SimpleNamespace(
            connection=connection, create_channel=lambda: channel
        ),
        job_processor=processor,
        config={"timeout": 1},
    )


def test_a_closed_channel_does_not_shut_the_worker_down() -> None:
    command = _stream_command()
    channel = _Channel()
    connection = _ChannelClosedUnderAJob(channel, connection_lost=False)
    processor = _SlowProcessor(command)

    # The slot still leaves (its loop reconnects and reopens the channel)...
    with pytest.raises(RuntimeError, match="ChannelClosedByBroker"):
        _consume(command, connection, channel, processor)

    # ...but only after the job ran to its end, with the worker still up and
    # the live connection serviced meanwhile (heartbeats, the job's ACK).
    assert processor.shutdown_seen_at_finish is False
    assert command.shutdown_requested is False
    assert connection.serviced_after_error > 0


def test_a_lost_connection_still_shuts_the_worker_down() -> None:
    command = _stream_command()
    channel = _Channel()
    connection = _ChannelClosedUnderAJob(channel, connection_lost=True)
    processor = _SlowProcessor(command)

    with pytest.raises(RuntimeError, match="ChannelClosedByBroker"):
        _consume(command, connection, channel, processor)

    assert processor.shutdown_seen_at_finish is True
    assert command.shutdown_requested is True
    # A closed connection is not serviced again.
    assert connection.serviced_after_error == 0


class _BrokerCancels:
    """An idle slot whose subscription the broker cancels (queue deleted)."""

    is_closed = False

    def __init__(self, channel: _Channel) -> None:
        self.channel = channel
        self.calls = 0

    def process_data_events(self, *, time_limit) -> None:
        self.calls += 1
        if self.calls == 1:
            self.channel.cancel_callbacks[0](SimpleNamespace())
        if self.calls > 50:
            raise AssertionError("zombie slot: still servicing a cancelled consumer")

    def add_callback_threadsafe(self, callback) -> None:
        callback()


def test_a_broker_cancelled_subscription_leaves_so_the_slot_resubscribes() -> None:
    command = _stream_command()
    channel = _Channel()
    connection = _BrokerCancels(channel)

    with pytest.raises(ConnectionError, match="cancelled the consumer"):
        _consume(
            command, connection, channel, SimpleNamespace(cancellation_registry=None)
        )

    assert connection.calls == 1
    # The slot is no longer counted as consuming.
    assert command._active_consumer_slots == 0
    assert command.shutdown_requested is False


def test_a_cancelled_error_does_not_end_the_slot_thread(monkeypatch) -> None:
    """``CancelledError`` is a BaseException: ``except Exception`` let it end
    the consumer thread, and nothing respawned the slot."""
    runtime = importlib.import_module("cara.commands.core._QueueWorkRuntime")
    monkeypatch.setattr(
        runtime,
        "AMQPConnectionManager",
        lambda _config, _driver: SimpleNamespace(close=lambda: None),
    )
    monkeypatch.setattr(
        runtime,
        "Queue",
        SimpleNamespace(
            driver=lambda *_a, **_k: SimpleNamespace(
                _connection_parameters=lambda _options: None,
                verify_runtime_health=lambda *_a, **_k: None,
            )
        ),
    )
    command = QueueWorkCommand.__new__(QueueWorkCommand)
    command.console = SimpleNamespace(print=lambda *_a, **_k: None)
    command.shutdown_requested = False
    command.jobs_processed = command.jobs_failed = 0
    command._concurrency = 1
    command._shutdown_signal = None
    command._shutdown_drain_seconds = lambda: 1.0
    command._shutdown_cancel_seconds = lambda: 1.0
    command._shutdown_worker_resources = lambda: True
    command._check_memory_usage = lambda: True
    command._should_stop = lambda _config: False

    streams: list[str] = []

    def _stream(**_kwargs) -> None:
        streams.append(threading.current_thread().name)
        if len(streams) == 1:
            raise asyncio.CancelledError()
        command.shutdown_requested = True

    command._consume_queue_stream = _stream
    command._run_worker({"queue_names": ["sync"], "driver_name": "amqp", "timeout": 1})

    # The same slot thread came back for a second subscription.
    assert streams == ["queue-worker-1", "queue-worker-1"]


# ── process_message ─────────────────────────────────────────────────────────


class _SelfCancellingJob:
    """Awaits a future a library cancelled; the worker is not shutting down."""

    queue = "sync"

    async def handle(self) -> None:
        future = asyncio.get_running_loop().create_future()
        future.cancel()
        await future


class _UpstreamBusy(TimeoutError):
    retry_after = 120


class _UpstreamBusyJob:
    queue = "sync"

    async def handle(self) -> None:
        raise _UpstreamBusy("upstream said: come back in 120s")


def _process(monkeypatch, job, registry):
    """Real ``process_message`` over a fake ledger; returns what was routed."""
    execution = importlib.import_module("cara.commands.core._JobExecution")
    payload = {
        "job_id": "55555555-5555-4555-8555-555555555555",
        "db_job_id": 91,
        "queue": "sync",
        "timeout_seconds": 30,
        "attempts": 0,
        "_tenant": 5,
        "_tenant_mode": "tenant",
    }
    events: list[str] = []
    store = SimpleNamespace(
        execution_heartbeat_seconds=30,
        claim_execution=lambda *, body, payload: DeliveryClaim(
            "claimed", "lease", lease_deadline=pendulum.now("UTC").add(seconds=600)
        ),
        renew_execution=lambda *_a, **_k: True,
        execution_timeout_for=lambda _cls: 30,
        abandon_execution=lambda _job_id, _token: events.append("abandon"),
    )
    tracker = SimpleNamespace(
        is_job_completed=lambda _id: False,
        update_job_status_strict=lambda _id, status: events.append(f"tracker:{status}"),
    )
    quiet = SimpleNamespace(
        debug=lambda *_a, **_k: None,
        info=lambda *_a, **_k: None,
        warning=lambda *_a, **_k: None,
        error=lambda *_a, **_k: None,
    )
    application = SimpleNamespace(
        has=lambda key: key == "JobTracker",
        call=lambda method, *args: method(*args),
        make=lambda key: {
            "queue": SimpleNamespace(
                driver=lambda *_a, **_k: SimpleNamespace(delivery_store=store)
            ),
            "JobTracker": tracker,
            "logger": quiet,
        }[key],
    )
    monkeypatch.setattr(builtins, "app", lambda: application, raising=False)
    monkeypatch.setattr(
        execution.SignedJsonJobSerializer,
        "inspect_envelope",
        classmethod(lambda cls, *_a, **_k: {"payload": payload}),
    )
    monkeypatch.setattr(
        execution.SignedJsonJobSerializer,
        "deserialize_verified",
        classmethod(lambda cls, verified, **_k: {**verified, "obj": job, "args": ()}),
    )
    monkeypatch.setattr(execution.observability, "capture_exception", lambda _exc: None)
    routed: dict = {}

    def _router(**kwargs) -> str:
        routed["exc"] = kwargs["exc"]
        return "retry_scheduled"

    monkeypatch.setattr(JobProcessor, "_route_failed_message", staticmethod(_router))

    def _run():
        return JobProcessor.process_message(
            SimpleNamespace(basic_ack=lambda **_k: None, basic_nack=lambda **_k: None),
            SimpleNamespace(delivery_tag=1),
            b"x",
            queue_name="sync",
            cancellation_registry=registry,
        )

    return _run, routed, events


def test_a_stray_cancelled_error_is_a_failure_not_a_shutdown(monkeypatch) -> None:
    registry = ActiveJobCancellationRegistry()
    run, routed, events = _process(monkeypatch, _SelfCancellingJob(), registry)

    assert run() == "failure"

    # It went through the failure router (an attempt is spent, the DLQ is
    # reachable) instead of being handed back unpenalised.
    assert isinstance(routed["exc"], QueueException)
    assert isinstance(routed["exc"].__cause__, asyncio.CancelledError)
    assert "abandon" not in events


def test_a_worker_shutdown_cancellation_is_still_handed_back(monkeypatch) -> None:
    registry = ActiveJobCancellationRegistry()
    assert registry.cancellation_requested is False
    registry.cancel_all()
    assert registry.cancellation_requested is True
    run, routed, events = _process(monkeypatch, _SelfCancellingJob(), registry)

    with pytest.raises(asyncio.CancelledError):
        run()

    assert routed == {}
    assert events == ["abandon", "tracker:pending"]


def test_the_jobs_own_timeout_reaches_the_router_as_itself(monkeypatch) -> None:
    run, routed, _events = _process(monkeypatch, _UpstreamBusyJob(), None)

    assert run() == "failure"

    assert type(routed["exc"]) is _UpstreamBusy
    assert routed["exc"].retry_after == 120
    assert "exceeded timeout" not in str(routed["exc"])


# ── the execution budget ────────────────────────────────────────────────────


def test_only_the_budget_expiring_is_called_a_job_timeout() -> None:
    async def _own_deadline() -> None:
        async with asyncio.timeout(0.01):
            await asyncio.sleep(5)

    with pytest.raises(TimeoutError) as own:
        JobProcessor._execute_async_job_with_timeout(_own_deadline, (), 30)
    assert "exceeded timeout" not in str(own.value)

    async def _hangs() -> None:
        await asyncio.sleep(5)

    with pytest.raises(TimeoutError, match="Job exceeded timeout of 0.05s"):
        JobProcessor._execute_async_job_with_timeout(_hangs, (), 0.05)


# ── retry_after is a floor after the jitter too ─────────────────────────────


class _Named(RuntimeError):
    def __init__(self, retry_after) -> None:
        super().__init__("upstream named its wait")
        self.retry_after = retry_after


def _requeue_delay(monkeypatch, exc, jitter) -> int:
    delays: list[int] = []
    queue_service = SimpleNamespace(
        driver=lambda: SimpleNamespace(_apply_retry_jitter=jitter),
        later=lambda delay, _instance, **_options: delays.append(delay),
    )
    quiet = SimpleNamespace(
        debug=lambda *_a, **_k: None,
        info=lambda *_a, **_k: None,
        warning=lambda *_a, **_k: None,
        error=lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        builtins,
        "app",
        lambda: SimpleNamespace(
            make=lambda key: {"queue": queue_service, "logger": quiet}[key]
        ),
        raising=False,
    )
    JobProcessor._requeue_with_delay(
        SimpleNamespace(basic_ack=lambda *, delivery_tag: None),
        SimpleNamespace(delivery_tag=1),
        {
            "attempts": 0,
            "queue": "q",
            "job_id": "j",
            "db_job_id": 1,
            "_tenant": 5,
            "_tenant_mode": "tenant",
            "_otel": {},
        },
        SimpleNamespace(retry_backoff=[5]),
        exc,
        "q",
        "lease",
        SimpleNamespace(ensure_retry_progress_strict=lambda _job_id: None),
        1,
    )
    return delays[0]


def _swing_down(delay, _instance) -> int:
    return int(delay * 0.75)


def _swing_up(delay, _instance) -> int:
    return int(delay * 1.25)


def test_downward_jitter_never_undercuts_the_named_wait(monkeypatch) -> None:
    assert _requeue_delay(monkeypatch, _Named(600), _swing_down) == 600


def test_upward_jitter_still_spreads_the_retries(monkeypatch) -> None:
    assert _requeue_delay(monkeypatch, _Named(600), _swing_up) == 750


def test_a_fractional_wait_is_rounded_up_not_down(monkeypatch) -> None:
    assert _requeue_delay(monkeypatch, _Named(7.5), _swing_down) == 8


def test_the_plain_schedule_keeps_its_two_sided_jitter(monkeypatch) -> None:
    assert _requeue_delay(monkeypatch, RuntimeError("no named wait"), _swing_down) == 3

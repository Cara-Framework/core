import importlib
import queue
from types import SimpleNamespace

import pytest

from cara.commands.core.QueueWorkCommand import (
    AMQPConnectionManager,
    JobProcessor,
    QueueWorkCommand,
    ThreadSafeAMQPAckChannel,
)
from cara.exceptions import ConfigurationException


class _Channel:
    def __init__(self):
        self.declarations = []
        self.callbacks = []
        self.prefetch = None
        self.is_open = True
        self.cancelled = []
        self.acks = []
        self.nacks = []

    def queue_declare(self, **kwargs):
        self.declarations.append(kwargs)

    def basic_qos(self, *, prefetch_count, global_qos=False):
        self.prefetch = (prefetch_count, global_qos)

    def basic_consume(self, *, queue, on_message_callback, auto_ack):
        self.callbacks.append((queue, on_message_callback, auto_ack))
        return f"consumer-{queue}"

    def basic_cancel(self, consumer_tag):
        self.cancelled.append(consumer_tag)

    def close(self):
        self.is_open = False

    def basic_nack(self, *, delivery_tag, requeue):
        self.nacks.append((delivery_tag, requeue))

    def basic_ack(self, *, delivery_tag):
        self.acks.append(delivery_tag)


class _Connection:
    is_closed = False

    def __init__(self, consumer_channel):
        self.consumer_channel = consumer_channel
        self.calls = 0

    def process_data_events(self, *, time_limit):
        self.calls += 1
        if self.calls > 1:
            return
        queue, callback, _auto_ack = self.consumer_channel.callbacks[0]
        callback(
            self.consumer_channel,
            SimpleNamespace(delivery_tag=1),
            SimpleNamespace(),
            b"payload",
        )

    def add_callback_threadsafe(self, callback):
        callback()


class _Manager:
    def __init__(self, queues):
        self.driver = SimpleNamespace(
            canonical_queue_arguments=lambda queue: {
                "x-dead-letter-exchange": "dead.letter.dlx",
                "x-dead-letter-routing-key": f"dead.{queue}",
                "x-max-priority": 4,
            }
        )
        self.setup_channels = [_Channel() for _ in queues]
        self.consumer_channel = _Channel()
        self.connection = _Connection(self.consumer_channel)
        self.channels = [*self.setup_channels, self.consumer_channel]

    def create_channel(self):
        return self.channels.pop(0)


class _Processor:
    cancellation_registry = None

    def __init__(self):
        self.calls = []

    def process_message(self, channel, method, body, **kwargs):
        self.calls.append((channel, method, body, kwargs))
        return "success"


class _BackToBackConnection(_Connection):
    def __init__(self, consumer_channel):
        super().__init__(consumer_channel)
        self.callbacks_threadsafe = queue.Queue()
        self.deliveries = 0

    def add_callback_threadsafe(self, callback):
        self.callbacks_threadsafe.put(callback)

    def process_data_events(self, *, time_limit):
        _ = time_limit
        if self.deliveries >= 2:
            return
        queue_name, callback, _auto_ack = self.consumer_channel.callbacks[0]
        callback(
            self.consumer_channel,
            SimpleNamespace(delivery_tag=self.deliveries + 1),
            SimpleNamespace(),
            b"payload",
        )
        self.deliveries += 1
        self.callbacks_threadsafe.get(timeout=1)()
        if self.deliveries == 1:
            # Rabbit can deliver the next message in this same I/O turn as
            # soon as the ACK restores prefetch credit.
            callback(
                self.consumer_channel,
                SimpleNamespace(delivery_tag=2),
                SimpleNamespace(),
                b"payload",
            )
            self.deliveries += 1
            self.callbacks_threadsafe.get(timeout=1)()


class _AckingProcessor(_Processor):
    def process_message(self, channel, method, body, **kwargs):
        self.calls.append((channel, method, body, kwargs))
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return "success"


def test_worker_registers_one_quorum_consumer_with_per_consumer_qos():
    queues = ["sync"]
    manager = _Manager(queues)
    processor = _Processor()
    command = QueueWorkCommand.__new__(QueueWorkCommand)
    command.shutdown_requested = False
    command.jobs_processed = 0
    command.jobs_failed = 0

    def record_outcome(outcome, _config):
        assert outcome == "success"
        command.shutdown_requested = True

    command._record_worker_outcome = record_outcome
    command._consume_queue_stream(
        queue_names=queues,
        connection_manager=manager,
        job_processor=processor,
        config={"timeout": 1},
    )

    assert manager.setup_channels[0].declarations == [
        {"queue": "sync", "passive": True}
    ]
    assert manager.consumer_channel.prefetch == (1, False)
    assert [row[0] for row in manager.consumer_channel.callbacks] == queues
    assert all(row[2] is False for row in manager.consumer_channel.callbacks)
    assert manager.connection.calls == 1
    assert processor.calls[0][3]["queue_name"] == "sync"
    assert manager.consumer_channel.cancelled == ["consumer-sync"]
    assert command._active_consumer_slots == 0


def test_worker_rejects_multiple_quorum_consumers_on_one_channel():
    command = QueueWorkCommand.__new__(QueueWorkCommand)

    with pytest.raises(ConfigurationException, match="exactly one queue"):
        command._consume_queue_stream(
            queue_names=["sync", "connector"],
            connection_manager=_Manager(["sync", "connector"]),
            job_processor=_Processor(),
            config={"timeout": 1},
        )


def test_back_to_back_delivery_after_ack_does_not_race_future_completion():
    manager = _Manager(["sync"])
    manager.connection = _BackToBackConnection(manager.consumer_channel)
    processor = _AckingProcessor()
    command = QueueWorkCommand.__new__(QueueWorkCommand)
    command.shutdown_requested = False
    command.jobs_processed = 0
    command.jobs_failed = 0
    outcomes = []

    def record_outcome(outcome, _config):
        outcomes.append(outcome)
        if len(outcomes) == 2:
            command.shutdown_requested = True

    command._record_worker_outcome = record_outcome
    command._consume_queue_stream(
        queue_names=["sync"],
        connection_manager=manager,
        job_processor=processor,
        config={"timeout": 1},
    )

    assert outcomes == ["success", "success"]
    assert manager.consumer_channel.acks == [1, 2]
    assert len(processor.calls) == 2


def test_job_thread_ack_is_scheduled_on_the_pika_connection_thread():
    channel = _Channel()
    channel.acks = []
    channel.basic_ack = lambda *, delivery_tag: channel.acks.append(delivery_tag)
    connection = _Connection(_Channel())

    ThreadSafeAMQPAckChannel(connection, channel).basic_ack(delivery_tag=42)

    assert channel.acks == [42]


def test_worker_connection_preserves_driver_timeout_contract(monkeypatch):
    parameters = SimpleNamespace(
        heartbeat=60,
        blocked_connection_timeout=10,
        socket_timeout=5,
        stack_timeout=10,
    )
    driver = SimpleNamespace(
        options={},
        _connection_parameters=lambda _options: parameters,
    )
    captured = []
    module = importlib.import_module("pika")
    connection = object()
    monkeypatch.setattr(
        module,
        "BlockingConnection",
        lambda value: captured.append(value) or connection,
    )

    manager = AMQPConnectionManager(lambda *_args: None, driver)

    assert manager._create_connection() is connection
    assert captured == [parameters]
    assert parameters.heartbeat == 60
    assert parameters.blocked_connection_timeout == 10
    assert parameters.socket_timeout == 5
    assert parameters.stack_timeout == 10


def test_job_thread_nack_is_scheduled_on_the_pika_connection_thread():
    channel = _Channel()
    channel.nacks = []
    connection = _Connection(_Channel())

    ThreadSafeAMQPAckChannel(connection, channel).basic_nack(
        delivery_tag=43,
        requeue=False,
    )

    assert channel.nacks == [(43, False)]


def test_oversized_payload_is_dead_lettered_instead_of_acked_and_lost():
    channel = _Channel()

    result = JobProcessor.process_message(
        channel,
        SimpleNamespace(delivery_tag=44),
        b"x" * (JobProcessor.MAX_PAYLOAD_SIZE + 1),
        queue_name="sync",
    )

    assert result is False
    assert channel.nacks == [(44, False)]


def test_async_handler_arms_and_cancels_hard_timeout_watchdog(monkeypatch):
    module = importlib.import_module(
        "cara.commands.core.QueueWorkCommand"
    )
    timers = []

    class _Timer:
        def __init__(self, interval, callback, *, kwargs):
            self.interval = interval
            self.callback = callback
            self.kwargs = kwargs
            self.daemon = False
            self.started = False
            self.cancelled = False
            timers.append(self)

        def start(self):
            self.started = True

        def cancel(self):
            self.cancelled = True

    monkeypatch.setattr(module.threading, "Timer", _Timer)

    async def _handler():
        return "done"

    assert JobProcessor._execute_async_job_with_timeout(
        _handler,
        (),
        10,
    ) == "done"
    assert timers[0].interval == 15
    assert timers[0].started is True
    assert timers[0].cancelled is True


def test_uncooperative_timeout_watchdog_hard_exits_worker(monkeypatch):
    module = importlib.import_module(
        "cara.commands.core.QueueWorkCommand"
    )
    exits = []
    monkeypatch.setattr(
        module.os,
        "_exit",
        lambda code: exits.append(code),
    )

    JobProcessor._hard_kill_uncooperative_timeout(timeout_seconds=10)

    assert exits == [getattr(module.os, "EX_TEMPFAIL", 75)]

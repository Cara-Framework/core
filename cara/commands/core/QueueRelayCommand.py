"""Broker-independent queue outbox relay processes."""

from __future__ import annotations

import signal
import threading
from typing import Any

from cara.commands.CommandBase import CommandBase
from cara.configuration import config
from cara.decorators import command
from cara.exceptions import InvalidArgumentException, QueueException
from cara.facades import Log, Queue
from cara.observability import MetricsBase, start_http_server


class _RelayLoop(CommandBase):
    metric_name: str
    metrics_port_config: str
    operation_name: str

    def _iteration_is_healthy(
        self,
        _driver,
        _result: dict[str, int],
    ) -> bool:
        return True

    def _iteration_has_failures(self, result: dict[str, int]) -> bool:
        return False

    @staticmethod
    def _has_activity(result: dict[str, int]) -> bool:
        return any(
            int(value or 0) for key, value in result.items() if not key.startswith("_")
        )

    def _handle_loop(
        self,
        *,
        once: bool,
        poll_interval: str | float | None,
        callback,
    ) -> int:
        interval = float(poll_interval or 0.25)
        if not 0.05 <= interval <= 60:
            raise InvalidArgumentException(
                "--poll-interval must be between 0.05 and 60 seconds."
            )

        metric = getattr(MetricsBase, self.metric_name)
        metric.set(0)
        start_http_server(
            port=self._metrics_port(),
            role=self.operation_name,
        )

        stop = threading.Event()

        def _request_stop(_signum: int, _frame: Any) -> None:
            stop.set()

        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, _request_stop)
            signal.signal(signal.SIGTERM, _request_stop)

        driver = Queue.driver("amqp")
        while not stop.is_set():
            try:
                result = callback(driver)
                healthy = self._iteration_is_healthy(driver, result)
                metric.set(1 if healthy else 0)
            except Exception as exc:
                metric.set(0)
                if once:
                    raise
                Log.error(
                    "%s iteration failed; durable rows remain pending: %s",
                    self.operation_name,
                    exc,
                    category="cara.queue.delivery",
                )
                stop.wait(min(max(interval, 1.0), 5.0))
                continue

            if once and self._iteration_has_failures(result):
                raise QueueException(
                    f"{self.operation_name} iteration left failed work: {result}"
                )
            if not healthy:
                message = (
                    f"{self.operation_name} iteration lost runtime capability: {result}"
                )
                Log.error(
                    message,
                    category="cara.queue.delivery",
                )
                stop.wait(min(max(interval, 1.0), 5.0))
                continue
            if once:
                return 0
            if not self._has_activity(result):
                wakeup = getattr(driver, "_relay_wakeup", None)
                if wakeup is None:
                    stop.wait(interval)
                else:
                    wakeup.wait(interval)
                    wakeup.clear()
        metric.set(0)
        return 0

    def _metrics_port(self) -> int:
        return int(config(f"metrics.{self.metrics_port_config}", 0))


@command(
    name="queue:relay",
    help=(
        "PUBLISHER: the ONLY process that puts dispatched jobs on RabbitMQ. "
        "Not sufficient on its own — see below.\n"
        "\n"
        "`Bus.dispatch` does not talk to RabbitMQ. It commits a row to the "
        "`queue_job_delivery` outbox; this relay claims those rows and "
        "publishes them with confirms. Without it, dispatch still reports "
        "success and the work simply accumulates as `pending` forever.\n"
        "\n"
        "It publishes but never executes. Run `craft queue:work` alongside "
        "it to actually consume and run the jobs, and `craft schedule:work` "
        "if you want scheduled jobs to fire. Rule of thumb: a system that "
        "runs background work needs all three."
    ),
    options=[
        {
            "name": "--once",
            "help": "Run one bounded relay iteration and exit.",
            "type": bool,
            "default": False,
            "is_flag": True,
        },
        {
            "name": "--poll-interval",
            "help": "Idle poll interval in seconds (default: 0.25).",
            "type": float,
            "default": None,
            "is_flag": False,
        },
    ],
)
class QueueRelayCommand(_RelayLoop):
    """Long-running PostgreSQL-to-RabbitMQ publication relay."""

    metric_name = "queue_relay_ready"
    metrics_port_config = "relay_port"
    operation_name = "queue-relay"

    def _iteration_is_healthy(
        self,
        _driver,
        result: dict[str, int],
    ) -> bool:
        return not any(
            int(result.get(key, 0) or 0)
            for key in (
                "retried",
                "settle_lost",
            )
        )

    def _iteration_has_failures(self, result: dict[str, int]) -> bool:
        return any(
            int(result.get(key, 0) or 0)
            for key in ("retried", "settle_lost", "quarantined")
        )

    def handle(
        self,
        once: bool = False,
        poll_interval: str | float | None = None,
    ) -> int:
        return self._handle_loop(
            once=once,
            poll_interval=poll_interval,
            callback=lambda driver: driver.relay_publish_once(),
        )

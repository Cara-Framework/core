"""Broker-independent queue outbox relay processes."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import uuid
from typing import Any

from cara.commands import CommandBase
from cara.configuration import config
from cara.decorators import command
from cara.exceptions import InvalidArgumentException, QueueException
from cara.facades import Log, Queue


class _RelayLoop(CommandBase):
    metric_name: str
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
            int(value or 0)
            for key, value in result.items()
            if not key.startswith("_")
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

        from cara.observability import MetricsBase, start_http_server

        metric = getattr(MetricsBase, self.metric_name)
        metric.set(0)
        start_http_server(
            port=int(config("metrics.port", 0)),
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
                    f"{self.operation_name} iteration left failed work: "
                    f"{result}"
                )
            if not healthy:
                message = (
                    f"{self.operation_name} iteration lost runtime capability: "
                    f"{result}"
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


@command(
    name="queue:relay",
    help="Publish durable queue outbox rows with confirms.",
    options={
        "--once": "Run one bounded relay iteration and exit.",
        "--poll-interval=?": "Idle poll interval in seconds (default: 0.25).",
    },
)
class QueueRelayCommand(_RelayLoop):
    """Long-running PostgreSQL-to-RabbitMQ publication relay."""

    metric_name = "queue_relay_ready"
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


@command(
    name="queue:hooks",
    help="Process durable queue terminal-hook outbox rows.",
    options={
        "--once": "Run one bounded hook iteration and exit.",
        "--poll-interval=?": "Idle poll interval in seconds (default: 0.25).",
    },
)
class QueueHooksCommand(_RelayLoop):
    """Long-running terminal-hook outbox relay."""

    metric_name = "queue_hooks_ready"
    operation_name = "queue-hooks"

    def _iteration_is_healthy(
        self,
        _driver,
        _result: dict[str, int],
    ) -> bool:
        # A child hook failure is a durable work-item outcome with its own
        # retry/quarantine metrics, not proof that the hook service cannot
        # access its DB or spawn children. Those systemic failures raise from
        # the callback and drive readiness to zero in the base loop.
        return True

    def _iteration_has_failures(self, result: dict[str, int]) -> bool:
        return any(
            int(result.get(key, 0) or 0)
            for key in ("failed", "quarantined")
        )

    def handle(
        self,
        once: bool = False,
        poll_interval: str | float | None = None,
    ) -> int:
        return self._handle_loop(
            once=once,
            poll_interval=poll_interval,
            callback=self._run_isolated_hooks,
        )

    @staticmethod
    def _run_isolated_hooks(driver) -> dict[str, int]:
        job_ids = driver.due_terminal_hook_ids()
        result = {
            "claimed": len(job_ids),
            "completed": 0,
            "failed": 0,
            "deferred": 0,
            "quarantined": 0,
            "skipped": 0,
        }
        timeout = int(driver.delivery_store.hook_timeout_seconds) + 15
        child_env = dict(os.environ)
        child_env["METRICS_PORT"] = "0"

        def _record_child_failure(job_id: str, error: str) -> None:
            outcome = driver.defer_terminal_hook_process_failure(
                job_id,
                error=error,
            )
            if outcome == "completed":
                result["completed"] += 1
                return
            result["failed"] += 1
            if outcome == "quarantined":
                result["quarantined"] += 1
            else:
                result["deferred"] += 1

        for job_id in job_ids:
            try:
                completed = subprocess.run(
                    [
                        sys.executable,
                        sys.argv[0],
                        "queue:hook",
                        "--job-id",
                        job_id,
                    ],
                    check=False,
                    env=child_env,
                    timeout=timeout,
                )
            except subprocess.TimeoutExpired:
                _record_child_failure(
                    job_id,
                    "isolated terminal-hook process timed out",
                )
                Log.error(
                    "Queue terminal hook %s exceeded the isolated process "
                    "timeout and was killed.",
                    job_id,
                    category="cara.queue.delivery",
                )
                continue
            if completed.returncode == 0:
                result["completed"] += 1
            elif completed.returncode == getattr(os, "EX_TEMPFAIL", 75):
                result["skipped"] += 1
            else:
                _record_child_failure(
                    job_id,
                    "isolated terminal-hook process exited with "
                    f"status {completed.returncode}",
                )
        driver.refresh_delivery_metrics()
        return result


@command(
    name="queue:hook",
    help="Process one claimed terminal-hook row in an isolated process.",
    options={"--job-id=?": "Queue delivery UUID."},
)
class QueueHookCommand(CommandBase):
    """Internal single-hook subprocess target."""

    def handle(self, job_id: str | None = None) -> int:
        try:
            canonical = str(uuid.UUID(str(job_id)))
        except (TypeError, ValueError, AttributeError) as exc:
            raise InvalidArgumentException(
                "--job-id must be a valid UUID."
            ) from exc
        processed = Queue.driver("amqp").process_terminal_hook(canonical)
        return 0 if processed else getattr(os, "EX_TEMPFAIL", 75)

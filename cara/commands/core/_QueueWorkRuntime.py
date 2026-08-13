"""Queue worker connection, consumer and memory runtime."""

from __future__ import annotations

import concurrent.futures
import contextlib
import os
import threading
import time
from collections import deque
from typing import Any

from cara.configuration import config as global_config
from cara.exceptions import ConfigurationException
from cara.facades import Log, Queue
from cara.observability import MetricsBase as _Metrics

from .ActiveJobCancellationRegistry import ActiveJobCancellationRegistry
from .AMQPConnectionManager import AMQPConnectionManager
from .JobProcessor import JobProcessor
from .ThreadSafeAMQPAckChannel import ThreadSafeAMQPAckChannel


def _queue_work_run_worker(self, config: dict[str, Any]) -> None:
    """Run long-lived AMQP consumers with bounded parallelism.

    We spin up N independent consumer threads, including when N=1, each
    with its own AMQP connection + broker-side subscription. Keeping the command/signal
    loop on the main thread is what makes the bounded shutdown and async
    cancellation contract enforceable for every concurrency setting. pika's
    BlockingConnection is not thread-safe across threads, so each
    thread keeps its own manager. The threads share:

    * The job processor (stateless, so safe to share).
    * The Cara DB connection pool + in-flight semaphore (module-level,
      built for multi-thread access from the start).
    * ``jobs_processed`` / ``jobs_failed`` counters — incremented
      under a lock below (would otherwise race and undercount).

    The number of in-flight jobs is bounded by ``concurrency``; each
    channel uses ``prefetch_count=1`` and handles one delivery at a time.
    RabbitMQ pushes jobs immediately; there is no ``basic_get`` polling,
    idle sleep, or per-cycle channel churn.
    """
    queue_names = config["queue_names"]
    concurrency = getattr(self, "_concurrency", 1)
    if concurrency < len(queue_names):
        raise ConfigurationException(
            "Quorum queue workers require at least one consumer slot per "
            f"configured queue ({concurrency} slots for "
            f"{len(queue_names)} queues)."
        )

    self._show_worker_startup_info(queue_names, concurrency)
    self.start_time = time.time()
    self._active_job_cancellations = ActiveJobCancellationRegistry()
    self._consumer_threads = []
    self._consumer_state_lock = threading.Lock()
    self._active_consumer_slots = 0

    # Lock protecting shared counters + shutdown flag read-modify-writes.
    # shutdown_requested itself is a bool (atomic) so we read it
    # unlocked; counters genuinely need a lock.
    self._stats_lock = threading.Lock()

    # Consumer-thread mode. Even concurrency=1 stays off the main thread so
    # SIGTERM can cancel/force-redeliver a job that exceeds the drain budget.

    job_processor = JobProcessor(self._active_job_cancellations)
    queue_driver = Queue.driver(config["driver_name"])
    if not hasattr(queue_driver, "_connection_parameters"):
        raise ConfigurationException(
            "queue:work requires the AMQP driver for durable subscriptions"
        )
    threads: list[threading.Thread] = []
    self._consumer_threads = threads

    def _consumer_loop(slot_idx: int) -> None:
        """One durable consumer slot with bounded reconnect backoff."""
        reconnect_delay = min(max(int(config.get("timeout", 5)), 1), 10)
        assigned_queue = queue_names[(slot_idx - 1) % len(queue_names)]
        while not self.shutdown_requested:
            mgr = AMQPConnectionManager(global_config, queue_driver)
            try:
                self._consume_queue_stream(
                    queue_names=[assigned_queue],
                    connection_manager=mgr,
                    job_processor=job_processor,
                    config=config,
                )
            except Exception as exc:
                if not self.shutdown_requested:
                    Log.warning(
                        "[worker-%s] AMQP consumer disconnected: %s",
                        slot_idx,
                        exc,
                    )
            finally:
                mgr.close()
            if not self.shutdown_requested:
                time.sleep(reconnect_delay)

    try:
        for i in range(concurrency):
            t = threading.Thread(
                target=_consumer_loop,
                args=(i + 1,),
                name=f"queue-worker-{i + 1}",
                daemon=True,
            )
            t.start()
            threads.append(t)

        # Main thread just waits for shutdown. Poll for the signal
        # rather than join() because join() on daemon threads would
        # block forever if one thread deadlocks. Also poll the
        # configured stop conditions so --max-time fires even if
        # every consumer is blocked on a slow job (otherwise a
        # poison-message that hangs forever would never trip the
        # cap).
        next_broker_probe_at = 0.0
        while not self.shutdown_requested:
            now = time.monotonic()
            if now >= next_broker_probe_at:
                try:
                    Queue.driver(config["driver_name"]).verify_runtime_health(
                        queue_names,
                        force=True,
                    )
                except Exception as exc:
                    _Metrics.queue_worker_ready.set(0)
                    Log.warning("Queue worker readiness probe failed: %s", exc)
                else:
                    with self._consumer_state_lock:
                        active_slots = self._active_consumer_slots
                    _Metrics.queue_worker_ready.set(
                        1 if active_slots == concurrency else 0
                    )
                next_broker_probe_at = now + 10.0
            if self._should_stop(config):
                self.shutdown_requested = True
                break
            time.sleep(1)
    finally:
        self.shutdown_requested = True
        drained = self._drain_consumer_threads(threads)
        # Production cannot observe ``False`` because the escalation path
        # calls ``os._exit``. The condition lets tests replace hard-exit
        # without accidentally running hooks while fake consumers remain.
        if drained:
            self._shutdown_worker_resources()


def _queue_work_check_memory_usage(self) -> bool:
    """
    Check worker memory usage and exit gracefully if limit exceeded.
    CRITICAL FIX #3: Enforce memory limit to prevent unbounded growth.
    Returns True if memory is within limits, False if exceeded.
    """
    try:
        import psutil  # local: heavy optional dep

        process = psutil.Process(os.getpid())
        rss_bytes = process.memory_info().rss

        if rss_bytes > self.memory_limit_bytes:
            limit_mb = self.memory_limit_bytes / (1024 * 1024)
            current_mb = rss_bytes / (1024 * 1024)
            Log.warning(
                "⚠️ Memory limit exceeded: %.1fMB > %.1fMB. "
                "Initiating graceful shutdown for supervisor restart.",
                current_mb,
                limit_mb,
            )
            self.shutdown_requested = True
            return False

        return True
    except ImportError:
        # psutil not available, fall back to /proc on Linux
        try:
            with open(f"/proc/{os.getpid()}/status", "r") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        rss_kb = int(line.split()[1])
                        rss_bytes = rss_kb * 1024

                        if rss_bytes > self.memory_limit_bytes:
                            limit_mb = self.memory_limit_bytes / (1024 * 1024)
                            current_mb = rss_bytes / (1024 * 1024)
                            Log.warning(
                                "⚠️ Memory limit exceeded: %.1fMB > %.1fMB. "
                                "Initiating graceful shutdown for supervisor restart.",
                                current_mb,
                                limit_mb,
                            )
                            self.shutdown_requested = True
                            return False
                        break
        except ImportError, RuntimeError, AttributeError, OSError:
            pass

        return True


def _queue_work_show_worker_startup_info(
    self, queue_names: list, concurrency: int = 1
) -> None:
    """Display worker startup information in ServeCommand style."""
    self.console.print("[bold #e5c07b]┌─ Worker Status[/bold #e5c07b]")

    if len(queue_names) > 1:
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Processing:[/white] [dim]{len(queue_names)} canonical queues[/dim]"
        )
    else:
        queue_color = (
            "#E21102"
            if "critical" in queue_names[0]
            else "#e5c07b"
            if "high" in queue_names[0]
            else "#30e047"
        )
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Monitoring:[/white] [{queue_color}]{queue_names[0]}[/{queue_color}]"
        )

    if concurrency > 1:
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Concurrency:[/white] "
            f"[#30e047]{concurrency} parallel consumer threads[/#30e047]"
        )

    self.console.print(
        "[#e5c07b]│[/#e5c07b] [white]Status:[/white] [#30e047]✓ Active - Waiting for jobs[/#30e047]"
    )

    self.console.print("[#e5c07b]└─[/#e5c07b]")
    self.console.print()

    # Simple ready message
    self.console.print("[dim]Press Ctrl+C to stop the worker[/dim]")
    self.console.print()


def _queue_work_record_worker_outcome(
    self,
    outcome: bool | str,
    config: dict[str, Any],
) -> None:
    if outcome:
        with self._stats_lock:
            if outcome == "success":
                self.jobs_processed += 1
            elif outcome == "failure":
                self.jobs_failed += 1
    if not self._check_memory_usage() or self._should_stop(config):
        self.shutdown_requested = True


def _queue_work_mark_consumer_slot(self, delta: int) -> None:
    """Track subscriptions so readiness reflects actual consuming ability."""
    state_lock = getattr(self, "_consumer_state_lock", None)
    if state_lock is None:
        state_lock = threading.Lock()
        self._consumer_state_lock = state_lock
    with state_lock:
        current = int(getattr(self, "_active_consumer_slots", 0))
        self._active_consumer_slots = max(0, current + delta)


def _queue_work_consume_queue_stream(
    self,
    *,
    queue_names: list[str],
    connection_manager: AMQPConnectionManager,
    job_processor: JobProcessor,
    config: dict[str, Any],
) -> None:
    """Register durable subscriptions and service broker deliveries."""
    if len(queue_names) != 1:
        raise ConfigurationException(
            "Each quorum-queue consumer channel must own exactly one queue."
        )
    for queue_name in queue_names:
        self._verify_consumer_queue(connection_manager, queue_name)

    channel = connection_manager.create_channel()
    if channel is None:
        raise ConnectionError("RabbitMQ consumer channel could not be created")
    # RabbitMQ quorum queues do not support global QoS. Each channel owns
    # exactly one consumer, so per-consumer prefetch=1 is also the exact
    # one-job-per-worker-slot bound.
    channel.basic_qos(prefetch_count=1, global_qos=False)

    consumer_tags: list[str] = []
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    in_flight: concurrent.futures.Future | None = None
    settled_futures: deque[concurrent.futures.Future] = deque()
    subscribed = False
    try:
        for queue_name in queue_names:

            def _on_message(
                ch,
                method_frame,
                _header_frame,
                body,
                *,
                consumed_queue: str = queue_name,
            ) -> None:
                nonlocal in_flight
                if in_flight is not None:
                    raise RuntimeError("RabbitMQ delivered more than prefetch_count=1")

                def _release_settled_slot() -> None:
                    nonlocal in_flight
                    if in_flight is None:
                        raise RuntimeError(
                            "RabbitMQ settled a delivery without an "
                            "in-flight worker future."
                        )
                    settled_futures.append(in_flight)
                    in_flight = None

                ack_channel = ThreadSafeAMQPAckChannel(
                    connection_manager.connection,
                    ch,
                    on_settled=_release_settled_slot,
                )
                start_gate = threading.Event()

                def _process_delivery():
                    start_gate.wait()
                    return job_processor.process_message(
                        ack_channel,
                        method_frame,
                        body,
                        queue_name=consumed_queue,
                        cancellation_registry=(job_processor.cancellation_registry),
                    )

                in_flight = executor.submit(_process_delivery)
                start_gate.set()

            consumer_tags.append(
                channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=_on_message,
                    auto_ack=False,
                )
            )

        self._mark_consumer_slot(1)
        subscribed = True
        Log.info(
            "AMQP worker subscribed to %s",
            ", ".join(queue_names),
        )
        while (
            not self.shutdown_requested
            or (in_flight is not None and not in_flight.done())
            or any(not future.done() for future in settled_futures)
        ):
            connection = connection_manager.connection
            if connection is None or connection.is_closed:
                pending = [
                    future
                    for future in (in_flight, *settled_futures)
                    if future is not None and not future.done()
                ]
                if pending:
                    self.shutdown_requested = True
                    time.sleep(0.1)
                    continue
                raise ConnectionError("RabbitMQ consumer connection closed")
            try:
                connection.process_data_events(time_limit=0.25)
            except Exception:
                pending = [
                    future
                    for future in (in_flight, *settled_futures)
                    if future is not None and not future.done()
                ]
                if pending:
                    self.shutdown_requested = True
                    while any(not future.done() for future in pending):
                        time.sleep(0.1)
                raise
            pending_settlements = [
                future
                for future in (in_flight, *settled_futures)
                if future is not None and not future.done()
            ]
            if pending_settlements:
                # Real pika blocks for ``time_limit`` while servicing the
                # I/O loop. Test/fallback connections may return
                # immediately; yield briefly so a handler that just
                # settled can finish without a hot process_data_events
                # spin. The short timeout cannot deadlock a handler waiting
                # for its thread-safe ACK callback because the I/O loop is
                # serviced again on the next iteration.
                concurrent.futures.wait(
                    pending_settlements,
                    timeout=0.01,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
            while settled_futures and settled_futures[0].done():
                outcome = settled_futures.popleft().result()
                self._record_worker_outcome(outcome, config)
            if in_flight is not None and in_flight.done():
                outcome = in_flight.result()
                in_flight = None
                self._record_worker_outcome(outcome, config)
            if not self.shutdown_requested and self._should_stop(config):
                self.shutdown_requested = True
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
        if subscribed:
            self._mark_consumer_slot(-1)
        if getattr(channel, "is_open", False):
            for consumer_tag in consumer_tags:
                with contextlib.suppress(Exception):
                    channel.basic_cancel(consumer_tag)
            with contextlib.suppress(Exception):
                channel.close()

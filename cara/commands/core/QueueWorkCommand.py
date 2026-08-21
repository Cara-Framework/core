"""
Queue Worker Command for the Cara framework.

This module provides a CLI command to process jobs from the queue with enhanced UX.
"""

from __future__ import annotations

import atexit
import contextlib
import logging
import os
import signal
import sys
import threading
import traceback

from cara.commands.CommandBase import CommandBase
from cara.commands.MakesAutoReload import MakesAutoReload
from cara.configuration import config
from cara.decorators import command
from cara.exceptions import (
    CaraException,
    InvalidArgumentException,
)
from cara.facades import Log
from cara.observability import MetricsBase as _Metrics
from cara.observability import start_http_server as _start_metrics
from cara.queues.delivery import PublicationBacklogProbe

from . import _QueueWorkConfiguration, _QueueWorkLifecycle, _QueueWorkRuntime

# These live under ``cara.queues``. The package is import-safe WITHOUT the
# optional 'queue' extra (pika) — ``AMQPDriver`` degrades its pika import to
# ``None`` and re-checks at connection time — so importing them at module top
# no longer forces every service to install pika just to load the command
# package. A worker that actually runs still needs pika and fails LOUD when it
# opens a connection.
from .ActiveJobCancellationRegistry import ActiveJobCancellationRegistry

# Prometheus metrics — framework-owned MetricsBase carries the queue/worker
# metrics. Guarded so a partial import never breaks the worker.
try:
    from cara.observability.MetricsBase import MetricsBase
except ImportError, RuntimeError:  # pragma: no cover
    MetricsBase = None  # type: ignore[assignment]

_logger = logging.getLogger("cara.queue.worker")


# Silence pika's remote Channel.Close (404) warnings — worker polls a
# superset of queue names via wildcards, so "queue doesn't exist" on a
# passive declare is expected for empty queues. The worker already caches
# the miss in ``_missing_queues``; pika still logs each channel close at
# WARNING level on the underlying logger, which spams the console every
# retry tick. Silencing here keeps the worker's own log line ("No job
# found" / job output) readable.
for _pika_logger in (
    "pika",
    "pika.channel",
    "pika.connection",
    "pika.adapters.blocking_connection",
    "pika.adapters.utils.connection_workflow",
    "pika.adapters.utils.io_services_utils",
):
    logging.getLogger(_pika_logger).setLevel(logging.CRITICAL)


@command(
    name="queue:work",
    help=(
        "CONSUMER: take jobs off RabbitMQ and run them. "
        "Not sufficient on its own — see below.\n"
        "\n"
        "This process only CONSUMES. It never publishes. `Bus.dispatch` does "
        "not write to RabbitMQ either: it commits a row to the "
        "`queue_job_delivery` outbox, and `craft queue:relay` is the ONLY "
        "thing that turns those rows into broker messages.\n"
        "\n"
        "So `queue:work` alone gets you a worker listening to an empty "
        "broker: every dispatch reports success, nothing ever runs, and "
        "nothing else complains. Run `craft queue:relay` alongside it. Add "
        "`craft schedule:work` if you also want scheduled jobs to fire.\n"
        "\n"
        "This command warns at startup if it finds an aged, undrained "
        "outbox — but it will still start, so read the banner."
    ),
    options=[
        {
            "name": "--driver",
            "help": "Queue driver to use (overrides default configuration)",
            "type": str,
        },
        {
            "name": "--queue",
            "help": "Queue name(s) to process (comma-separated)",
            "type": str,
        },
        {
            "name": "--pool",
            "help": "Worker pool name from config/queue.py WORKER_POOLS",
            "type": str,
        },
        {
            "name": "--timeout",
            "help": "Reconnect backoff in seconds after a broker disconnect",
            "type": str,
        },
        {
            "name": "--max-jobs",
            "help": "Maximum number of jobs to process before stopping",
            "type": str,
        },
        {
            "name": "--max-time",
            "help": "Maximum runtime in seconds before stopping",
            "type": str,
        },
        {
            "name": "--concurrency",
            "help": "Parallel consumer threads inside this worker process",
            "type": str,
        },
        {
            "name": "--reload",
            "help": "Enable auto-reload on file changes",
            "is_flag": True,
        },
    ],
)
class QueueWorkCommand(MakesAutoReload, CommandBase):
    """Run queue worker with enhanced monitoring and graceful shutdown."""

    def __init__(self, application=None):
        super().__init__(application)
        self.start_time = None
        self.jobs_processed = 0
        self.jobs_failed = 0
        self.memory_limit_bytes = self._resolve_memory_limit_mb() * 1024 * 1024
        self._signal_handlers_installed = False
        self._atexit_registered = False
        self._shutdown_signal: int | None = None
        self._consumer_threads: list[threading.Thread] = []
        self._consumer_state_lock = threading.Lock()
        self._active_consumer_slots = 0
        self._active_job_cancellations = ActiveJobCancellationRegistry()
        self._resource_shutdown_lock = threading.Lock()
        self._worker_resources_shutdown = False
        self._reload_requested = False

    def _setup_worker_lifecycle_hooks(self) -> None:
        """Install graceful-shutdown hooks once per worker process.

        Without SIGINT/SIGTERM handlers the worker only stopped when the
        poll loop happened to check ``shutdown_requested`` — Ctrl+C could
        abort mid-job and skip ``_shutdown_worker_resources``. ``atexit``
        covers abrupt ``sys.exit`` / supervisor SIGKILL-followup paths that
        still run interpreter teardown.
        """
        if not self._signal_handlers_installed:

            def _graceful_stop(signum, _frame):
                # First signal requests a bounded graceful drain. A second
                # signal is an explicit operator request to stop immediately;
                # hard process exit is the only safe way to interrupt arbitrary
                # Python threads, and lets RabbitMQ redeliver every unacked job.
                if self.shutdown_requested:
                    self._force_terminate_for_redelivery(
                        reason="second shutdown signal",
                        signal_number=signum,
                    )
                    return

                self._shutdown_signal = signum
                self.shutdown_requested = True
                Log.info(
                    "Queue worker received %s — draining current job then exiting",
                    signal.Signals(signum).name if hasattr(signal, "Signals") else signum,
                )

            signal.signal(signal.SIGINT, _graceful_stop)
            signal.signal(signal.SIGTERM, _graceful_stop)
            self._signal_handlers_installed = True

        if not self._atexit_registered:
            atexit.register(self._shutdown_worker_resources)
            self._atexit_registered = True

    @staticmethod
    def _resolve_memory_limit_mb() -> int:
        """Graceful-recycle threshold in MiB — WORKER_MEMORY_LIMIT_MB env,
        2 GiB when unset.

        The configured value is a DEFAULT, never a floor: recycle only works
        when this threshold sits BELOW the container's cgroup cap, and the
        old ``max(limit_mb, 2048)`` floor made every lower setting dead —
        with a 2 GiB container cap the kernel OOM-killed the worker (un-acked
        job, lease wait) before the recycle check could ever fire.
        """
        try:
            return int(config("queue.worker_memory_limit_mb", 2048))
        except Exception:
            return 2048

    def handle(
        self,
        driver: str | None = None,
        queue: str | None = None,
        pool: str | None = None,
        timeout: str | None = None,
        max_jobs: str | None = None,
        max_time: str | None = None,
        concurrency: str | None = None,
    ):
        """Handle queue worker execution with enhanced monitoring."""
        # ── Pool resolution ────────────────────────────────────────
        # --pool=<name> reads WORKER_POOLS from config/queue.py and
        # overrides --queue, --concurrency, and --timeout with pool
        # values. Explicit flags still take precedence.
        if pool:
            pool_cfg = self._resolve_pool(pool)
            if pool_cfg is None:
                raise InvalidArgumentException(f"Invalid worker pool: {pool}")
            if not queue:
                queue = ",".join(pool_cfg["queues"])
            if not concurrency:
                concurrency = str(pool_cfg.get("concurrency", 1))
            if not timeout:
                timeout = str(pool_cfg.get("timeout", 5))
            # A pool may declare its own graceful-recycle threshold.
            # Precedence: an EXPLICIT deploy-time env always wins (that is
            # the operator's knob), then the pool declaration, then the
            # config default — so neither knob can silently dead-end the
            # other again.
            pool_memory_mb = pool_cfg.get("max_memory_mb")
            if pool_memory_mb and os.environ.get("WORKER_MEMORY_LIMIT_MB") is None:
                self.memory_limit_bytes = int(pool_memory_mb) * 1024 * 1024

        self.console.print()  # Empty line for spacing
        self.console.print("[bold #e5c07b]╭─ Queue Worker ─╮[/bold #e5c07b]")
        self.console.print()

        # Stand up /metrics on a side-thread HTTP server so Prometheus
        # can scrape the worker. Opt out with METRICS_PORT=0.
        try:
            _Metrics.queue_worker_ready.set(0)
            _Metrics.queue_worker_configured_queues.set(0)
            _port = _start_metrics(role="worker")
            if _port:
                Log.info("📈 Metrics server on :%s/metrics", _port)
        except Exception as e:
            if str(config("app.env", "local")).lower() in {"production", "prod"}:
                raise CaraException("Worker metrics server failed to start") from e
            Log.warning("metrics server startup failed: %s", e)

        # Is anything actually PUBLISHING the work this worker consumes?
        # ``queue:work`` and ``queue:relay`` read like a matched pair but
        # only one of them is a consumer; starting this one alone leaves a
        # worker listening to an empty broker while dispatches pile up in
        # the outbox (2026-07-20: 1250 jobs, zero complaints). Advisory
        # only — see PublicationBacklogProbe on why this must never be
        # able to stop a worker from starting.
        self._warn_when_nothing_is_publishing()

        # Worker-startup hooks — declared by the app in
        # config/queue.py::WORKER_STARTUP_HOOKS (dotted paths to sync
        # callables, e.g. a domain metrics sampler). Kept out of the
        # framework so cara carries no app/domain startup logic.
        self._run_worker_startup_hooks()

        # Parse concurrency early so we can use it to gate the reload path
        # (auto-reload restarts the whole worker — fine with 1 thread, but
        # with N parallel consumer threads we want to drain them first).
        concurrency_val = 1
        if concurrency:
            try:
                concurrency_val = max(1, int(concurrency))
            except ValueError:
                raise InvalidArgumentException(
                    f"Invalid --concurrency value: {concurrency!r}"
                )
        self._concurrency = concurrency_val

        # Store parameters for restart
        self.store_restart_params(driver, queue, timeout, max_jobs, max_time)

        # Auto-reload only when explicitly requested — module purging
        # invalidates IoC container bindings (contract→implementation
        # identity is lost after re-import), causing resolution failures
        # like "Can't instantiate abstract class …Contract".
        if self.option("reload"):
            self.enable_auto_reload()

        self._setup_worker_lifecycle_hooks()

        # Start main worker loop
        try:
            self._run_main_loop(driver, queue, timeout, max_jobs, max_time)
        except Exception as e:
            self.error(f"× Worker error: {e}")
            self.error(f"× Stack trace: {traceback.format_exc()}")
            raise
        finally:
            with contextlib.suppress(Exception):
                _Metrics.queue_worker_ready.set(0)
            self.cleanup_auto_reload()
            self._show_final_stats()
        if self._reload_requested:
            self._restart_worker_process()

    def _warn_when_nothing_is_publishing(self) -> str | None:
        """Print a loud banner when the outbox is aged and undrained.

        Returns the advisory text (for tests), or ``None`` when there is
        nothing to say. NEVER raises and NEVER exits: an operator whose
        relay is down still needs their worker to come up, and a
        diagnostic that kills its host process is worse than the silence
        it replaces.
        """

        def _emit(message: str) -> None:
            self.console.print()
            self.console.print(
                "[bold #e06c75]⚠ NOTHING IS PUBLISHING TO THE BROKER[/bold #e06c75]"
            )
            for line in message.splitlines():
                self.console.print(f"[#e5c07b]  {line}[/#e5c07b]")
            self.console.print()

        try:
            return PublicationBacklogProbe.announce(emit=_emit)
        except Exception:  # noqa: BLE001 — belt and braces; see docstring
            return None

    def _trigger_auto_reload(self) -> None:
        """Drain the worker, then replace the process for code reload.

        The generic in-process reload purges app modules and resource pools
        after a fixed 500ms sleep. That is unsafe for queue consumers whose
        jobs may still be executing in other threads. A process replacement
        after the normal drain gives every consumer the same shutdown contract
        as SIGTERM and starts with a coherent module/container graph.
        """
        if not self._auto_reload_enabled or self._reload_requested:
            return
        self.info("🔄 File changed — draining worker before process reload")
        self._reload_requested = True
        self.shutdown_requested = True

    @staticmethod
    def _restart_worker_process() -> None:
        """Replace the drained worker with the same interpreter/arguments."""
        os.execv(sys.executable, [sys.executable, *sys.argv])

    _prepare_config = _QueueWorkConfiguration._queue_work_prepare_config
    _resolve_pool = _QueueWorkConfiguration._queue_work_resolve_pool
    _parse_queue_names = _QueueWorkConfiguration._queue_work_parse_queue_names
    _expand_wildcard_pattern = _QueueWorkConfiguration._queue_work_expand_wildcard_pattern
    _discover_rabbitmq_queues = (
        _QueueWorkConfiguration._queue_work_discover_rabbitmq_queues
    )
    _show_config = _QueueWorkConfiguration._queue_work_show_config
    _verify_consumer_queue = staticmethod(
        _QueueWorkConfiguration._queue_work_verify_consumer_queue
    )

    _run_worker = _QueueWorkRuntime._queue_work_run_worker
    _check_memory_usage = _QueueWorkRuntime._queue_work_check_memory_usage
    _show_worker_startup_info = _QueueWorkRuntime._queue_work_show_worker_startup_info
    _record_worker_outcome = _QueueWorkRuntime._queue_work_record_worker_outcome
    _mark_consumer_slot = _QueueWorkRuntime._queue_work_mark_consumer_slot
    _consume_queue_stream = _QueueWorkRuntime._queue_work_consume_queue_stream

    _drain_consumer_threads = _QueueWorkLifecycle._queue_work_drain_consumer_threads
    _join_threads_until = staticmethod(_QueueWorkLifecycle._queue_work_join_threads_until)
    _force_terminate_for_redelivery = staticmethod(
        _QueueWorkLifecycle._queue_work_force_terminate_for_redelivery
    )
    _shutdown_drain_seconds = staticmethod(
        _QueueWorkLifecycle._queue_work_shutdown_drain_seconds
    )
    _shutdown_cancel_seconds = staticmethod(
        _QueueWorkLifecycle._queue_work_shutdown_cancel_seconds
    )
    _run_worker_startup_hooks = staticmethod(
        _QueueWorkLifecycle._queue_work_run_worker_startup_hooks
    )
    _shutdown_worker_resources = _QueueWorkLifecycle._queue_work_shutdown_worker_resources
    _should_stop = _QueueWorkLifecycle._queue_work_should_stop
    _get_runtime = _QueueWorkLifecycle._queue_work_get_runtime
    _show_final_stats = _QueueWorkLifecycle._queue_work_show_final_stats
    _resolve_job_model = _QueueWorkLifecycle._queue_work_resolve_job_model
    _run_main_loop = _QueueWorkLifecycle._queue_work_run_main_loop
    _cleanup_connections_for_restart = (
        _QueueWorkLifecycle._queue_work_cleanup_connections_for_restart
    )
    _cleanup_watching = _QueueWorkLifecycle._queue_work_cleanup_watching

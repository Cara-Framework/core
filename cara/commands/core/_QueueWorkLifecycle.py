"""Queue worker draining, shutdown and main-loop lifecycle."""

from __future__ import annotations

import asyncio
import builtins
import contextlib
import importlib
import logging
import os
import threading
import time
from typing import Any

from cara.configuration import config
from cara.facades import Log, Queue
from cara.observability import MetricsBase as _Metrics

_logger = logging.getLogger("cara.queue.worker")


def _queue_work_drain_consumer_threads(self, threads: list[threading.Thread]) -> bool:
    """Drain, then cancel, parallel consumers without tearing resources down.

    Phase 1 gives in-flight work the configured graceful budget. Phase 2
    cooperatively cancels registered asyncio jobs; their deliveries remain
    unacknowledged and each consumer closes its own AMQP connection before
    returning. Python cannot safely interrupt a running synchronous thread,
    so anything still alive after the cancellation grace forces a process
    exit. The OS then closes sockets and DB connections atomically, which
    makes RabbitMQ redeliver instead of fabricating job failures.
    """
    drain_budget = self._shutdown_drain_seconds()
    self._join_threads_until(threads, time.monotonic() + drain_budget)
    still_alive = [thread for thread in threads if thread.is_alive()]
    if not still_alive:
        return True

    cancelled = self._active_job_cancellations.cancel_all()
    cancel_budget = self._shutdown_cancel_seconds()
    Log.warning(
        "Worker shutdown: %s consumer thread(s) exceeded the %ss drain; "
        "requested cancellation for %s async job(s)",
        len(still_alive),
        drain_budget,
        cancelled,
    )
    self._join_threads_until(still_alive, time.monotonic() + cancel_budget)
    still_alive = [thread for thread in still_alive if thread.is_alive()]
    if not still_alive:
        return True

    self._force_terminate_for_redelivery(
        reason=(
            f"{len(still_alive)} consumer thread(s) remained active after "
            f"{drain_budget + cancel_budget:g}s shutdown budget"
        ),
        signal_number=self._shutdown_signal,
    )
    return False


def _queue_work_join_threads_until(
    threads: list[threading.Thread], deadline: float
) -> None:
    """Join multiple threads against one shared deadline."""
    for thread in threads:
        remaining = max(0.0, deadline - time.monotonic())
        with contextlib.suppress(ImportError, RuntimeError, AttributeError, OSError):
            thread.join(timeout=remaining)


def _queue_work_force_terminate_for_redelivery(
    *, reason: str, signal_number: int | None = None
) -> None:
    """Terminate without cleanup so the broker redelivers unacked work.

    Running resource hooks or closing pika connections from the main thread
    would race live consumers and turn shutdown into ordinary job errors.
    ``os._exit`` deliberately skips those callbacks; the kernel closes the
    process' sockets and open DB transactions, giving RabbitMQ/DB their
    native redelivery/rollback semantics.
    """
    Log.error("Worker forced shutdown: %s; unacked jobs will redeliver", reason)
    exit_code = (
        128 + int(signal_number)
        if signal_number is not None
        else getattr(os, "EX_TEMPFAIL", 75)
    )
    os._exit(exit_code)


def _queue_work_shutdown_drain_seconds() -> float:
    """Graceful-shutdown drain budget (seconds) for in-flight jobs.

    Configurable via ``queue.shutdown_drain_seconds``. Defaults GENEROUS
    (120s) so a normal long-running batch job finishes cleanly on
    SIGTERM instead of being killed mid-transaction at the old flat 10s cap,
    while still bounding how long a wedged poison-thread can delay a deploy.
    Operators can raise it (long batch jobs) or lower it (fast-only workers).
    """
    try:
        return max(0.0, float(config("queue.shutdown_drain_seconds", 120.0)))
    except TypeError, ValueError:
        return 120.0


def _queue_work_shutdown_cancel_seconds() -> float:
    """Grace after async cancellation before forced process exit."""
    try:
        return max(0.0, float(config("queue.shutdown_cancel_seconds", 5.0)))
    except TypeError, ValueError:
        return 5.0


def _queue_work_run_worker_startup_hooks() -> None:
    """Run app-declared worker-startup hooks.

    Hooks live in the APP (``config/queue.py::WORKER_STARTUP_HOOKS``) as
    dotted paths to sync, non-blocking module-level callables (they should
    spawn their own background threads). Keeping them in config means the
    framework worker holds no app/domain startup logic (e.g. a metrics
    sampler that queries product tables).
    """

    try:
        hooks = config("queue.worker_startup_hooks", []) or []
    except Exception:
        hooks = []
    for path in hooks:
        try:
            module_path, attr = path.rsplit(".", 1)
            fn = getattr(importlib.import_module(module_path), attr)
            fn()
        except Exception as exc:
            Log.warning("worker startup hook %s failed: %s", path, exc)


def _queue_work_shutdown_worker_resources(self) -> bool:
    """Run app-declared worker-shutdown hooks (release pooled resources).

    Hooks live in the APP (``config/queue.py::WORKER_SHUTDOWN_HOOKS``) as
    dotted paths to callables (sync or async); coroutine results are
    awaited. Domain teardown — browser pools, fetch drivers that leak OS
    handles (semaphores / Playwright children) on abrupt exit — registers
    here, so the framework worker holds no app/domain teardown logic.
    """

    active_consumers = [
        thread for thread in getattr(self, "_consumer_threads", []) if thread.is_alive()
    ]
    if active_consumers:
        Log.warning(
            "Worker resource shutdown deferred: %s consumer thread(s) are still active",
            len(active_consumers),
        )
        return False

    resource_lock = getattr(self, "_resource_shutdown_lock", None)
    if resource_lock is None:
        resource_lock = threading.Lock()
        self._resource_shutdown_lock = resource_lock

    with resource_lock:
        if getattr(self, "_worker_resources_shutdown", False):
            return True
        # Mark before invoking hooks so atexit and the normal finally path
        # cannot race the same browser/executor shutdown twice.
        self._worker_resources_shutdown = True

    try:
        hooks = config("queue.worker_shutdown_hooks", []) or []
    except Exception:
        hooks = []
    if not hooks:
        return True

    async def _close_all() -> None:
        for path in hooks:
            try:
                module_path, attr = path.rsplit(".", 1)
                fn = getattr(importlib.import_module(module_path), attr)
                result = fn()
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                Log.debug("[QueueWorkCommand] shutdown hook %s skipped: %s", path, exc)

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(_close_all())
        else:
            loop.run_until_complete(_close_all())
    except RuntimeError:
        asyncio.run(_close_all())
    return True


def _queue_work_should_stop(self, config: dict[str, Any]) -> bool:
    """Check if worker should stop due to configured limits.

    ``--max-jobs`` is a *terminal-attempt* cap, not a *successful-job*
    cap. Under a failure storm (poison-message stream, DB outage,
    misconfigured retention, etc.) every dequeue increments
    ``jobs_failed`` while ``jobs_processed`` stays at 0 — and the
    cap was never tripped, so the worker drained an unbounded
    number of jobs into the DLQ before --max-time eventually
    kicked in. Counting both completed and failed terminal
    attempts gives operators the safety bound they expect when
    load-testing or running short triage workers.
    """
    terminal_jobs = self.jobs_processed + self.jobs_failed
    max_jobs = config.get("max_jobs")
    if max_jobs and terminal_jobs >= max_jobs:
        self.info(
            f"🎯 Reached maximum job limit ({max_jobs}) "
            f"[processed={self.jobs_processed} failed={self.jobs_failed}]"
        )
        return True

    max_time = config.get("max_time")
    if max_time and (time.time() - self.start_time) >= max_time:
        self.info(f"⏰ Reached maximum runtime ({max_time} seconds)")
        return True

    return False


def _queue_work_get_runtime(self) -> str:
    """Get formatted runtime duration."""
    if not self.start_time:
        return "00:00:00"

    runtime_seconds = int(time.time() - self.start_time)
    hours, remainder = divmod(runtime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _queue_work_show_final_stats(self):
    """Show enhanced worker statistics with job status breakdown."""
    total_jobs = self.jobs_processed + self.jobs_failed
    success_rate = (self.jobs_processed / total_jobs * 100) if total_jobs > 0 else 100.0

    self.info("\n📊 Final Worker Statistics:")
    self.info(f"   Runtime: {self._get_runtime()}")
    self.info(f"   Jobs Processed: {self.jobs_processed}")
    self.info(f"   Jobs Failed: {self.jobs_failed}")
    self.info(f"   Success Rate: {success_rate:.1f}%")

    # Show enhanced queue stats if available
    try:
        # Try to resolve Job model from container (framework agnostic)
        job_model = self._resolve_job_model()
        if job_model and hasattr(job_model, "get_queue_stats"):
            queue_display = getattr(self, "_queue_names_display", "default")
            stats = job_model.get_queue_stats(queue_display)
            self.info(f"\n📈 Current Queue Status ({queue_display}):")
            self.info(f"   Pending: {stats.get('pending_jobs', 0)}")
            self.info(f"   Processing: {stats.get('processing_jobs', 0)}")
            self.info(f"   Completed: {stats.get('completed_jobs', 0)}")
            self.info(f"   Cancelled: {stats.get('cancelled_jobs', 0)}")
            self.info(f"   Failed: {stats.get('failed_jobs', 0)}")
    except Exception as exc:
        _logger.debug("enhanced stats unavailable: %s", exc)


def _queue_work_resolve_job_model(self):
    """Resolve Job model from JobTracker."""

    if hasattr(builtins, "app"):
        app_instance = builtins.app()
        if app_instance and app_instance.has("JobTracker"):
            tracker = app_instance.make("JobTracker")
            return getattr(tracker, "job_model", None)
    return None


def _queue_work_run_main_loop(self, *args, **kwargs):
    """Main worker loop - called by MakesAutoReload on restart."""
    # Use stored parameters from store_restart_params
    if hasattr(self, "_restart_params") and self._restart_params:
        driver, queue, timeout, max_jobs, max_time = self._restart_params
    else:
        driver, queue, timeout, max_jobs, max_time = (
            args if args else (None, None, None, None, None)
        )

    # Prepare config with current parameters
    try:
        worker_config = self._prepare_config(driver, queue, timeout, max_jobs, max_time)
    except Exception as e:
        self.error(f"❌ Configuration error: {e}")
        raise

    # Show worker configuration
    self._show_config(worker_config)

    # Clean up connections before starting
    self._cleanup_connections_for_restart()

    # Reset counters for fresh start
    self.jobs_processed = 0
    self.jobs_failed = 0

    queue_driver = Queue.driver(worker_config["driver_name"])
    queue_driver.verify_runtime_health(
        worker_config["queue_names"],
        force=True,
    )

    _Metrics.queue_worker_configured_queues.set(len(worker_config["queue_names"]))
    _Metrics.queue_worker_ready.set(0)

    # Run the worker
    self._run_worker(worker_config)


def _queue_work_cleanup_connections_for_restart(self):
    """Clean up connections specifically for restart - simple and effective."""
    try:
        # Simple approach: Just clear all references without trying to close broken connections
        drivers = config("queue.drivers", {})
        for driver_name in drivers:
            try:
                driver = Queue.driver(driver_name)

                # Just clear references - don't try to close broken connections
                if hasattr(driver, "channel"):
                    driver.channel = None

                if hasattr(driver, "connection"):
                    driver.connection = None

                # Reset driver state
                if hasattr(driver, "_connected"):
                    driver._connected = False

            except Exception as exc:
                _logger.warning(
                    "Could not reset queue driver %s during worker restart: %s",
                    driver_name,
                    exc,
                )
                continue

        # Force a small delay to let any pending operations complete

        time.sleep(0.1)

    except (ImportError, RuntimeError, AttributeError, OSError) as exc:
        _logger.warning("Queue driver restart cleanup failed: %s", exc)


def _queue_work_cleanup_watching(self):
    """Cleanup file watching resources."""
    if hasattr(self, "command_watcher") and self.command_watcher:
        with contextlib.suppress(ImportError, RuntimeError, AttributeError, OSError):
            self.command_watcher.shutdown()

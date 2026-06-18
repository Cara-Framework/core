"""APScheduler Driver for the Cara framework.

Always uses BackgroundScheduler internally so the calling command
retains control of the main thread (required for auto-reload, graceful
shutdown, health-check endpoints, etc.).

Job registration is decoupled from scheduler lifecycle: jobs are stored
in a local registry and (re-)applied to the underlying APScheduler
instance on every ``start()`` call. This means ``shutdown()`` +
``start()`` never loses registered jobs — the exact bug that caused the
scheduler to sit idle for hours.
"""

from __future__ import annotations

import inspect
import logging
import time as _time
from collections.abc import Callable, Iterable
from typing import Any

from cara.exceptions import InvalidArgumentException
from cara.facades import Log
from cara.scheduling.contracts import Scheduling

_logger = logging.getLogger("cara.scheduling")

# ── Prometheus instrumentation wrapper ────────────────────────────────


def _instrument_scheduled(identifier: str, callback: Callable) -> Callable:
    """Wrap a scheduled callback with Prometheus counter + histogram."""
    try:
        from app.support.Metrics import Metrics as _M
    except ImportError:
        _M = None  # type: ignore[assignment]

    def _observe(outcome: str, duration: float) -> None:
        if _M is None:
            return
        try:
            _M.scheduled_tasks_total.labels(task=identifier, outcome=outcome).inc()
            _M.scheduled_task_duration_seconds.labels(task=identifier).observe(duration)
        except (AttributeError, TypeError):
            pass

    if inspect.iscoroutinefunction(callback):

        async def _async_wrapped(*a, **kw):
            start = _time.time()
            try:
                result = await callback(*a, **kw)
                _observe("success", _time.time() - start)
                return result
            except Exception:
                _observe("failure", _time.time() - start)
                raise

        _async_wrapped.__name__ = getattr(callback, "__name__", f"scheduled_{identifier}")
        return _async_wrapped

    def _sync_wrapped(*a, **kw):
        start = _time.time()
        try:
            result = callback(*a, **kw)
            _observe("success", _time.time() - start)
            return result
        except Exception:
            _observe("failure", _time.time() - start)
            raise

    _sync_wrapped.__name__ = getattr(callback, "__name__", f"scheduled_{identifier}")
    return _sync_wrapped


def _wrap_without_overlapping(
    identifier: str, callback: Callable, lock_timeout: int
) -> Callable:
    """Wrap ``callback`` so concurrent fires across processes are serialized.

    APScheduler's own ``max_instances=1`` only guards same-process re-entry;
    a second pod (or a second worker that booted the scheduler) sees a
    fresh in-memory state and happily fires the same job at the same
    wall-clock minute. The ``Cache.add`` primitive in Cara is atomic
    (Redis ``SET NX`` underneath) so it gives us a distributed mutex
    without pulling in a new dependency.

    On contention the run is *skipped*, not queued — matching Laravel's
    ``withoutOverlapping`` semantics. The lock TTL caps tail damage if the
    holder crashes before releasing.
    """
    from cara.facades import Cache as _Cache

    lock_key = f"scheduler:lock:{identifier}"

    if inspect.iscoroutinefunction(callback):

        async def _async_locked(*a, **kw):
            if not _Cache.add(lock_key, "1", lock_timeout):
                Log.info("Scheduled job '%s' skipped — previous run still in flight.", identifier, category='scheduler')
                return None
            try:
                return await callback(*a, **kw)
            finally:
                try:
                    _Cache.forget(lock_key)
                except Exception:
                    _logger.debug(
                        "lock release failed for %s", identifier, exc_info=True
                    )

        _async_locked.__name__ = getattr(callback, "__name__", f"locked_{identifier}")
        return _async_locked

    def _sync_locked(*a, **kw):
        if not _Cache.add(lock_key, "1", lock_timeout):
            Log.info("Scheduled job '%s' skipped — previous run still in flight.", identifier, category='scheduler')
            return None
        try:
            return callback(*a, **kw)
        finally:
            try:
                _Cache.forget(lock_key)
            except Exception:
                _logger.debug(
                    "lock release failed for %s", identifier, exc_info=True
                )

    _sync_locked.__name__ = getattr(callback, "__name__", f"locked_{identifier}")
    return _sync_locked


# ── Error listener ────────────────────────────────────────────────────


def _job_error_listener(event) -> None:
    """Forward APScheduler job errors to Cara's Log facade."""
    if event.exception:
        Log.error("Scheduled job '%s' failed: %s", event.job_id, event.exception, category='scheduler')
        if event.traceback:
            Log.debug(
                "Scheduled job '%s' traceback:\n%s",
                event.job_id,
                event.traceback,
                category="scheduler",
            )
    else:
        Log.debug("Scheduled job '%s' executed successfully.", event.job_id, category='scheduler')


# ── Driver ────────────────────────────────────────────────────────────


class APSchedulerDriver(Scheduling):
    """APScheduler driver — always BackgroundScheduler, auto-reload safe."""

    driver_name = "apscheduler"

    def __init__(self, settings: dict[str, Any] | None = None) -> None:
        settings = settings or {}
        self._settings = settings
        self._configure_logging()

        # Registry: list of (identifier, instrumented_callback, trigger, job_opts)
        # Survives scheduler restarts — jobs are re-applied on start().
        self._job_registry: list[tuple[str, Callable, Any, dict]] = []

        self.scheduler = self._create_scheduler()

    # ── Scheduler creation ────────────────────────────────────────────

    def _configure_logging(self) -> None:
        import logging

        # Let warnings through so misfire notices are visible.
        logging.getLogger("apscheduler").setLevel(logging.WARNING)
        # Executor errors are forwarded via our event listener, so keep
        # APScheduler's own executor logger quiet.
        logging.getLogger("apscheduler.executors").setLevel(logging.ERROR)

    def _create_scheduler(self):
        from apscheduler.schedulers.background import BackgroundScheduler

        kwargs: dict[str, Any] = {}
        for key in ("jobstores", "executors", "job_defaults", "timezone"):
            if key in self._settings:
                kwargs[key] = self._settings[key]

        # Generous misfire window: if the scheduler was down (restart,
        # deploy) for up to 5 min, fire the job immediately on return
        # rather than silently skipping it.
        kwargs.setdefault("job_defaults", {})
        kwargs["job_defaults"].setdefault("misfire_grace_time", 300)
        kwargs["job_defaults"].setdefault("coalesce", True)

        scheduler = BackgroundScheduler(**kwargs)

        # Wire error listener so job failures appear in Cara logs.
        try:
            from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

            scheduler.add_listener(
                _job_error_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR
            )
        except ImportError:
            pass

        return scheduler

    # ── Job registration ──────────────────────────────────────────────

    def schedule_job(
        self,
        identifier: str,
        callback: Callable,
        schedule_spec: dict[str, Any],
        options: dict[str, Any],
    ) -> None:
        silent = bool(options.get("silent", False))

        trigger = self._build_trigger(schedule_spec)
        instrumented = _instrument_scheduled(identifier, callback)
        if isinstance(options, dict) and options.get("without_overlapping"):
            # Lock TTL falls back to one day — matches the
            # ``ScheduleBuilder.without_overlapping`` default and prevents a
            # crashed holder from blocking the slot forever.
            lock_timeout = int(options.get("lock_timeout", 86400))
            instrumented = _wrap_without_overlapping(
                identifier, instrumented, lock_timeout
            )
        job_opts = (
            options.get("apscheduler_job_options", {})
            if isinstance(options, dict)
            else {}
        )

        # Remove any previous entry with the same id.
        self._job_registry = [
            (jid, cb, tr, jo)
            for jid, cb, tr, jo in self._job_registry
            if jid != identifier
        ]
        self._job_registry.append((identifier, instrumented, trigger, job_opts))

        # If the scheduler is already running, hot-add the job.
        if self.scheduler.running:
            try:
                self.scheduler.remove_job(job_id=identifier)
            except Exception:
                _logger.debug("pre-add job removal skipped", exc_info=True)
            try:
                self.scheduler.add_job(
                    func=instrumented,
                    trigger=trigger,
                    id=identifier,
                    **job_opts,
                )
            except Exception as e:
                if not silent:
                    Log.error("Failed to schedule job '%s': %s", identifier, e)
                raise

        if not silent:
            Log.info("Registered scheduled job '%s'.", identifier)

    # ── Lifecycle ─────────────────────────────────────────────────────

    def start(self) -> None:
        """Start (or restart) the scheduler with all registered jobs."""
        # Tear down a previously running instance.
        if self.scheduler.running:
            Log.info("Scheduler running — restarting with fresh instance.")
            try:
                self.scheduler.shutdown(wait=False)
            except Exception:
                _logger.warning(
                    "scheduler shutdown failed during restart", exc_info=True
                )
            self.scheduler = self._create_scheduler()

        # Apply every registered job to the fresh scheduler.
        for identifier, callback, trigger, job_opts in self._job_registry:
            try:
                self.scheduler.add_job(
                    func=callback,
                    trigger=trigger,
                    id=identifier,
                    **job_opts,
                )
            except Exception as e:
                Log.warning("Failed to add job '%s' on start: %s", identifier, e)

        job_count = len(self._job_registry)
        self.scheduler.start()
        Log.info("APScheduler started with %s jobs (background mode).", job_count)

    def shutdown(self, wait: bool = True) -> None:
        try:
            if self.scheduler.running:
                self.scheduler.shutdown(wait=wait)
                Log.info("APScheduler shut down.")
        except Exception as e:
            Log.warning("Error shutting down APScheduler: %s", e)

    def remove_job(self, identifier: str) -> None:
        self._job_registry = [
            (jid, cb, tr, jo)
            for jid, cb, tr, jo in self._job_registry
            if jid != identifier
        ]
        try:
            self.scheduler.remove_job(job_id=identifier)
        except Exception:
            _logger.warning(
                "job removal failed: %s", identifier, exc_info=True
            )

    def list_jobs(self) -> Iterable[Any]:
        try:
            return self.scheduler.get_jobs()
        except Exception:
            _logger.warning("list_jobs failed, returning empty", exc_info=True)
            return []

    # ── Trigger builders ──────────────────────────────────────────────

    def _build_trigger(self, spec: dict[str, Any]):
        sched_type = spec.get("type")
        if not sched_type:
            raise InvalidArgumentException("schedule_spec must include 'type' key.")

        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.triggers.date import DateTrigger
        from apscheduler.triggers.interval import IntervalTrigger

        # Fall back to the scheduler-configured timezone (e.g. "UTC") when a
        # job spec doesn't set its own. Pre-fix this was bare
        # ``spec.get("timezone")`` → ``None`` for every dict-config cron, and
        # APScheduler resolves ``timezone=None`` to the LOCAL OS timezone at
        # construction, NOT the scheduler default — so on a non-UTC host every
        # "daily at 03:00" cron fired at 03:00 local while its SQL window used
        # pendulum.now("UTC"), shifting the job off its data window. (Masked in
        # prod only because the slim base image happens to be UTC.)
        tz = spec.get("timezone") or self._settings.get("timezone")

        if sched_type == "interval":
            return IntervalTrigger(
                seconds=spec.get("seconds", 0),
                minutes=spec.get("minutes", 0),
                hours=spec.get("hours", 0),
                days=spec.get("days", 0),
                weeks=spec.get("weeks", 0),
                timezone=tz,
            )
        elif sched_type == "cron":
            expr = spec.get("expression", "")
            if not isinstance(expr, str) or not expr.strip():
                raise InvalidArgumentException("Cron type requires 'expression' string.")
            parts = expr.split()
            if len(parts) != 5:
                raise InvalidArgumentException(
                    "Cron expression must have 5 fields: minute hour day month day_of_week"
                )
            minute, hour, day, month, day_of_week = parts
            return CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                timezone=tz,
            )
        elif sched_type == "daily":
            return CronTrigger(
                hour=spec.get("hour", 0),
                minute=spec.get("minute", 0),
                timezone=tz,
            )
        elif sched_type == "hourly":
            return CronTrigger(minute=spec.get("minute", 0), timezone=tz)
        elif sched_type == "weekly":
            return CronTrigger(
                minute=spec.get("minute", 0),
                hour=spec.get("hour", 0),
                day="*",
                month="*",
                day_of_week=spec.get("day_of_week"),
                timezone=tz,
            )
        elif sched_type in ("date", "at"):
            # "at" is the canonical name from the Scheduling contract and
            # ScheduleBuilder.at(); "date" mirrors APScheduler's own
            # trigger name.  Pre-fix only "date" was handled — every
            # ScheduleBuilder.at() call raised InvalidArgumentException
            # because the builder emits type="at" (matching the contract
            # docstring) but the driver only checked for "date".
            run_date = spec.get("run_date")
            if not run_date:
                raise InvalidArgumentException("Date/at type requires 'run_date'.")
            return DateTrigger(run_date=run_date, timezone=tz)
        else:
            raise InvalidArgumentException(f"Unknown schedule type: {sched_type}")

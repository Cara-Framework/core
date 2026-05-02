"""APScheduler Driver for the Cara framework.

Always uses BackgroundScheduler internally so the calling command
retains control of the main thread (required for auto-reload, graceful
shutdown, health-check endpoints, etc.). The ``mode`` setting in config
is kept for backward compatibility but has no effect — background mode
is the only sane option when the command layer manages its own loop.

Job registration is decoupled from scheduler lifecycle: jobs are stored
in a local registry and (re-)applied to the underlying APScheduler
instance on every ``start()`` call. This means ``shutdown()`` +
``start()`` never loses registered jobs — the exact bug that caused the
scheduler to sit idle for hours.
"""

import inspect
import time as _time
import traceback
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from cara.facades import Log
from cara.scheduling.contracts import Scheduling


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


# ── Error listener ────────────────────────────────────────────────────

def _job_error_listener(event) -> None:
    """Forward APScheduler job errors to Cara's Log facade."""
    if event.exception:
        Log.error(
            f"Scheduled job '{event.job_id}' failed: {event.exception}",
            category="scheduler",
        )
        if event.traceback:
            Log.debug(
                f"Scheduled job '{event.job_id}' traceback:\n{event.traceback}",
                category="scheduler",
            )
    else:
        Log.debug(
            f"Scheduled job '{event.job_id}' executed successfully.",
            category="scheduler",
        )


# ── Driver ────────────────────────────────────────────────────────────

class APSchedulerDriver(Scheduling):
    """APScheduler driver — always BackgroundScheduler, auto-reload safe."""

    driver_name = "apscheduler"

    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        settings = settings or {}
        self._settings = settings
        self._configure_logging()

        # Registry: list of (identifier, instrumented_callback, trigger, job_opts)
        # Survives scheduler restarts — jobs are re-applied on start().
        self._job_registry: List[Tuple[str, Callable, Any, Dict]] = []

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

        kwargs: Dict[str, Any] = {}
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
            scheduler.add_listener(_job_error_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        except ImportError:
            pass

        return scheduler

    # ── Job registration ──────────────────────────────────────────────

    def schedule_job(
        self,
        identifier: str,
        callback: Callable,
        schedule_spec: Dict[str, Any],
        options: Dict[str, Any],
    ) -> None:
        silent = bool(options.get("silent", False))

        trigger = self._build_trigger(schedule_spec)
        instrumented = _instrument_scheduled(identifier, callback)
        job_opts = options.get("apscheduler_job_options", {}) if isinstance(options, dict) else {}

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
                pass
            try:
                self.scheduler.add_job(
                    func=instrumented,
                    trigger=trigger,
                    id=identifier,
                    **job_opts,
                )
            except Exception as e:
                if not silent:
                    Log.error(f"Failed to schedule job '{identifier}': {e}")
                raise

        if not silent:
            Log.info(f"Registered scheduled job '{identifier}'.")

    # ── Lifecycle ─────────────────────────────────────────────────────

    def start(self) -> None:
        """Start (or restart) the scheduler with all registered jobs."""
        # Tear down a previously running instance.
        if self.scheduler.running:
            Log.info("Scheduler running — restarting with fresh instance.")
            try:
                self.scheduler.shutdown(wait=False)
            except Exception:
                pass
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
                Log.warning(f"Failed to add job '{identifier}' on start: {e}")

        job_count = len(self._job_registry)
        self.scheduler.start()
        Log.info(f"APScheduler started with {job_count} jobs (background mode).")

    def shutdown(self, wait: bool = True) -> None:
        try:
            if self.scheduler.running:
                self.scheduler.shutdown(wait=wait)
                Log.info("APScheduler shut down.")
        except Exception as e:
            Log.warning(f"Error shutting down APScheduler: {e}")

    def remove_job(self, identifier: str) -> None:
        self._job_registry = [
            (jid, cb, tr, jo)
            for jid, cb, tr, jo in self._job_registry
            if jid != identifier
        ]
        try:
            self.scheduler.remove_job(job_id=identifier)
        except Exception:
            pass

    def list_jobs(self) -> Iterable[Any]:
        try:
            return self.scheduler.get_jobs()
        except Exception:
            return []

    # ── Trigger builders ──────────────────────────────────────────────

    def _build_trigger(self, spec: Dict[str, Any]):
        sched_type = spec.get("type")
        if not sched_type:
            raise ValueError("schedule_spec must include 'type' key.")

        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.triggers.date import DateTrigger
        from apscheduler.triggers.interval import IntervalTrigger

        tz = spec.get("timezone")

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
                raise ValueError("Cron type requires 'expression' string.")
            parts = expr.split()
            if len(parts) != 5:
                raise ValueError("Cron expression must have 5 fields: minute hour day month day_of_week")
            minute, hour, day, month, day_of_week = parts
            return CronTrigger(
                minute=minute, hour=hour, day=day,
                month=month, day_of_week=day_of_week, timezone=tz,
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
                day="*", month="*",
                day_of_week=spec.get("day_of_week"),
                timezone=tz,
            )
        elif sched_type == "date":
            run_date = spec.get("run_date")
            if not run_date:
                raise ValueError("Date type requires 'run_date'.")
            return DateTrigger(run_date=run_date, timezone=tz)
        else:
            raise ValueError(f"Unknown schedule type: {sched_type}")

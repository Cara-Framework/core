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

import contextlib
import inspect
import logging
import os
import time as _time
import uuid as _uuid
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

from cara.exceptions import InvalidArgumentException
from cara.facades import Cache as _Cache
from cara.facades import Log
from cara.observability import MetricsBase
from cara.scheduling.contracts import SchedulingContract

_logger = logging.getLogger("cara.scheduling")

# ── Prometheus instrumentation wrapper ────────────────────────────────


def _instrument_scheduled(identifier: str, callback: Callable) -> Callable:
    """Wrap a scheduled callback with Prometheus counter + histogram."""
    try:
        metrics = MetricsBase
    except ImportError:
        metrics = None

    def _observe(outcome: str, duration: float) -> None:
        if metrics is None:
            return
        try:
            metrics.scheduled_tasks_total.labels(task=identifier, outcome=outcome).inc()
            metrics.scheduled_task_duration_seconds.labels(task=identifier).observe(
                duration
            )
            metrics.scheduled_task_last_run_timestamp_seconds.labels(task=identifier).set(
                _time.time()
            )
        except AttributeError, TypeError:
            pass

    def _mark_tick() -> None:
        if metrics is None:
            return
        with contextlib.suppress(AttributeError, TypeError):
            metrics.scheduler_last_tick_timestamp_seconds.set(_time.time())

    if inspect.iscoroutinefunction(callback):

        async def _async_wrapped(*a, **kw):
            _mark_tick()
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
        _mark_tick()
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

    OWNER-FENCED: each fire acquires with a UNIQUE ``{pid}:{uuid}`` owner token
    (``add``, atomic SET-NX) and releases with ``forget_if(key, owner)`` (atomic
    compare-and-delete). The previous ``add("1")`` + bare ``forget(key)`` had no
    owner fence, so a run that overran ``lock_timeout`` (the TTL then lapsed and
    a peer/pod re-acquired) would delete the PEER's freshly-acquired lock on its
    ``finally`` — letting a third fire start while two were already running. The
    token makes release delete ONLY our own still-held lock.
    """

    lock_key = f"scheduler:lock:{identifier}"

    def _new_owner() -> str:
        return f"{os.getpid()}:{_uuid.uuid4().hex}"

    def _release(owner: str) -> None:
        try:
            _Cache.forget_if(lock_key, owner)
        except Exception:
            _logger.debug("lock release failed for %s", identifier, exc_info=True)

    if inspect.iscoroutinefunction(callback):

        async def _async_locked(*a, **kw):
            owner = _new_owner()
            if not _Cache.add(lock_key, owner, lock_timeout):
                Log.info(
                    "Scheduled job '%s' skipped — previous run still in flight.",
                    identifier,
                    category="scheduler",
                )
                return None
            try:
                return await callback(*a, **kw)
            finally:
                _release(owner)

        _async_locked.__name__ = getattr(callback, "__name__", f"locked_{identifier}")
        return _async_locked

    def _sync_locked(*a, **kw):
        owner = _new_owner()
        if not _Cache.add(lock_key, owner, lock_timeout):
            Log.info(
                "Scheduled job '%s' skipped — previous run still in flight.",
                identifier,
                category="scheduler",
            )
            return None
        try:
            return callback(*a, **kw)
        finally:
            _release(owner)

    _sync_locked.__name__ = getattr(callback, "__name__", f"locked_{identifier}")
    return _sync_locked


# ── Catch-up memory ───────────────────────────────────────────────────

# Long enough to remember the previous fire of any calendar cadence a
# schedule can express (yearly crons included); a forgotten stamp only means
# that one boot cannot catch up.
_CATCH_UP_MEMORY_SECONDS = 366 * 86400


def _last_fire_key(identifier: str) -> str:
    return f"scheduler:last_fire:{identifier}"


def _wrap_catch_up(identifier: str, callback: Callable) -> Callable:
    """Remember when ``callback`` last fired, outside this process.

    The job store lives in memory, so a restart computes every next run from
    boot time: a cron fire that fell inside the restart is never run, and an
    interval re-anchors to boot. The stamp is the fire's START time, written
    once the run returns or raises — a process killed mid-run leaves the
    previous stamp, so the next boot runs the interrupted fire again.
    Catch-up is opt-in because it re-runs work late; entries whose run is
    idempotent and whose lateness beats absence declare it.
    """

    def _record(fired_at: float) -> None:
        try:
            _Cache.put(_last_fire_key(identifier), fired_at, _CATCH_UP_MEMORY_SECONDS)
        except Exception:
            _logger.warning("catch-up stamp failed for %s", identifier, exc_info=True)

    if inspect.iscoroutinefunction(callback):

        async def _async_remembered(*a, **kw):
            fired_at = _time.time()
            try:
                result = await callback(*a, **kw)
            except Exception:
                _record(fired_at)
                raise
            _record(fired_at)
            return result

        _async_remembered.__name__ = getattr(
            callback, "__name__", f"remembered_{identifier}"
        )
        return _async_remembered

    def _sync_remembered(*a, **kw):
        fired_at = _time.time()
        try:
            result = callback(*a, **kw)
        except Exception:
            _record(fired_at)
            raise
        _record(fired_at)
        return result

    _sync_remembered.__name__ = getattr(callback, "__name__", f"remembered_{identifier}")
    return _sync_remembered


def _catch_up_run_time(identifier: str, trigger: Any) -> datetime | None:
    """The first run a restart must honour, from the remembered last fire.

    ``None`` when nothing is remembered (the engine's own next run stands).
    A fire that fell due while the scheduler was down runs NOW; otherwise
    the remembered cadence is kept, so an interval does not re-anchor to
    boot and a scheduler restarted more often than its interval still runs.
    """
    try:
        stamp = _Cache.get(_last_fire_key(identifier))
    except Exception:
        _logger.warning("catch-up stamp unreadable for %s", identifier, exc_info=True)
        return None
    if stamp is None:
        return None
    try:
        last_fire = datetime.fromtimestamp(float(stamp), tz=UTC)
    except TypeError, ValueError, OverflowError:
        return None
    now = datetime.now(UTC)
    due = trigger.get_next_fire_time(last_fire, now)
    if due is None:
        return None
    if due <= now:
        Log.warning(
            "Scheduled job '%s' missed its %s run while the scheduler was "
            "down; running it now.",
            identifier,
            due.isoformat(),
            category="scheduler",
        )
        return now
    return due


# ── Error listener ────────────────────────────────────────────────────


def _job_error_listener(event) -> None:
    """Forward APScheduler job errors to Cara's Log facade."""
    if event.exception:
        Log.error(
            "Scheduled job '%s' failed: %s",
            event.job_id,
            event.exception,
            category="scheduler",
        )
        if event.traceback:
            Log.debug(
                "Scheduled job '%s' traceback:\n%s",
                event.job_id,
                event.traceback,
                category="scheduler",
            )
    else:
        Log.debug(
            "Scheduled job '%s' executed successfully.",
            event.job_id,
            category="scheduler",
        )


def _count_dropped_fire(event, outcome: str) -> None:
    with contextlib.suppress(AttributeError, TypeError):
        MetricsBase.scheduled_tasks_total.labels(
            task=event.job_id,
            outcome=outcome,
        ).inc()


def _job_missed_listener(event) -> None:
    """Report a run the engine dropped after waiting past its grace time.

    A run that waits for a free executor thread longer than
    ``misfire_grace_time`` never reaches the callback, so the
    instrumentation wrapper never sees it — without this listener a dropped
    daily run leaves no log line, no metric and no alert.
    """
    Log.warning(
        "Scheduled job '%s' missed its %s run (waited past the misfire grace "
        "time for an executor thread).",
        event.job_id,
        getattr(event, "scheduled_run_time", None),
        category="scheduler",
    )
    _count_dropped_fire(event, "missed")


def _job_max_instances_listener(event) -> None:
    """Count a run skipped because its previous instance is still executing.

    The engine already logs the skip at WARNING; this adds the metric.
    """
    _count_dropped_fire(event, "skipped")


# ── Driver ────────────────────────────────────────────────────────────


class APSchedulerDriver(SchedulingContract):
    """APScheduler driver — always BackgroundScheduler, auto-reload safe."""

    driver_name = "apscheduler"

    def __init__(self, settings: dict[str, Any] | None = None) -> None:
        settings = settings or {}
        self._settings = settings
        self._configure_logging()

        # Registry: list of (identifier, instrumented_callback, trigger, job_opts)
        # Survives scheduler restarts — jobs are re-applied on start().
        self._job_registry: list[tuple[str, Callable, Any, dict]] = []
        # Ids whose first run after a (re)start honours the remembered last
        # fire — see ``_wrap_catch_up``.
        self._catch_up_ids: set[str] = set()

        # Opaque per-entry metadata for the published snapshot, keyed by job
        # id. Kept OUTSIDE the APScheduler job (its Job objects carry no
        # arbitrary attributes) and pruned in ``remove_job`` so a long-lived
        # scheduler that churns jobs cannot leak the map.
        self._snapshot_meta: dict[str, dict] = {}

        self.scheduler = self._create_scheduler()

    # ── Scheduler creation ────────────────────────────────────────────

    def _configure_logging(self) -> None:

        # Let the engine's own warnings (max-instances skips) through.
        logging.getLogger("apscheduler").setLevel(logging.WARNING)
        # Job errors and misfires are forwarded — with a metric — by our
        # event listeners, so keep APScheduler's own executor logger quiet.
        logging.getLogger("apscheduler.executors").setLevel(logging.ERROR)

    def _create_scheduler(self):
        from apscheduler.schedulers.background import (
            BackgroundScheduler,  # local: heavy optional dep
        )

        kwargs: dict[str, Any] = {}
        for key in ("jobstores", "executors", "job_defaults", "timezone"):
            if key in self._settings:
                value = self._settings[key]
                # APScheduler pops ``type``/``class`` out of each plugin dict
                # while configuring; hand it copies so ``start()`` can build
                # a fresh scheduler from the same settings.
                if isinstance(value, dict):
                    value = {
                        name: dict(item) if isinstance(item, dict) else item
                        for name, item in value.items()
                    }
                kwargs[key] = value

        # A run that waited up to 5 min for a free executor thread still
        # fires; past that it is dropped as MISSED and counted by
        # ``_job_missed_listener``. This window does NOT cover downtime: the
        # job store is in memory, so a restart computes next runs from boot
        # and only ``catch_up`` entries recover a fire that fell inside it.
        kwargs.setdefault("job_defaults", {})
        kwargs["job_defaults"].setdefault("misfire_grace_time", 300)
        kwargs["job_defaults"].setdefault("coalesce", True)

        scheduler = BackgroundScheduler(**kwargs)

        # Wire error listener so job failures appear in Cara logs.
        try:
            from apscheduler.events import (  # local: heavy optional dep
                EVENT_JOB_ERROR,
                EVENT_JOB_EXECUTED,
                EVENT_JOB_MAX_INSTANCES,
                EVENT_JOB_MISSED,
            )

            scheduler.add_listener(
                _job_error_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR
            )
            scheduler.add_listener(_job_missed_listener, EVENT_JOB_MISSED)
            scheduler.add_listener(_job_max_instances_listener, EVENT_JOB_MAX_INSTANCES)
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
        catch_up = isinstance(options, dict) and bool(options.get("catch_up"))
        if catch_up:
            instrumented = _wrap_catch_up(identifier, instrumented)
        if isinstance(options, dict) and options.get("without_overlapping"):
            # Lock TTL falls back to one day — matches the
            # ``ScheduleBuilder.without_overlapping`` default and prevents a
            # crashed holder from blocking the slot forever.
            lock_timeout = int(options.get("lock_timeout", 86400))
            instrumented = _wrap_without_overlapping(
                identifier, instrumented, lock_timeout
            )
        job_opts = dict(
            options.get("apscheduler_job_options", {})
            if isinstance(options, dict)
            else {}
        )
        # The operator-facing label from the schedule config. Explicit
        # apscheduler_job_options win — this is a default, not an override.
        display_name = options.get("display_name") if isinstance(options, dict) else None
        if display_name:
            job_opts.setdefault("name", str(display_name))
        executor = options.get("executor") if isinstance(options, dict) else None
        if executor:
            job_opts.setdefault("executor", str(executor))

        # Opaque snapshot metadata rides options straight through to the
        # snapshot publisher. Re-registering an id replaces its metadata
        # (including clearing it) so a restart never resurrects a stale dict.
        meta = options.get("snapshot_meta") if isinstance(options, dict) else None
        if isinstance(meta, dict) and meta:
            self._snapshot_meta[identifier] = dict(meta)
        else:
            self._snapshot_meta.pop(identifier, None)

        # Remove any previous entry with the same id.
        self._job_registry = [
            (jid, cb, tr, jo)
            for jid, cb, tr, jo in self._job_registry
            if jid != identifier
        ]
        self._job_registry.append((identifier, instrumented, trigger, job_opts))
        if catch_up:
            self._catch_up_ids.add(identifier)
        else:
            self._catch_up_ids.discard(identifier)

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
                    **self._engine_job_options(identifier, trigger, job_opts),
                )
            except Exception as e:
                if not silent:
                    Log.error(
                        "Failed to schedule job '%s': %s", identifier, e, exc_info=True
                    )
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
                _logger.warning("scheduler shutdown failed during restart", exc_info=True)
            self.scheduler = self._create_scheduler()

        # Apply every registered job to the fresh scheduler.
        for identifier, callback, trigger, job_opts in self._job_registry:
            try:
                self.scheduler.add_job(
                    func=callback,
                    trigger=trigger,
                    id=identifier,
                    **self._engine_job_options(identifier, trigger, job_opts),
                )
            except Exception as e:
                Log.warning("Failed to add job '%s' on start: %s", identifier, e)

        job_count = len(self._job_registry)
        self.scheduler.start()
        Log.info("APScheduler started with %s jobs (background mode).", job_count)

    def _engine_job_options(self, identifier: str, trigger: Any, job_opts: dict) -> dict:
        """``job_opts`` plus the catch-up first run for an opted-in entry."""
        if identifier not in self._catch_up_ids or "next_run_time" in job_opts:
            return job_opts
        first_run = _catch_up_run_time(identifier, trigger)
        if first_run is None:
            return job_opts
        return {**job_opts, "next_run_time": first_run}

    def run_all(self) -> int:
        """Execute every registered job's callback immediately, once.

        Powers ``schedule:work --once`` — runs the callbacks inline
        instead of starting the background cron engine (which would
        never fire before the command exits).
        """
        executed = 0
        for identifier, callback, _trigger, _job_opts in list(self._job_registry):
            try:
                callback()
                executed += 1
            except Exception as e:
                Log.error(
                    "Job '%s' failed during run-once: %s",
                    identifier,
                    e,
                    exc_info=True,
                )
        return executed

    def shutdown(self, wait: bool = True) -> None:
        try:
            if self.scheduler.running:
                self.scheduler.shutdown(wait=wait)
                Log.info("APScheduler shut down.")
        except Exception as e:
            Log.warning("Error shutting down APScheduler: %s", e)

    def snapshot_meta(self, identifier: str) -> dict | None:
        """The opaque metadata registered for ``identifier``, or ``None``.

        Read by the snapshot publisher. Optional in the driver contract —
        the publisher probes for it with ``getattr`` so a driver that does
        not implement it stays valid.
        """
        return self._snapshot_meta.get(identifier)

    def remove_job(self, identifier: str) -> None:
        self._job_registry = [
            (jid, cb, tr, jo)
            for jid, cb, tr, jo in self._job_registry
            if jid != identifier
        ]
        self._snapshot_meta.pop(identifier, None)
        self._catch_up_ids.discard(identifier)
        try:
            self.scheduler.remove_job(job_id=identifier)
        except Exception:
            _logger.warning("job removal failed: %s", identifier, exc_info=True)

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

        from apscheduler.triggers.cron import CronTrigger  # local: heavy optional dep
        from apscheduler.triggers.date import DateTrigger  # local: heavy optional dep
        from apscheduler.triggers.interval import (
            IntervalTrigger,  # local: heavy optional dep
        )

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

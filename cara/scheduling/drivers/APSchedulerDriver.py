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

import asyncio
import contextlib
import inspect
import logging
import os
import threading
import time as _time
import uuid as _uuid
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

from cara.broadcasting import drained
from cara.exceptions import InvalidArgumentException
from cara.facades import Cache as _Cache
from cara.facades import Log
from cara.observability import MetricsBase
from cara.queues import ConcurrencyExceeded, JobThrottledException
from cara.scheduling.contracts import SchedulingContract

from . import _ScheduleTriggers

_logger = logging.getLogger("cara.scheduling")

#: Every ``outcome`` a fire records on ``scheduled_tasks_total``.
_OUTCOMES = ("success", "failure", "throttled", "missed", "skipped")

# A failed overlap-lock release is retried this many times, a short pause
# apart, before its token is left for the next fire to release: a cache blip
# usually clears within a fraction of a second.
_RELEASE_ATTEMPTS = 3
_RELEASE_RETRY_DELAY_SECONDS = 0.2

# ── Prometheus instrumentation wrapper ────────────────────────────────


def _zero_outcome_series(identifier: str) -> None:
    """Export ``identifier``'s outcome series at 0 from registration:
    ``increase()`` cannot see a counter's first sample, so a series born at 1
    after a restart hid a task's first success from ``unless … success``
    rules (live 2026-11-01, ``ScheduledTaskFailingContinuously``)."""
    with contextlib.suppress(AttributeError, TypeError):
        for outcome in _OUTCOMES:
            MetricsBase.scheduled_tasks_total.labels(task=identifier, outcome=outcome)


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

    def _sync_wrapped(*a, **kw):
        _mark_tick()
        start = _time.time()
        try:
            result = callback(*a, **kw)
            _observe("success", _time.time() - start)
            return result
        except ConcurrencyExceeded:
            # The concurrency gate refused the tick past its retry budget
            # (``_ScheduledThrottle``): a held slot, not a code failure — its
            # own outcome, so the throttle alert sees it and the failure-rate
            # rules do not double-count it (live 2026-11-01).
            _observe("throttled", _time.time() - start)
            raise
        except JobThrottledException:
            # The job's own gate refused the fire before its body ran (the
            # previous tick still holds the overlap lock, a cooldown, a rate
            # limit). Nothing ran, so this is the same "skipped" this driver
            # gives a fire its own overlap lock skips. The registration used
            # to swallow the refusal and the fire was counted a success.
            _count_skipped_fire(identifier)
            return None
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
    ``withoutOverlapping`` semantics — and counted as ``skipped``. The lock
    TTL caps tail damage if the holder crashes before releasing.

    OWNER-FENCED: each fire acquires with a UNIQUE ``{pid}:{uuid}`` owner token
    (``add``, atomic SET-NX) and releases with ``forget_if(key, owner)`` (atomic
    compare-and-delete). The previous ``add("1")`` + bare ``forget(key)`` had no
    owner fence, so a run that overran ``lock_timeout`` (the TTL then lapsed and
    a peer/pod re-acquired) would delete the PEER's freshly-acquired lock on its
    ``finally`` — letting a third fire start while two were already running. The
    token makes release delete ONLY our own still-held lock.

    A release that fails (a cache blip) is retried briefly, logged, and its
    token REMEMBERED: the next fire in this process first releases that
    token again — still compare-and-delete, so a lock a peer has since taken
    is never touched. Before, one failed release left the entry skipping
    every fire for the whole lock TTL (a day by default) with nothing logged
    above DEBUG.
    """

    lock_key = f"scheduler:lock:{identifier}"
    unreleased: set[str] = set()
    unreleased_guard = threading.Lock()

    def _new_owner() -> str:
        return f"{os.getpid()}:{_uuid.uuid4().hex}"

    def _release(owner: str, attempts: int) -> bool:
        for attempt in range(attempts):
            try:
                _Cache.forget_if(lock_key, owner)
                return True
            except Exception:
                if attempt + 1 < attempts:
                    _time.sleep(_RELEASE_RETRY_DELAY_SECONDS * (attempt + 1))
                    continue
                Log.warning(
                    "Scheduled job '%s' could not release its overlap lock; "
                    "the next fire retries the release.",
                    identifier,
                    category="scheduler",
                    exc_info=True,
                )
        return False

    def _release_leftovers() -> None:
        with unreleased_guard:
            leftovers = list(unreleased)
        for owner in leftovers:
            if _release(owner, attempts=1):
                with unreleased_guard:
                    unreleased.discard(owner)

    def _sync_locked(*a, **kw):
        _release_leftovers()
        owner = _new_owner()
        if not _Cache.add(lock_key, owner, lock_timeout):
            Log.info(
                "Scheduled job '%s' skipped — previous run still in flight.",
                identifier,
                category="scheduler",
            )
            _count_skipped_fire(identifier)
            return None
        try:
            return callback(*a, **kw)
        finally:
            if not _release(owner, attempts=_RELEASE_ATTEMPTS):
                with unreleased_guard:
                    unreleased.add(owner)

    _sync_locked.__name__ = getattr(callback, "__name__", f"locked_{identifier}")
    return _sync_locked


def _count_skipped_fire(identifier: str) -> None:
    with contextlib.suppress(AttributeError, TypeError):
        MetricsBase.scheduled_tasks_total.labels(task=identifier, outcome="skipped").inc()


def _run_to_completion(identifier: str, callback: Callable) -> Callable:
    """A sync callable that runs whatever ``callback`` returns to completion.

    The engine's executors call the job and keep what it returns: handed a
    coroutine function (or a sync wrapper returning a coroutine) they
    dropped the coroutine, so the body never ran while the fire was logged
    and counted as a success (``run_all`` too). Every job therefore reaches
    the engine sync; an awaitable result runs on its own loop in the
    executor thread, drained like a queue job's, and its exception
    propagates as the fire's failure.
    """

    def _awaited(*a, **kw):
        result = callback(*a, **kw)
        if inspect.isawaitable(result):
            return asyncio.run(drained(result))
        return result

    _awaited.__name__ = getattr(callback, "__name__", f"awaited_{identifier}")
    return _awaited


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

        # The outcome of the last ``run_all`` pass (``schedule:work --once``).
        self.last_run_failures = 0
        self.last_run_failed_ids: list[str] = []

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

        try:
            trigger = self._build_trigger(schedule_spec)
        except InvalidArgumentException as e:
            # Name the entry: a refusal at boot must say which one to fix.
            raise InvalidArgumentException(f"Scheduled job '{identifier}': {e}") from e
        _zero_outcome_series(identifier)
        instrumented = _instrument_scheduled(
            identifier, _run_to_completion(identifier, callback)
        )
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
        self._refuse_undeclared_executor(identifier, job_opts.get("executor"))

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

    def _refuse_undeclared_executor(self, identifier: str, executor: Any) -> None:
        """Refuse an entry whose executor pool the settings do not declare.

        The engine looks the executor up only when the job first falls due,
        then logs to its own logger and REMOVES the job: the entry was
        accepted, listed at boot, and never ran. ``default`` always exists —
        the engine creates it when the settings name none.
        """
        if not executor:
            return
        declared = {"default", *(self._settings.get("executors") or {})}
        if str(executor) not in declared:
            raise InvalidArgumentException(
                f"Scheduled job '{identifier}' names executor {str(executor)!r}, "
                f"which the scheduler settings do not declare "
                f"(declared: {', '.join(sorted(declared))})."
            )

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

        Returns the number that succeeded, as before. A failing task is
        logged and the pass carries on; how many failed (and which) is left
        on ``last_run_failures`` / ``last_run_failed_ids`` so the caller can
        exit non-zero without inferring failures from the success count.
        """
        executed = 0
        failed: list[str] = []
        for identifier, callback, _trigger, _job_opts in list(self._job_registry):
            try:
                callback()
                executed += 1
            except Exception as e:
                failed.append(identifier)
                Log.error(
                    "Job '%s' failed during run-once: %s",
                    identifier,
                    e,
                    exc_info=True,
                )
        self.last_run_failures = len(failed)
        self.last_run_failed_ids = failed
        return executed

    def shutdown(self, wait: bool = True) -> None:
        try:
            if self.scheduler.running:
                self.scheduler.shutdown(wait=wait)
                Log.info("APScheduler shut down.")
        except Exception as e:
            Log.warning("Error shutting down APScheduler: %s", e)
        # A shut-down APScheduler keeps its executors, whose thread pools are
        # closed for good: a later ``start()`` on the same instance (the
        # schedule:work hot reload) "starts" but every fire then dies with
        # "cannot schedule new futures after shutdown". Swap in a fresh,
        # never-started instance so a restart really runs the jobs.
        self.scheduler = self._create_scheduler()

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

    # Spec → engine trigger (and the crontab weekday translation) lives in
    # ``_ScheduleTriggers``; bound here so it stays this driver's method.
    _build_trigger = _ScheduleTriggers._build_trigger

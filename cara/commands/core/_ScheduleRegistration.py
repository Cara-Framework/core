"""Registration of dictionary-defined scheduler entries."""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import inspect
from typing import Any

from cara.broadcasting import drained
from cara.commands.ProcessReload import import_when_settled
from cara.configuration import config
from cara.context import Tenancy
from cara.exceptions import ConfigurationException
from cara.facades import Log, Schedule
from cara.queues import Bus, JobThrottledException
from cara.queues.middleware import run_through_middleware_async
from cara.scheduling import declared_interval_seconds, on_wall_clock

from . import _ScheduledThrottle
from .ScheduledJobRunner import instantiate_scheduled_job, scheduled_handle_kwargs


def _register_scheduled_dict_job(self, spec: dict[str, Any]) -> dict[str, Any] | None:
    """Register a job defined as a dict in config/scheduling.py.

    Expected keys:
        job      – dotted import path  e.g. "app.jobs.FooJob.FooJob"
        trigger  – "interval" | "cron"
        id       – unique job identifier
        name     – human label
        + interval fields: hours, minutes, seconds
          or hour/minute/day_of_week (cron)
        kwargs   – (optional) passed to handle()
        dispatch – (optional) True queues the job for a worker instead of
                   running it inline in the scheduler
    """

    # Every entry is registered on the wall clock (``cara.scheduling.WallClock``):
    # an interval the clock divides becomes its cron, so a scheduler restart
    # no longer pushes the next fire a whole interval out.
    spec = on_wall_clock(spec)

    job_path: str = spec.get("job", "")
    module_path, _, class_name = job_path.rpartition(".")
    if not module_path or not class_name:
        self.warning(f"⚠️  Invalid job path: '{job_path}'")
        return None

    # A reloading scheduler imports the job from a tree an edit may have
    # saved only half-way: wait for it to land (``import_when_settled``)
    # rather than drop the job until the next reload. Without auto-reload a
    # broken import is a broken deploy and fails at once.
    if getattr(self, "_auto_reload_enabled", False):
        mod = import_when_settled(module_path)
    else:
        mod = importlib.import_module(module_path)
    job_cls = getattr(mod, class_name)
    is_central_job = bool(getattr(job_cls, "central_job", False))
    scheduled_tenant_id = spec.get("tenant_id")
    if is_central_job:
        if scheduled_tenant_id is not None:
            raise ConfigurationException(
                f"Central scheduled job {job_path} cannot declare tenant_id."
            )
    elif scheduled_tenant_id is None:
        raise ConfigurationException(
            f"Ordinary scheduled job {job_path} requires explicit tenant_id."
        )

    job_id = spec.get("id", class_name)
    job_name = spec.get("name", class_name)
    trigger = spec.get("trigger", "interval")
    job_kwargs = spec.get("kwargs", {})

    # Build the callable — instantiate with kwargs (covers jobs whose
    # __init__ requires parameters like ``source``), then call handle().
    # If handle() needs DI-resolved arguments (contracts), resolve them
    # through the container; if handle() is a coroutine, run it via
    # asyncio so async jobs work transparently.
    # The DECLARED period, kept on an entry ``on_wall_clock`` moved to a cron:
    # the throttle budget is half of it either way.
    interval_seconds = declared_interval_seconds(spec)

    def _make_and_run(
        _cls=job_cls,
        _kw=job_kwargs,
        _app=self.application,
        _central=is_central_job,
        _tenant_id=scheduled_tenant_id,
        _identifier=job_id,
        _budget=_ScheduledThrottle._throttle_budget_seconds(interval_seconds),
        _dispatch=bool(spec.get("dispatch")),
    ):

        # Instantiation, the idempotency result-cache opt-out and handle()'s
        # arguments are shared with every hand-run of a schedule entry
        # (``ScheduledJobRunner``), so a manual run cannot drift from a tick.
        instance = instantiate_scheduled_job(_app, _cls, _kw)
        if _dispatch:
            # ``"dispatch": True``: the tick only QUEUES the job and a worker
            # runs it on its own lane, under its kill timer and middleware.
            # Every tick runs inline in ``schedule:work`` otherwise, holding a
            # pool thread (and a reload) for its whole run — a long or
            # remote-calling tick made the others miss their fire.
            async def _queue_scoped():
                scope = Tenancy.central() if _central else Tenancy.as_tenant(_tenant_id)
                with scope:
                    await Bus.dispatch(instance)

            asyncio.run(drained(_queue_scoped()))
            return
        handle_method = getattr(instance, "handle", None)
        if handle_method is None:
            return
        handle_kwargs = scheduled_handle_kwargs(_app, handle_method, _kw)

        # OPT-IN scheduler-tick observability (default OFF). The scheduler
        # runs jobs INLINE via handle() — bypassing Bus/driver — so a
        # scheduled tick normally leaves NO ``job`` row (only the child
        # jobs it dispatches get tracked). That invisibility is by design,
        # but it makes "did my scheduled sweep actually run?" un-queryable.
        # Flip ``SCHEDULER_TRACK_TICKS=true`` to record one row per fire
        # (pending → processing → completed/failed) so scheduled runs sit
        # alongside dispatched jobs. Kept OFF by default because high-
        # cadence timers (e.g. 30s flushes) would otherwise write thousands
        # of tick rows/day into the very table retention is bounding.
        # Tracking is fully guarded — a tracker failure NEVER affects the
        # actual job run.
        tracker = None
        db_job_id = None
        if config("scheduling.track_ticks", False):
            try:
                if _app is not None and _app.has("JobTracker"):
                    tracker = _app.make("JobTracker")
                    db_job_id = tracker.create_job_record(
                        job_name=_cls.__name__,
                        job_class=f"{_cls.__module__}.{_cls.__name__}",
                        queue="scheduler",
                        execution_mode="scheduler",
                        metadata={"scheduled_tick": True, "schedule_id": job_id},
                    )
                    if db_job_id is not None:
                        tracker.update_job_status(db_job_id, "processing")
            except Exception:  # tracking never breaks the run
                tracker = None
                db_job_id = None

        def _finish_tick(status: str, _t=tracker, _id=db_job_id) -> None:
            if _t is not None and _id is not None:
                # Tracking must never break the tick itself.
                with contextlib.suppress(Exception):
                    _t.update_job_status(_id, status)

        async def _invoke(_job):
            result = handle_method(**handle_kwargs)
            if inspect.isawaitable(result):
                return await result
            return result

        async def _run_scoped():

            scope = Tenancy.central() if _central else Tenancy.as_tenant(_tenant_id)
            with scope:
                return await run_through_middleware_async(instance, _invoke)

        try:
            # A concurrency-gate refusal is retried within the tick: inline
            # there is no queue runner to requeue it (live 2026-11-01, 15
            # Amazon fires lost in an hour).
            _ScheduledThrottle._run_with_throttle_retry(
                _identifier,
                lambda: asyncio.run(drained(_run_scoped())),
                budget_seconds=_budget,
            )
        except JobThrottledException as refused:
            # The job's own gate (``WithoutOverlapping`` still held by the
            # previous tick, a cooldown, a rate limit) refused the fire before
            # its body ran. That is the driver's "skipped" — the previous run
            # is still going — not a failure: the tick row was written
            # ``failed`` and the failure-rate alerts counted it. The refusal
            # is handed on so the driver counts the fire once, as skipped.
            _skip_tick(_identifier, tracker, db_job_id, refused)
            raise
        except BaseException:
            _finish_tick("failed")
            raise
        else:
            _finish_tick("completed")

    builder = Schedule.call(_make_and_run)
    builder.identifier = job_id
    # ``display_name`` rides options into the driver, which forwards it
    # as APScheduler's ``name`` — otherwise every job introspects as the
    # wrapper's function name and the published snapshot (plus
    # APScheduler's own logs) reads ``_sync_wrapped`` 58 times.
    builder.options.update({"silent": True, "display_name": job_name})

    # Opaque per-entry metadata for the published snapshot. Declaring it
    # on the schedule entry is what keeps a reader process from needing a
    # second, application-invented cache key (and the duplicated constant
    # that comes with it) to learn anything beyond the next run time.
    snapshot_meta = spec.get("snapshot_meta")
    if isinstance(snapshot_meta, dict) and snapshot_meta:
        builder.options["snapshot_meta"] = dict(snapshot_meta)

    # ROOT-CAUSE: pre-fix the dict-job registration silently dropped
    # any ``without_overlapping`` flag in the spec, so every entry in
    # ``config/scheduling.py`` ran without scheduler-level overlap
    # protection — a 30 s interval whose body takes 45 s cascaded; a
    # multi-pod deploy fired the same cron tick on every pod in
    # parallel. The flag must be applied BEFORE the terminal
    # ``builder.interval()`` / ``.daily()`` / ``.cron()`` call
    # because those methods dispatch ``options`` to the driver
    # immediately. Default stays False so existing entries that
    # rely on overlap (or that implement their own internal
    # ``Cache.add`` fence) keep their current behaviour.
    # ``lock_timeout`` mirrors the
    # ``APSchedulerDriver._wrap_without_overlapping`` default of
    # 86400 s (1 day) so a crashed holder can't wedge the slot
    # forever — same TTL the rest of the lock surface uses.
    if spec.get("without_overlapping"):
        builder.without_overlapping(
            timeout=int(spec.get("lock_timeout", 86400)),
        )
    # Same before-the-terminal-call rule: ``catch_up`` recovers a fire that
    # fell inside a scheduler restart, ``executor`` moves the entry onto a
    # named pool declared in the driver settings.
    if spec.get("catch_up"):
        builder.catch_up()
    if spec.get("executor"):
        builder.executor(spec["executor"])

    if trigger == "interval":
        # ``days`` / ``weeks`` ride through with the rest. Dropping them left a
        # ``{"days": 2}`` entry with a period of ZERO — which APScheduler ran
        # every second, and which the driver now refuses at registration.
        builder.interval(
            seconds=spec.get("seconds", 0),
            minutes=spec.get("minutes", 0),
            hours=spec.get("hours", 0),
            days=spec.get("days", 0),
            weeks=spec.get("weeks", 0),
        )
        parts = []
        if spec.get("weeks"):
            parts.append(f"{spec['weeks']}w")
        if spec.get("days"):
            parts.append(f"{spec['days']}d")
        if spec.get("hours"):
            parts.append(f"{spec['hours']}h")
        if spec.get("minutes"):
            parts.append(f"{spec['minutes']}m")
        if spec.get("seconds"):
            parts.append(f"{spec['seconds']}s")
        schedule_desc = f"Every {' '.join(parts)}" if parts else "interval"
    elif trigger == "cron":
        cron_kw = {}
        for k in ("hour", "minute", "day_of_week"):
            if k in spec:
                cron_kw[k] = spec[k]
        # Forward per-job timezone override so dict-config entries
        # can pin a timezone just like spec-config entries can.
        if spec.get("timezone"):
            builder.timezone(spec["timezone"])
        # APScheduler cron via expression
        # Build a cron expression or use daily/hourly helpers
        if "day_of_week" in cron_kw:
            builder.cron(
                f"{cron_kw.get('minute', 0)} {cron_kw.get('hour', 0)} * * {cron_kw['day_of_week']}"
            )
        else:
            builder.daily(
                hour=cron_kw.get("hour", 0),
                minute=cron_kw.get("minute", 0),
            )
        schedule_desc = f"Cron {cron_kw}"
    else:
        self.warning(f"⚠️  Unknown trigger '{trigger}' for {job_name}")
        return None

    return {
        "name": job_name,
        "id": job_id,
        "type": "dict",
        "schedule": schedule_desc,
    }


def _skip_tick(identifier: str, tracker: Any, db_job_id: Any, refused: Exception) -> None:
    """Record a fire the job's own gate refused as SKIPPED, never failed.

    The driver counts the fire (``outcome="skipped"`` on
    ``scheduled_tasks_total``, as for a fire its own overlap lock skipped)
    when the refusal reaches it; this writes the tick's row. A tracked tick row
    is called off (``cancelled``, reason kept in metadata) — the tracker's
    terminal word for an inline run a gate refused, since ``failed`` would
    read as an incident and ``processing`` as a run stuck forever.
    """
    Log.info(
        "Scheduled job '%s' skipped — %s",
        identifier,
        refused,
        category="scheduler",
    )
    if tracker is not None and db_job_id is not None:
        # Tracking must never break the tick itself.
        with contextlib.suppress(Exception):
            tracker.track_job_throttled(db_job_id, f"skipped: {refused}")

"""
Schedule Worker Command for the Cara framework.

This module provides a CLI command to process scheduled jobs with enhanced UX.
"""

from __future__ import annotations

import contextlib
import time
import traceback
import uuid
from typing import Any

from cara.commands import CommandBase
from cara.commands.MakesAutoReload import MakesAutoReload
from cara.configuration import config
from cara.decorators import command
from cara.exceptions import (
    CaraException,
    ConfigurationException,
    InvalidArgumentException,
)
from cara.facades import Log, Queue, Schedule
from cara.scheduling.contracts import ShouldSchedule


@command(
    name="schedule:work",
    help="Run the schedule worker to register and execute scheduled tasks.",
    options={
        "--driver=?": "Scheduler driver to use (overrides default)",
        "--once": "Run scheduled tasks once and exit",
        "--stats": "Show scheduling statistics",
        "--reload": "Enable auto-reload on file changes",
    },
)
class ScheduleWorkCommand(MakesAutoReload, CommandBase):
    """Run schedule worker with enhanced monitoring and task registration."""

    def __init__(self, application=None):
        super().__init__(application)
        self.start_time = None

    def handle(self, driver: str | None = None):
        """Handle schedule worker execution with enhanced monitoring."""
        self.console.print()  # Empty line for spacing
        self.console.print("[bold #e5c07b]╭─ Schedule Worker ─╮[/bold #e5c07b]")
        self.console.print()

        # Store parameters for restart
        self.store_restart_params(driver)

        # Stand up /metrics on a side-thread HTTP server so Prometheus can
        # scrape the SCHEDULER process too — its tick/dispatch counters live
        # in this process and are invisible from the worker's exporter. The
        # port is ``metrics.scheduler_port`` (NOT ``metrics.port``: worker and
        # scheduler share one host, so they must not race for one socket).
        # Opt out with a port of 0.
        try:
            from cara.observability import start_http_server as _start_metrics

            _port = _start_metrics(port=int(config("metrics.scheduler_port", 0)))
            if _port:
                Log.info("📈 Metrics server on :%s/metrics", _port)
        except Exception as e:
            # Non-fatal: scheduler keeps running with no metrics exposure.
            Log.warning("metrics server startup failed: %s", e)

        # Setup auto-reload if enabled (default: true for development)
        if self.option("reload") or config("app.debug", True):
            self.enable_auto_reload()

        # Start main scheduler loop
        try:
            self._run_main_loop(driver)
        except Exception as e:
            import traceback

            self.error(f"× Scheduler error: {e}")
            self.error(f"× Stack trace: {traceback.format_exc()}")
        finally:
            self.cleanup_auto_reload()
            self._show_final_stats()

    def _run_main_loop(self, *args, **kwargs):
        """Main scheduler loop - called by MakesAutoReload on restart."""
        # Use stored parameters from store_restart_params
        if hasattr(self, "_restart_params") and self._restart_params:
            driver = self._restart_params[0] if self._restart_params else None
        else:
            driver = args[0] if args else None

        # Prepare configuration
        try:
            scheduler_config = self._prepare_config(driver)
        except Exception as e:
            self.error(f"× Configuration error: {e}")
            return

        # Show scheduler configuration
        self._show_config(scheduler_config)

        # Register and run scheduled jobs
        try:
            job_entries = self._register_jobs()
            if not job_entries:
                self.warning("⚠️  No scheduled jobs found to register")
                return

            self._show_jobs(job_entries)
            self._start_scheduler(scheduler_config)

        except KeyboardInterrupt:
            self.info("\n⏸️  Schedule worker stopped by user")
        except Exception as e:
            self.error(f"× Scheduler error: {e}")
            if config("app.debug", False):
                self.error(f"Stack trace: {traceback.format_exc()}")

    def _prepare_config(self, driver: str | None) -> dict[str, Any]:
        """Prepare and validate scheduler configuration."""
        driver_name = driver or config("scheduling.default")
        if not driver_name:
            raise ConfigurationException("No scheduler driver specified and no default configured")

        return {
            "driver_name": driver_name,
            "run_once": self.option("once"),
            "show_stats": self.option("stats"),
            "debug": config("app.debug", False),
        }

    def _show_config(self, scheduler_config: dict[str, Any]):
        """Display scheduler configuration in ServeCommand style."""
        self.console.print("[bold #e5c07b]┌─ Configuration[/bold #e5c07b]")

        # Driver info
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Driver:[/white] [bold white]{scheduler_config['driver_name'].upper()}[/bold white]"
        )

        # Run mode
        run_mode = "Once" if scheduler_config["run_once"] else "Continuous"
        mode_color = "#e5c07b" if scheduler_config["run_once"] else "#30e047"
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Run Mode:[/white] [{mode_color}]{run_mode}[/{mode_color}]"
        )

        # Statistics
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Statistics:[/white] [{'#30e047' if scheduler_config['show_stats'] else '#E21102'}]{'✓' if scheduler_config['show_stats'] else '×'}[/{'#30e047' if scheduler_config['show_stats'] else '#E21102'}]"
        )

        # Auto-reload status (default: enabled in development)
        from cara.configuration import config as global_config

        auto_reload = self.option("reload") or global_config("app.debug", True)
        self.console.print(
            f"[#e5c07b]│[/#e5c07b] [white]Auto-reload:[/white] [{'#30e047' if auto_reload else '#E21102'}]{'✓' if auto_reload else '×'}[/{'#30e047' if auto_reload else '#E21102'}]"
        )

        self.console.print("[#e5c07b]└─[/#e5c07b]")
        self.console.print()

    def _register_jobs(self) -> list[dict[str, Any]]:
        """Register all scheduled jobs and return summary.

        Each ``_register_dict_job`` / ``_register_spec_job`` call may
        return ``None`` silently — e.g. when ``ScheduleBuilder`` raises
        a non-``Exception`` during the daily/cron chain, or when
        APScheduler's internal ``add_job`` swallows a conflicting id.
        Pre-fix the only signal of those silent drops was the
        ``Status: N jobs`` banner being lower than the dict count, and
        operators almost never noticed (a job could sit un-fired for
        weeks). The post-loop reconciliation logs WARNING for every
        config entry whose ``id`` didn't land in the registration set,
        so silent drops surface at boot.
        """
        jobs = config("scheduling.jobs", []) or []
        if not jobs:
            return []

        self.info("📋 Registering scheduled jobs...")
        job_entries = []
        # ``expected_dict_ids`` tracks every dict-config entry's id so
        # the post-loop reconciliation can diff registered vs. expected.
        # Decorator / ShouldSchedule entries don't carry an operator-
        # facing id so they're excluded.
        expected_dict_ids: list[tuple[str, str]] = []

        for job_target in jobs:
            try:
                # ── Dict-based config (config-file style) ───────────
                if isinstance(job_target, dict):
                    expected_dict_ids.append(
                        (
                            str(job_target.get("id") or "?"),
                            str(job_target.get("name") or job_target.get("job") or "?"),
                        )
                    )
                    entry = self._register_dict_job(job_target)
                    if entry:
                        job_entries.append(entry)
                    continue

                job_name = getattr(job_target, "__name__", str(job_target))

                # Handle ShouldSchedule interface
                if isinstance(job_target, type) and issubclass(
                    job_target, ShouldSchedule
                ):
                    job_target.schedule(Schedule)
                    job_entries.append(
                        {
                            "name": job_name,
                            "id": "self-scheduled",
                            "type": "ShouldSchedule",
                            "schedule": "See schedule() method",
                        }
                    )
                    continue

                # Handle decorator-based scheduling
                specs = getattr(job_target, "_schedule_specs", None)
                if specs:
                    for spec in specs:
                        entry = self._register_spec_job(job_target, job_name, spec)
                        if entry:
                            job_entries.append(entry)
                else:
                    self.warning(f"⚠️  No schedule metadata found for '{job_name}'")

            except Exception as e:
                job_name = (
                    getattr(job_target, "__name__", str(job_target))
                    if not isinstance(job_target, dict)
                    else job_target.get("name", job_target.get("job", "?"))
                )
                self.warning(f"⚠️  Failed to register job '{job_name}': {e}")

        # ── Reconciliation: which dict-config ids never produced an
        # entry? ``_register_dict_job`` returning ``None`` (or any of
        # the downstream builder calls swallowing a failure) ends up
        # here. Surface each one at WARNING level with the id + name
        # so an operator scanning boot logs sees exactly what was
        # silently dropped.
        registered_ids = {str(e.get("id")) for e in job_entries if e.get("id")}
        for spec_id, spec_name in expected_dict_ids:
            if spec_id == "?" or spec_id in registered_ids:
                continue
            self.warning(
                f"⚠️  Scheduled job '{spec_id}' ({spec_name}) was in "
                f"config but never registered — check the cron/interval "
                f"spec, the dotted import path, or APScheduler id "
                f"conflicts. This job will NOT fire."
            )

        return job_entries

    def _register_dict_job(self, spec: dict[str, Any]) -> dict[str, Any] | None:
        """Register a job defined as a dict in config/scheduling.py.

        Expected keys:
            job      – dotted import path  e.g. "app.jobs.FooJob.FooJob"
            trigger  – "interval" | "cron"
            id       – unique job identifier
            name     – human label
            + trigger-specific keys: hours/minutes/seconds (interval)
              or hour/minute/day_of_week (cron)
            kwargs   – (optional) passed to handle()
        """
        import importlib

        job_path: str = spec.get("job", "")
        module_path, _, class_name = job_path.rpartition(".")
        if not module_path or not class_name:
            self.warning(f"⚠️  Invalid job path: '{job_path}'")
            return None

        mod = importlib.import_module(module_path)
        job_cls = getattr(mod, class_name)

        job_id = spec.get("id", class_name)
        job_name = spec.get("name", class_name)
        trigger = spec.get("trigger", "interval")
        job_kwargs = spec.get("kwargs", {})

        # Build the callable — instantiate with kwargs (covers jobs whose
        # __init__ requires parameters like ``source``), then call handle().
        # If handle() needs DI-resolved arguments (contracts), resolve them
        # through the container; if handle() is a coroutine, run it via
        # asyncio so async jobs work transparently.
        def _make_and_run(_cls=job_cls, _kw=job_kwargs, _app=self.application):
            import asyncio
            import inspect

            # Instantiate — pass kwargs to __init__ so required params
            # like a scheduled job(source=...) are satisfied.
            try:
                instance = _cls(**_kw) if _kw else _app.make(_cls)
            except TypeError:
                # Fallback: kwargs don't match __init__ signature,
                # try DI container without kwargs.
                instance = _app.make(_cls)

            # Scheduled jobs run on a fixed cadence — the scheduler IS the
            # dedup authority. Opt them out of the 24h idempotency *result
            # cache*: a no-arg recurring job (e.g. one that runs every 30s)
            # hashes to one stable key, so the result cache would return
            # the first tick's cached result for a full day and the job would
            # effectively run once per 24h. That silently broke deferred
            # flush-style jobs, so freshly processed records never got their
            # follow-up work and stayed in their initial state (hidden from
            # the client). Overlap between slow ticks is still guarded by the
            # idempotency job lock + any WithoutOverlapping middleware.
            with contextlib.suppress(OSError, RuntimeError, AttributeError, ConnectionError):
                instance.idempotency_cache_results = False

            # Resolve handle() parameters via DI container if needed.
            handle_method = getattr(instance, "handle", None)
            if handle_method is None:
                return

            sig = inspect.signature(handle_method)
            handle_kwargs = {}
            for param_name, param in sig.parameters.items():
                if param_name in _kw:
                    handle_kwargs[param_name] = _kw[param_name]
                elif param.annotation != inspect.Parameter.empty:
                    with contextlib.suppress(OSError, RuntimeError, AttributeError, ConnectionError):
                        handle_kwargs[param_name] = _app.make(param.annotation)

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
                        db_job_id = tracker.create_sync_job_record(
                            job_name=_cls.__name__,
                            job_class=f"{_cls.__module__}.{_cls.__name__}",
                            queue="scheduler",
                            metadata={"scheduled_tick": True, "schedule_id": job_id},
                        )
                        if db_job_id is not None:
                            tracker.update_job_status(db_job_id, "processing")
                except Exception:  # noqa: BLE001 — tracking never breaks the run
                    tracker = None
                    db_job_id = None

            def _finish_tick(status: str, _t=tracker, _id=db_job_id) -> None:
                if _t is not None and _id is not None:
                    # Tracking must never break the tick itself.
                    with contextlib.suppress(Exception):
                        _t.update_job_status(_id, status)

            try:
                result = handle_method(**handle_kwargs)
                # Support async handle() methods
                if inspect.isawaitable(result):
                    loop = asyncio.new_event_loop()
                    try:
                        loop.run_until_complete(result)
                    finally:
                        loop.close()
            except BaseException:
                _finish_tick("failed")
                raise
            else:
                _finish_tick("completed")

        builder = Schedule.call(_make_and_run)
        builder.identifier = job_id
        builder.options.update({"silent": True})

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

        if trigger == "interval":
            builder.interval(
                seconds=spec.get("seconds", 0),
                minutes=spec.get("minutes", 0),
                hours=spec.get("hours", 0),
            )
            parts = []
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

    def _register_spec_job(
        self, job_target: Any, job_name: str, spec: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Register a job from schedule specification."""
        mode = spec["mode"]
        job_id = spec.get("identifier") or f"{job_name}_{uuid.uuid4().hex[:8]}"

        # Create schedule builder
        if mode == "call":
            builder = Schedule.call(job_target)
        elif mode == "job":
            builder = Schedule.call(lambda: self.application.make(job_target).handle())
        elif mode == "command":
            driver_name = spec.get("driver_name")
            builder = Schedule.call(lambda: self._queue_command(job_target, driver_name))
        else:
            self.warning(f"⚠️  Unsupported schedule mode '{mode}' for {job_name}")
            return None

        # Configure builder
        builder.identifier = job_id
        builder.options.update({"silent": True})

        # Apply schedule based on type
        schedule_type = spec["type"]
        args = spec.get("args", ())
        kwargs = dict(spec.get("kwargs", {}))

        if spec.get("timezone"):
            kwargs["timezone"] = spec["timezone"]

        try:
            if schedule_type == "cron":
                builder.cron(*args, **kwargs)
            elif schedule_type == "daily":
                builder.daily(**kwargs)
            elif schedule_type == "hourly":
                builder.hourly(**kwargs)
            elif schedule_type == "interval":
                builder.interval(**kwargs)
            elif schedule_type == "at":
                builder.at(*args)
            elif schedule_type == "weekly":
                day = args[0]
                builder.weekly(
                    day,
                    hour=kwargs.get("hour", 0),
                    minute=kwargs.get("minute", 0),
                    timezone=kwargs.get("timezone"),
                )
            else:
                raise InvalidArgumentException(f"Unknown schedule type '{schedule_type}'")

            return {
                "name": job_name,
                "id": job_id,
                "type": mode,
                "schedule": self._describe_schedule(spec),
            }

        except Exception as e:
            raise CaraException(f"Failed to configure schedule: {e}") from e

    def _queue_command(self, command_target: Any, driver_name: str | None = None):
        """Queue command for execution."""
        instance = (
            self.application.make(command_target)
            if isinstance(command_target, type)
            else command_target
        )

        if driver_name:
            Queue.push(instance, driver_name=driver_name)
        else:
            Queue.push(instance)

    def _describe_schedule(self, spec: dict[str, Any]) -> str:
        """Create human-readable schedule description."""
        schedule_type = spec["type"]
        args = spec.get("args", ())
        kwargs = spec.get("kwargs", {})
        tz = kwargs.get("timezone", "")
        tz_str = f" ({tz})" if tz else ""

        if schedule_type == "cron":
            expr = args[0] if args else kwargs.get("expression", "")
            return f"Cron: {expr}{tz_str}"
        elif schedule_type == "daily":
            hour = kwargs.get("hour", 0)
            minute = kwargs.get("minute", 0)
            return f"Daily at {hour:02d}:{minute:02d}{tz_str}"
        elif schedule_type == "hourly":
            minute = kwargs.get("minute", 0)
            return f"Hourly at :{minute:02d}{tz_str}"
        elif schedule_type == "interval":
            parts = []
            if kwargs.get("hours"):
                parts.append(f"{kwargs['hours']}h")
            if kwargs.get("minutes"):
                parts.append(f"{kwargs['minutes']}m")
            if kwargs.get("seconds"):
                parts.append(f"{kwargs['seconds']}s")
            interval = " ".join(parts) if parts else "0s"
            return f"Every {interval}{tz_str}"
        elif schedule_type == "weekly":
            day = args[0] if args else "?"
            hour = kwargs.get("hour", 0)
            minute = kwargs.get("minute", 0)
            return f"Weekly {day} at {hour:02d}:{minute:02d}{tz_str}"
        else:
            return schedule_type

    def _show_jobs(self, job_entries: list[dict[str, Any]]):
        """Display registered jobs in ServeCommand style."""
        self.console.print("[bold #e5c07b]┌─ Scheduled Jobs[/bold #e5c07b]")

        for i, job in enumerate(job_entries[:5], 1):  # Show first 5
            job_type_color = "#30e047" if job["type"] == "command" else "#e5c07b"
            self.console.print(
                f"[#e5c07b]│[/#e5c07b]   [white]{i}.[/white] [{job_type_color}]{job['name']}[/{job_type_color}] [dim]({job['schedule']})[/dim]"
            )

        if len(job_entries) > 5:
            self.console.print(
                f"[#e5c07b]│[/#e5c07b]   [dim]... and {len(job_entries) - 5} more jobs[/dim]"
            )

        self.console.print("[#e5c07b]└─[/#e5c07b]")
        self.console.print()

    def _show_scheduler_status(self):
        """Display scheduler status in ServeCommand style."""
        self.console.print("[bold #e5c07b]┌─ Scheduler Status[/bold #e5c07b]")
        self.console.print(
            "[#e5c07b]│[/#e5c07b] [white]Status:[/white] [#30e047]✓ Active - Processing scheduled tasks[/#30e047]"
        )
        self.console.print("[#e5c07b]└─[/#e5c07b]")
        self.console.print()

        # Simple ready message
        self.console.print("[dim]Press Ctrl+C to stop the scheduler[/dim]")
        self.console.print()

    def _start_scheduler(self, scheduler_config: dict[str, Any]):
        """Start the scheduler with the specified configuration."""
        self._show_scheduler_status()
        self.start_time = time.time()

        # Initialised before the try — if Schedule.driver() itself raises,
        # the finally must not trip over an unbound local and mask the
        # informative CaraException with an UnboundLocalError.
        driver = None
        try:
            driver = Schedule.driver(scheduler_config["driver_name"])

            if scheduler_config["run_once"]:
                # Execute every registered task inline, once. Starting the
                # background engine here (the old behavior) meant the
                # finally shut it down before any cron trigger could fire —
                # --once executed nothing.
                executed = driver.run_all()
                self.console.print(
                    f"[#30e047]Scheduled tasks executed once ({executed} succeeded)[/#30e047]"
                )
            else:
                # BackgroundScheduler: start() returns immediately, jobs
                # run in a thread pool. The while-loop below keeps the
                # command alive until Ctrl-C or auto-reload sets
                # shutdown_requested.
                driver.start()
                while not self.shutdown_requested:
                    time.sleep(1)

        except Exception as e:
            raise CaraException(f"Failed to start scheduler: {e}") from e
        finally:
            # Ensure the background scheduler stops its thread pool
            # when the command exits (Ctrl-C, auto-reload, --once).
            if driver is not None:
                with contextlib.suppress(OSError, RuntimeError, AttributeError, ConnectionError):
                    driver.shutdown(wait=False)

    def _show_final_stats(self):
        """Show final scheduler statistics."""
        if not hasattr(self, "start_time") or not self.start_time:
            return

        runtime_seconds = int(time.time() - self.start_time)
        hours, remainder = divmod(runtime_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        runtime = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        self.console.print()
        self.console.print("[bold #e5c07b]📊 Final Scheduler Statistics:[/bold #e5c07b]")
        self.console.print(f"   Runtime: {runtime}")
        self.console.print(f"   Tasks Executed: {getattr(self, 'tasks_executed', 0)}")
        self.console.print()

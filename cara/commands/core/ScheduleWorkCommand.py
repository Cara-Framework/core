"""
Schedule Worker Command for the Cara framework.

This module provides a CLI command to process scheduled jobs with enhanced UX.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import time
import traceback
import uuid
from datetime import UTC, datetime
from typing import Any

import cara.facades as facades
from cara.commands.CommandBase import CommandBase
from cara.commands.MakesAutoReload import MakesAutoReload
from cara.configuration import config
from cara.configuration import config as global_config
from cara.context import Tenancy
from cara.decorators import command
from cara.exceptions import (
    CaraException,
    ConfigurationException,
    InvalidArgumentException,
)
from cara.facades import Log, Queue, Schedule
from cara.observability import MetricsBase as _Metrics
from cara.observability import start_http_server as _start_metrics
from cara.queues.middleware import run_through_middleware_async
from cara.scheduling import (
    SCHEDULE_SNAPSHOT_CACHE_KEY,
    SCHEDULE_SNAPSHOT_EVERY_SECONDS,
    SCHEDULE_SNAPSHOT_TTL_SECONDS,
)
from cara.scheduling.contracts import ShouldSchedule

from . import _ScheduleRegistration


@command(
    name="schedule:work",
    help=(
        "SCHEDULER: fire scheduled tasks on their cadence.\n"
        "\n"
        "Third of the three background processes. Tasks that dispatch jobs "
        "still need `craft queue:relay` to publish them and `craft "
        "queue:work` to run them."
    ),
    options=[
        {
            "name": "--driver",
            "help": "Scheduler driver to use (overrides default)",
            "type": str,
        },
        {
            "name": "--once",
            "help": "Run scheduled tasks once and exit",
            "is_flag": True,
        },
        {
            "name": "--stats",
            "help": "Show scheduling statistics",
            "is_flag": True,
        },
        {
            "name": "--reload",
            "help": "Enable auto-reload on file changes",
            "is_flag": True,
        },
    ],
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
            _Metrics.scheduler_ready.set(0)
            _Metrics.scheduler_registered_tasks.set(0)
            _port = _start_metrics(
                port=int(config("metrics.scheduler_port", 0)),
                role="scheduler",
            )
            if _port:
                Log.info("📈 Metrics server on :%s/metrics", _port)
        except Exception as e:
            if str(config("app.env", "local")).lower() in {"production", "prod"}:
                raise CaraException("Scheduler metrics server failed to start") from e
            Log.warning("metrics server startup failed: %s", e)

        # Setup auto-reload if enabled (default: true for development)
        if self.option("reload") or config("app.debug", True):
            self.enable_auto_reload()

        # Start main scheduler loop
        try:
            self._run_main_loop(driver)
        except Exception as e:
            self.error(f"× Scheduler error: {e}")
            self.error(f"× Stack trace: {traceback.format_exc()}")
            raise
        finally:
            with contextlib.suppress(Exception):
                _Metrics.scheduler_ready.set(0)
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
            raise

        # Show scheduler configuration
        self._show_config(scheduler_config)

        # Register and run scheduled jobs
        try:
            job_entries = self._register_jobs()
            if not job_entries:
                raise ConfigurationException("No scheduled jobs were registered")

            self._show_jobs(job_entries)

            _Metrics.scheduler_registered_tasks.set(len(job_entries))
            _Metrics.scheduler_ready.set(1)
            self._start_scheduler(scheduler_config)

        except KeyboardInterrupt:
            self.info("\n⏸️  Schedule worker stopped by user")
        except Exception as e:
            self.error(f"× Scheduler error: {e}")
            if config("app.debug", False):
                self.error(f"Stack trace: {traceback.format_exc()}")
            raise

    def _prepare_config(self, driver: str | None) -> dict[str, Any]:
        """Prepare and validate scheduler configuration."""
        driver_name = driver or config("scheduling.default")
        if not driver_name:
            raise ConfigurationException(
                "No scheduler driver specified and no default configured"
            )

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
        missing_entries: list[str] = []
        for spec_id, spec_name in expected_dict_ids:
            if spec_id == "?" or spec_id in registered_ids:
                continue
            missing_entries.append(f"{spec_id} ({spec_name})")
            self.warning(
                f"⚠️  Scheduled job '{spec_id}' ({spec_name}) was in "
                f"config but never registered — check the cron/interval "
                f"spec, the dotted import path, or APScheduler id "
                f"conflicts. This job will NOT fire."
            )
        if missing_entries:
            raise ConfigurationException(
                "Configured scheduled jobs failed registration: "
                + ", ".join(missing_entries)
            )

        return job_entries

    _register_dict_job = _ScheduleRegistration._register_scheduled_dict_job

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
            if not bool(getattr(job_target, "central_job", False)):
                raise ConfigurationException(
                    f"Decorator-scheduled job {job_name} must explicitly declare "
                    "central_job = True; tenant jobs need dict scheduling with tenant_id."
                )
            builder = Schedule.call(lambda: self._run_decorated_central_job(job_target))
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

    def _run_decorated_central_job(self, job_target: Any) -> None:

        instance = self.application.make(job_target)
        handle_method = getattr(instance, "handle", None)
        if not callable(handle_method):
            raise ConfigurationException(
                f"Scheduled job {job_target.__name__} has no callable handle()."
            )

        async def _invoke(_job):
            result = handle_method()
            if inspect.isawaitable(result):
                return await result
            return result

        async def _run():
            with Tenancy.central():
                return await run_through_middleware_async(instance, _invoke)

        asyncio.run(_run())

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
                    scheduler = getattr(driver, "scheduler", None)
                    if scheduler is None or not bool(
                        getattr(scheduler, "running", False)
                    ):
                        _Metrics.scheduler_ready.set(0)
                        raise CaraException(
                            "Scheduler engine stopped while the process remained alive"
                        )
                    self._publish_schedule_snapshot(driver)
                    time.sleep(1)

        except Exception as e:
            raise CaraException(f"Failed to start scheduler: {e}") from e
        finally:
            # Ensure the background scheduler stops its thread pool
            # when the command exits (Ctrl-C, auto-reload, --once).
            if driver is not None:
                with contextlib.suppress(
                    OSError, RuntimeError, AttributeError, ConnectionError
                ):
                    driver.shutdown(wait=False)

    def _publish_schedule_snapshot(self, driver) -> None:
        """Publish the live schedule to the shared cache, rate-limited.

        Runs on the 1-second keep-alive tick, so it rate-limits itself
        instead of asking the loop to count. Publishing must never take the
        scheduler down — the schedule executing matters more than it being
        observable — so every failure is swallowed into a debug line.
        """
        now = time.time()
        if now - getattr(self, "_snapshot_at", 0.0) < SCHEDULE_SNAPSHOT_EVERY_SECONDS:
            return
        self._snapshot_at = now
        try:
            # Optional in the driver contract: a driver that does not carry
            # per-entry metadata stays valid and simply publishes no ``meta``.
            read_meta = getattr(driver, "snapshot_meta", None)

            jobs = []
            for job in driver.list_jobs():
                next_run = getattr(job, "next_run_time", None)
                job_id = str(getattr(job, "id", "") or "")
                entry = {
                    "id": job_id,
                    "name": str(getattr(job, "name", "") or getattr(job, "id", "")),
                    # None while a job is paused — readers must not
                    # invent a time for it.
                    "next_run_at": next_run.isoformat() if next_run else None,
                }
                # Metadata publishes for paused entries too: "when does this
                # run" being unknown does not make "how often is it meant to
                # run" unknown.
                meta = read_meta(job_id) if callable(read_meta) else None
                if meta:
                    entry["meta"] = meta
                jobs.append(entry)
            facades.Cache.put(
                SCHEDULE_SNAPSHOT_CACHE_KEY,
                json.dumps(
                    {
                        "published_at": datetime.now(UTC).isoformat(),
                        "jobs": jobs,
                    }
                ),
                SCHEDULE_SNAPSHOT_TTL_SECONDS,
            )
        except Exception as e:
            Log.debug(
                "schedule snapshot publish failed: %s", e, category="cara.scheduling"
            )

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

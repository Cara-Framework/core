"""Canonical ``QueueHooksCommand`` command."""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time

import pendulum

from cara.decorators import command
from cara.facades import Log
from cara.queues.delivery._DeliverySettlement import (
    _delivery_release_interrupted_terminal_hook,
    _delivery_settle_callbackless_terminal_hook,
)

from .QueueRelayCommand import _RelayLoop

#: How long a hook child stopped by shutdown gets between SIGTERM and SIGKILL.
#: Deliberately far inside the compose ``stop_grace_period``.
_CHILD_STOP_GRACE_SECONDS = 5

_CHILD_POLL_SECONDS = 0.05


def _run_child(
    argv: list[str],
    *,
    env: dict[str, str],
    timeout: int,
    stop: threading.Event,
) -> int | None:
    """Wait for one hook child; ``None`` means shutdown stopped it.

    ``subprocess.run`` cannot be interrupted, which is what held a SIGTERM
    for the rest of the batch and orphaned the child when the wait expired.
    """
    process = subprocess.Popen(argv, env=env)
    deadline = time.monotonic() + timeout
    while (returncode := process.poll()) is None:
        if stop.wait(_CHILD_POLL_SECONDS):
            process.terminate()
            try:
                process.wait(timeout=_CHILD_STOP_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            return None
        if time.monotonic() >= deadline:
            process.kill()
            process.wait()
            raise subprocess.TimeoutExpired(argv, timeout)
    return returncode


@command(
    name="queue:hooks",
    help=(
        "Process durable queue terminal-hook outbox rows.\n"
        "\n"
        "Sibling of `queue:relay` — same outbox-draining shape, different "
        "outbox. Handles post-completion hooks; it is not a substitute for "
        "`queue:relay` (job publication) or `queue:work` (job execution)."
    ),
    options=[
        {
            "name": "--once",
            "help": "Run one bounded hook iteration and exit.",
            "type": bool,
            "default": False,
            "is_flag": True,
        },
        {
            "name": "--poll-interval",
            "help": "Idle poll interval in seconds (default: 0.25).",
            "type": float,
            "default": None,
            "is_flag": False,
        },
    ],
)
class QueueHooksCommand(_RelayLoop):
    """Long-running terminal-hook outbox relay."""

    metric_name = "queue_hooks_ready"
    metrics_port_config = "hooks_port"
    operation_name = "queue-hooks"

    def _iteration_is_healthy(
        self,
        _driver,
        _result: dict[str, int],
    ) -> bool:
        # A child hook failure is a durable work-item outcome with its own
        # retry/quarantine metrics, not proof that the hook service cannot
        # access its DB or spawn children. Those systemic failures raise from
        # the callback and drive readiness to zero in the base loop.
        return True

    def _iteration_has_failures(self, result: dict[str, int]) -> bool:
        return any(int(result.get(key, 0) or 0) for key in ("failed", "quarantined"))

    def handle(
        self,
        once: bool = False,
        poll_interval: str | float | None = None,
    ) -> int:
        return self._handle_loop(
            once=once,
            poll_interval=poll_interval,
            callback=self._run_isolated_hooks,
        )

    @staticmethod
    def _run_isolated_hooks(driver, stop: threading.Event) -> dict[str, int]:
        job_ids = driver.due_terminal_hook_ids()
        result = {
            "claimed": len(job_ids),
            "completed": 0,
            "failed": 0,
            "deferred": 0,
            "quarantined": 0,
            "skipped": 0,
            "interrupted": 0,
        }
        store = driver.delivery_store
        timeout = int(store.hook_timeout_seconds) + 15
        child_env = dict(os.environ)
        child_env["METRICS_PORT"] = "0"

        def _record_child_failure(job_id: str, error: str) -> None:
            outcome = driver.defer_terminal_hook_process_failure(
                job_id,
                error=error,
            )
            if outcome == "completed":
                result["completed"] += 1
                return
            result["failed"] += 1
            if outcome == "quarantined":
                result["quarantined"] += 1
            else:
                result["deferred"] += 1

        # Rows with no callback go first: they cost milliseconds each and
        # must not wait behind the interpreter boots of the ones that do.
        needs_child: list[str] = []
        for job_id in job_ids:
            if stop.is_set():
                break
            outcome = _delivery_settle_callbackless_terminal_hook(store, job_id)
            if outcome == "isolate":
                needs_child.append(job_id)
            else:
                result["completed" if outcome == "completed" else "skipped"] += 1

        for job_id in needs_child:
            if stop.is_set():
                break
            # One interpreter boot per row makes a full batch last minutes,
            # and a gauge refreshed only when it ends froze at the burst's
            # peak: 202 pending for five minutes of a backlog already gone
            # (live, 2026-11-08). The refresh holds its own cadence, so
            # asking before each child costs one scan per cadence at most.
            # A gauge scan that fails must not stop the hooks it reports on.
            try:
                driver.refresh_delivery_metrics()
            except Exception as exc:
                Log.warning(
                    "Queue hook gauges could not refresh mid-batch: %s",
                    exc,
                    category="cara.queue.delivery",
                )
            spawned_at = pendulum.now("UTC")
            try:
                returncode = _run_child(
                    [
                        sys.executable,
                        sys.argv[0],
                        "queue:hook",
                        "--job-id",
                        job_id,
                    ],
                    env=child_env,
                    timeout=timeout,
                    stop=stop,
                )
            except subprocess.TimeoutExpired:
                _record_child_failure(
                    job_id,
                    "isolated terminal-hook process timed out",
                )
                Log.error(
                    "Queue terminal hook %s exceeded the isolated process "
                    "timeout and was killed.",
                    job_id,
                    category="cara.queue.delivery",
                )
                continue
            if returncode is None:
                _delivery_release_interrupted_terminal_hook(
                    store,
                    job_id,
                    spawned_at=spawned_at,
                )
                result["interrupted"] += 1
            elif returncode == 0:
                result["completed"] += 1
            elif returncode == getattr(os, "EX_TEMPFAIL", 75):
                result["skipped"] += 1
            else:
                _record_child_failure(
                    job_id,
                    f"isolated terminal-hook process exited with status {returncode}",
                )
        if not stop.is_set():
            driver.refresh_delivery_metrics()
        return result

"""Canonical ``QueueHooksCommand`` command."""

from __future__ import annotations

import os
import subprocess
import sys

from cara.decorators import command
from cara.facades import Log

from .QueueRelayCommand import _RelayLoop


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
    def _run_isolated_hooks(driver) -> dict[str, int]:
        job_ids = driver.due_terminal_hook_ids()
        result = {
            "claimed": len(job_ids),
            "completed": 0,
            "failed": 0,
            "deferred": 0,
            "quarantined": 0,
            "skipped": 0,
        }
        timeout = int(driver.delivery_store.hook_timeout_seconds) + 15
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

        for job_id in job_ids:
            try:
                completed = subprocess.run(
                    [
                        sys.executable,
                        sys.argv[0],
                        "queue:hook",
                        "--job-id",
                        job_id,
                    ],
                    check=False,
                    env=child_env,
                    timeout=timeout,
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
            if completed.returncode == 0:
                result["completed"] += 1
            elif completed.returncode == getattr(os, "EX_TEMPFAIL", 75):
                result["skipped"] += 1
            else:
                _record_child_failure(
                    job_id,
                    "isolated terminal-hook process exited with "
                    f"status {completed.returncode}",
                )
        driver.refresh_delivery_metrics()
        return result
